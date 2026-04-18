from __future__ import annotations

import httpx

from asset_allocation_runtime_common.control_plane_transport import ControlPlaneTransport, ControlPlaneTransportConfig
from asset_allocation_runtime_common.universe_repository import UniverseRepository


def _build_transport(handler) -> ControlPlaneTransport:
    client = httpx.Client(transport=httpx.MockTransport(handler))
    return ControlPlaneTransport(
        ControlPlaneTransportConfig(base_url="https://control-plane.example", api_scope="api://asset-allocation"),
        http_client=client,
        access_token_provider=lambda: "test-token",
    )


def test_get_universe_config_reads_internal_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/internal/universes/large-cap-quality"
        return httpx.Response(
            200,
            json={"name": "large-cap-quality", "version": 2, "config": {"source": "postgres_gold"}},
        )

    transport = _build_transport(handler)
    try:
        repo = UniverseRepository(transport=transport)
        result = repo.get_universe_config("large-cap-quality")
    finally:
        transport.close()

    assert result == {
        "name": "large-cap-quality",
        "version": 2,
        "config": {"source": "postgres_gold"},
    }


def test_get_universe_config_revision_returns_none_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "Universe config revision 'large-cap-quality' not found."})

    transport = _build_transport(handler)
    try:
        repo = UniverseRepository(transport=transport)
        assert repo.get_universe_config_revision("large-cap-quality", version=9) is None
    finally:
        transport.close()


def test_universe_mutations_and_listing_are_blocked() -> None:
    transport = _build_transport(lambda request: httpx.Response(200, json={}))
    try:
        repo = UniverseRepository(transport=transport)
        try:
            repo.list_universe_configs()
        except NotImplementedError as exc:
            assert "does not list" in str(exc)
        else:
            raise AssertionError("Expected NotImplementedError for jobs-side universe listing.")

        for method in (repo.save_universe_config, repo.delete_universe_config):
            try:
                method("large-cap-quality")
            except NotImplementedError as exc:
                assert "does not" in str(exc)
            else:
                raise AssertionError("Expected NotImplementedError for jobs-side control-plane mutation method.")
    finally:
        transport.close()
