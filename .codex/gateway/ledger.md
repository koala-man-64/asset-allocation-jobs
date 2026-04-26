# Gateway Ledger (Local)

This ledger tracks tool usage and delivery orchestration for work items executed by Codex.

## Policy
- MCP-first: attempt to discover MCP tools/resources; if unavailable, fallback to direct local tools with justification.
- Log: intent → tool → outcome → next decision.

## Session Log

### 2026-04-26
- **Work Item:** `bronze-layer-remediation-20260426` jobs remediation and central coordination.
  - **Branch:** `agent/codex/bronze-layer-remediation-20260426/asset-allocation-jobs` from `origin/main` at `0a6b1c7b574ae4a73920e0d1542ec7879137ac28`.
  - **Scope:** bronze job safety, schedules, disabled-provider handling, artifact publication gating, and downstream callback discipline.
  - **Contract routing:** local-only; no shared data contract or public schema changes planned.
  - **Coordination:** central ledger for runtime-common, control-plane, and jobs implementation until validation gates pass.
- **Progress:** implemented jobs-side bronze remediation in `asset-allocation-jobs`: deferred alpha26 active artifacts to finalize, disabled scheduled market AV enrichment by default, fail-closed earnings publish gates, sanitized economic catalyst source failure handling, disabled-by-default Quiver bronze no-op, and docs/env/manifest updates.
- **Validation:** `python -m py_compile ...` passed for modified job modules; targeted `python -m pytest tests/tasks/common/test_bronze_alpha26_publish.py tests/market_data/test_bronze_market_data.py tests/earnings_data/test_bronze_earnings_data.py tests/economic_catalyst_data/test_bronze_economic_catalyst_data.py tests/economic_catalyst_data/test_sources.py tests/quiver_data/test_config.py tests/quiver_data/test_bronze_quiver_data.py tests/test_workflow_runtime_ownership.py tests/test_env_contract.py -q` -> `132 passed`; targeted `python -m ruff check ...` -> passed.
- **Coordinator follow-up:** full-suite validation exposed a Quiver config backward-compatibility regression in older unit tests; patched `QuiverDataConfig.enabled` to default to `False` while preserving `from_env()` behavior.
- **Coordinator validation:** reran focused jobs remediation tests and full `python -m ruff check .`; both passed.
- **Coordinator validation:** reran Quiver config/universe tests (`11 passed`), full `python -m ruff check .` (passed), and `git diff --check` (passed with line-ending warnings only). Full `python -m pytest -q --tb=short` now has two unrelated baseline failures: installed shared package dependency drift and gold column lookup seed coverage drift.

### 2026-02-03
- **MCP discovery:** `functions.list_mcp_resources` / `functions.list_mcp_resource_templates` returned empty; no MCP tools available → fallback to local tools permitted.
- **Fallback tooling:** Using `functions.shell_command` and `functions.apply_patch` with explicit intent logging in Orchestrator Updates.
- **Work Item:** `WI-CONFIGJS-001` standardize `/config.js` at domain root (docs + tests + dev proxy toggle).
  - **Code changes:** added `VITE_PROXY_CONFIG_JS` toggle in `ui/vite.config.ts`; documented contract in `docs/config_js_contract.md`; added backend contract tests in `tests/api/test_config_js_contract.py`; updated `.env.template`.
  - **Verification:** `python3 -m pytest -q tests/api/test_config_js_contract.py tests/monitoring/test_system_health.py` → `13 passed`.
