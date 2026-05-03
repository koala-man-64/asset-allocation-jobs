from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
from typing import Any

import yaml


_BOOLEAN_TEXT = {"true": "true", "false": "false"}
_REQUIRED_MARKET_RUNTIME = {
    "bronze-market-job": {"triggerType": "schedule", "cronExpression": "0 22 * * 1-5"},
    "silver-market-job": {"triggerType": "manual"},
    "gold-market-job": {"triggerType": "manual"},
    "gold-regime-job": {
        "triggerType": "schedule",
        "cronExpression": "30 2 * * 2-6",
        "maxReplicaRetryLimit": 1,
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify deployed ACA Job runtime settings against rendered manifests."
    )
    parser.add_argument("--rendered-dir", required=True, help="Directory containing rendered manifests.")
    parser.add_argument("--resource-group", required=True, help="Azure resource group containing the jobs.")
    parser.add_argument("--expected-image", required=True, help="Expected container image digest or reference.")
    return parser.parse_args()


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"Manifest {path} did not render to a YAML object.")
    return payload


def _first_container(payload: dict[str, Any], *, source: str) -> dict[str, Any]:
    properties = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
    template = properties.get("template") if isinstance(properties.get("template"), dict) else {}
    containers = template.get("containers") if isinstance(template.get("containers"), list) else []
    if not containers or not isinstance(containers[0], dict):
        raise SystemExit(f"{source} does not define properties.template.containers[0].")
    return containers[0]


def _configuration(payload: dict[str, Any]) -> dict[str, Any]:
    properties = payload.get("properties") if isinstance(payload.get("properties"), dict) else {}
    return properties.get("configuration") if isinstance(properties.get("configuration"), dict) else {}


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value)
    boolean_text = _BOOLEAN_TEXT.get(text.strip().lower())
    if boolean_text is not None:
        return boolean_text
    return text


def _normalize_command(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, list):
        return tuple(_normalize(item) for item in value)
    return (_normalize(value),)


def _env_contract(container: dict[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    values: dict[str, str] = {}
    secret_refs: dict[str, str] = {}
    for item in container.get("env") or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue
        if "secretRef" in item:
            secret_refs[name] = _normalize(item.get("secretRef"))
        else:
            values[name] = _normalize(item.get("value"))
    return values, secret_refs


def _secret_names(configuration: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    for item in configuration.get("secrets") or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if name:
            names.add(name)
    return names


def _cron(configuration: dict[str, Any]) -> str:
    trigger = configuration.get("scheduleTriggerConfig")
    if not isinstance(trigger, dict):
        return ""
    return _normalize(trigger.get("cronExpression"))


def query_job_runtime(*, job_name: str, resource_group: str) -> dict[str, Any]:
    output = subprocess.check_output(
        [
            "az",
            "containerapp",
            "job",
            "show",
            "--name",
            job_name,
            "--resource-group",
            resource_group,
            "-o",
            "json",
        ],
        text=True,
    )
    payload = json.loads(output)
    if not isinstance(payload, dict):
        raise SystemExit(f"Azure returned a non-object payload for {job_name}.")
    return payload


def _compare_manifest_to_live(
    *,
    manifest_path: Path,
    rendered: dict[str, Any],
    live: dict[str, Any],
    expected_image: str,
) -> list[str]:
    job_name = str(rendered.get("name") or "").strip()
    errors: list[str] = []
    expected_configuration = _configuration(rendered)
    live_configuration = _configuration(live)
    expected_container = _first_container(rendered, source=str(manifest_path))
    live_container = _first_container(live, source=f"live job {job_name}")

    expected_manifest_image = _normalize(expected_container.get("image"))
    live_image = _normalize(live_container.get("image"))
    if expected_manifest_image != expected_image:
        errors.append(f"{job_name}: rendered image does not match expected deployment image")
    if live_image != expected_image:
        errors.append(f"{job_name}: image mismatch: expected {expected_image}, found {live_image}")

    expected_command = _normalize_command(expected_container.get("command"))
    live_command = _normalize_command(live_container.get("command"))
    if expected_command != live_command:
        errors.append(f"{job_name}: command mismatch")

    for field_name in ("triggerType", "replicaRetryLimit", "replicaTimeout"):
        expected = _normalize(expected_configuration.get(field_name))
        actual = _normalize(live_configuration.get(field_name))
        if expected != actual:
            errors.append(f"{job_name}: {field_name} mismatch: expected {expected}, found {actual}")

    expected_trigger = _normalize(expected_configuration.get("triggerType")).lower()
    if expected_trigger == "schedule":
        expected_cron = _cron(expected_configuration)
        actual_cron = _cron(live_configuration)
        if expected_cron != actual_cron:
            errors.append(f"{job_name}: cronExpression mismatch: expected {expected_cron}, found {actual_cron}")

    expected_secrets = _secret_names(expected_configuration)
    actual_secrets = _secret_names(live_configuration)
    for name in sorted(expected_secrets - actual_secrets):
        errors.append(f"{job_name}: missing configuration secret {name}")
    for name in sorted(actual_secrets - expected_secrets):
        errors.append(f"{job_name}: unexpected configuration secret {name}")

    expected_values, expected_secret_refs = _env_contract(expected_container)
    actual_values, actual_secret_refs = _env_contract(live_container)
    expected_env_names = set(expected_values) | set(expected_secret_refs)
    actual_env_names = set(actual_values) | set(actual_secret_refs)
    for name in sorted(expected_env_names - actual_env_names):
        errors.append(f"{job_name}: missing env {name}")
    for name in sorted(actual_env_names - expected_env_names):
        errors.append(f"{job_name}: unexpected env {name}")

    for name, expected in sorted(expected_values.items()):
        if name in actual_secret_refs:
            errors.append(f"{job_name}: env {name} expected literal value but found secretRef")
            continue
        if name in actual_values and actual_values[name] != expected:
            errors.append(f"{job_name}: env {name} value mismatch")

    for name, expected_ref in sorted(expected_secret_refs.items()):
        if name in actual_values:
            errors.append(f"{job_name}: env {name} expected secretRef but found literal value")
            continue
        if name in actual_secret_refs and actual_secret_refs[name] != expected_ref:
            errors.append(
                f"{job_name}: env {name} secretRef mismatch: expected {expected_ref}, found {actual_secret_refs[name]}"
            )

    return errors


def _manifest_runtime_invariant_errors(rendered_jobs: dict[str, dict[str, Any]]) -> list[str]:
    errors: list[str] = []
    for job_name, expected in _REQUIRED_MARKET_RUNTIME.items():
        rendered = rendered_jobs.get(job_name)
        if rendered is None:
            continue
        configuration = _configuration(rendered)
        trigger_type = _normalize(configuration.get("triggerType")).lower()
        expected_trigger = str(expected.get("triggerType") or "").lower()
        if trigger_type != expected_trigger:
            errors.append(f"{job_name}: triggerType invariant mismatch: expected {expected_trigger}, found {trigger_type}")
        expected_cron = expected.get("cronExpression")
        if expected_cron is not None:
            actual_cron = _cron(configuration)
            if actual_cron != expected_cron:
                errors.append(f"{job_name}: cronExpression invariant mismatch: expected {expected_cron}, found {actual_cron}")
        max_retry = expected.get("maxReplicaRetryLimit")
        if max_retry is not None:
            try:
                actual_retry = int(configuration.get("replicaRetryLimit"))
            except (TypeError, ValueError):
                errors.append(f"{job_name}: replicaRetryLimit invariant mismatch: expected <= {max_retry}, found invalid")
                continue
            if actual_retry > int(max_retry):
                errors.append(f"{job_name}: replicaRetryLimit invariant mismatch: expected <= {max_retry}, found {actual_retry}")
    return errors


def verify_deployed_job_runtime(*, rendered_dir: Path, resource_group: str, expected_image: str) -> None:
    errors: list[str] = []
    rendered_jobs: dict[str, dict[str, Any]] = {}
    for manifest in sorted(rendered_dir.glob("job_*.yaml")):
        rendered = _load_yaml(manifest)
        job_name = str(rendered.get("name") or "").strip()
        if not job_name:
            errors.append(f"{manifest}: missing job name")
            continue
        rendered_jobs[job_name] = rendered
        live = query_job_runtime(job_name=job_name, resource_group=resource_group)
        errors.extend(
            _compare_manifest_to_live(
                manifest_path=manifest,
                rendered=rendered,
                live=live,
                expected_image=expected_image,
            )
        )
    errors.extend(_manifest_runtime_invariant_errors(rendered_jobs))

    if errors:
        raise SystemExit("Deployed job runtime drift detected:\n" + "\n".join(errors))


def main() -> None:
    args = parse_args()
    verify_deployed_job_runtime(
        rendered_dir=Path(args.rendered_dir),
        resource_group=args.resource_group,
        expected_image=args.expected_image,
    )


if __name__ == "__main__":
    main()
