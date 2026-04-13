# Contributing

## Development Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -e .
python3 -m pip install -r requirements-dev.txt
```

Use Python 3.14 for local jobs-runtime work so local installs match CI and container images.

## Day-to-Day Checks

```bash
python3 scripts/run_quality_gate.py lint-python
python3 scripts/run_quality_gate.py test-fast
python3 scripts/run_quality_gate.py test-full
```

`.github/workflows/quality.yml` runs the jobs-owned validation path, dependency audits, and dependency-governance checks.

## Dependency Governance

- Runtime dependency source of truth is `pyproject.toml` under `[project].dependencies`.
- Regenerate `requirements.txt` and `requirements.lock.txt` from `pyproject.toml` with:

```bash
python3 scripts/dependency_governance.py sync
```

- Validate runtime and dev dependency alignment with:

```bash
python3 scripts/dependency_governance.py check --report artifacts/dependency_governance_report.json
```

- If you change `requirements-dev.txt`, keep `requirements-dev.lock.txt` aligned in the same change. CI installs the dev lockfile when it is present.

## Docs and Config Changes

- Update `docs/ops/env-contract.csv` when you add, rename, or remove GitHub variables or secrets.
- Update `.env.template` only for contract rows where `template=true`.
- Treat `asset-allocation-control-plane` and `asset-allocation-ui` as owners of local API and UI implementation docs. This repo should describe them only as external dependencies.
- If you add, remove, or rename repo-local agents under `.codex/skills`, update `AGENTS.md` in the same change.

## Pull Requests

- Keep changes scoped to jobs-owned runtime, monitoring, deployment, provider, and integration surfaces.
- Add or update tests for behavior changes.
- Call out auth, storage, Postgres, job-manifest, or cross-repo compatibility impacts when you touch `deploy/`, runtime config, or control-plane transport code.

## Evidence

- `pyproject.toml`
- `requirements.txt`
- `requirements.lock.txt`
- `requirements-dev.txt`
- `requirements-dev.lock.txt`
- `scripts/dependency_governance.py`
- `scripts/run_quality_gate.py`
- `.github/workflows/quality.yml`
- `pytest.ini`
- `docs/ops/env-contract.csv`
- `.env.template`
- `DEPLOYMENT_SETUP.md`
