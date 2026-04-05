from __future__ import annotations

JOB_EXIT_CODE_SUCCESS = 0
JOB_EXIT_CODE_FAILURE = 1

JOB_STATUS_SUCCEEDED = "succeeded"
JOB_STATUS_SUCCEEDED_WITH_WARNINGS = "succeededWithWarnings"
JOB_STATUS_FAILED = "failed"


def resolve_job_run_status(*, failed_count: int, warning_count: int = 0) -> tuple[str, int]:
    failed_total = int(failed_count or 0)
    warning_total = int(warning_count or 0)

    if failed_total > 0:
        return JOB_STATUS_FAILED, JOB_EXIT_CODE_FAILURE
    if warning_total > 0:
        return JOB_STATUS_SUCCEEDED_WITH_WARNINGS, JOB_EXIT_CODE_SUCCESS
    return JOB_STATUS_SUCCEEDED, JOB_EXIT_CODE_SUCCESS
