from __future__ import annotations

from dataclasses import dataclass

JOB_EXIT_CODE_SUCCESS = 0
JOB_EXIT_CODE_FAILURE = 1

JOB_STATUS_SUCCEEDED = "succeeded"
JOB_STATUS_SUCCEEDED_WITH_WARNINGS = "succeededWithWarnings"
JOB_STATUS_FAILED = "failed"


@dataclass(frozen=True)
class ProviderRunStatusDecision:
    job_status: str
    exit_code: int
    failed_count: int
    warning_count: int
    all_provider_calls_failed: bool


def resolve_job_run_status(*, failed_count: int, warning_count: int = 0) -> tuple[str, int]:
    failed_total = int(failed_count or 0)
    warning_total = int(warning_count or 0)

    if failed_total > 0:
        return JOB_STATUS_FAILED, JOB_EXIT_CODE_FAILURE
    if warning_total > 0:
        return JOB_STATUS_SUCCEEDED_WITH_WARNINGS, JOB_EXIT_CODE_SUCCESS
    return JOB_STATUS_SUCCEEDED, JOB_EXIT_CODE_SUCCESS


def resolve_provider_gated_job_run_status(
    *,
    fatal_failure_count: int,
    provider_call_count: int,
    retryable_provider_failure_count: int,
    warning_count: int = 0,
) -> ProviderRunStatusDecision:
    fatal_total = max(int(fatal_failure_count or 0), 0)
    provider_total = max(int(provider_call_count or 0), 0)
    retryable_provider_failures = max(int(retryable_provider_failure_count or 0), 0)
    warning_total = max(int(warning_count or 0), 0) + retryable_provider_failures

    all_provider_calls_failed = provider_total > 0 and retryable_provider_failures >= provider_total
    failed_total = fatal_total + (1 if all_provider_calls_failed else 0)
    job_status, exit_code = resolve_job_run_status(
        failed_count=failed_total,
        warning_count=warning_total,
    )
    return ProviderRunStatusDecision(
        job_status=job_status,
        exit_code=exit_code,
        failed_count=failed_total,
        warning_count=warning_total,
        all_provider_calls_failed=all_provider_calls_failed,
    )
