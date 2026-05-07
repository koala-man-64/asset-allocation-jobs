from tasks.common.job_status import (
    JOB_EXIT_CODE_FAILURE,
    JOB_EXIT_CODE_SUCCESS,
    JOB_STATUS_FAILED,
    JOB_STATUS_SUCCEEDED,
    JOB_STATUS_SUCCEEDED_WITH_WARNINGS,
    resolve_provider_gated_job_run_status,
    resolve_job_run_status,
)


def test_resolve_job_run_status_returns_warning_status_with_success_exit_code() -> None:
    status, exit_code = resolve_job_run_status(failed_count=0, warning_count=3)

    assert status == JOB_STATUS_SUCCEEDED_WITH_WARNINGS
    assert exit_code == JOB_EXIT_CODE_SUCCESS


def test_resolve_job_run_status_returns_failed_status_when_errors_exist() -> None:
    status, exit_code = resolve_job_run_status(failed_count=1, warning_count=10)

    assert status == JOB_STATUS_FAILED
    assert exit_code == JOB_EXIT_CODE_FAILURE


def test_resolve_job_run_status_returns_success_status_without_failures_or_warnings() -> None:
    status, exit_code = resolve_job_run_status(failed_count=0, warning_count=0)

    assert status == JOB_STATUS_SUCCEEDED
    assert exit_code == JOB_EXIT_CODE_SUCCESS


def test_resolve_provider_gated_job_run_status_returns_clean_success() -> None:
    decision = resolve_provider_gated_job_run_status(
        fatal_failure_count=0,
        provider_call_count=3,
        retryable_provider_failure_count=0,
    )

    assert decision.job_status == JOB_STATUS_SUCCEEDED
    assert decision.exit_code == JOB_EXIT_CODE_SUCCESS
    assert decision.failed_count == 0
    assert decision.warning_count == 0
    assert decision.all_provider_calls_failed is False


def test_resolve_provider_gated_job_run_status_returns_warning_success_for_partial_failures() -> None:
    decision = resolve_provider_gated_job_run_status(
        fatal_failure_count=0,
        provider_call_count=3,
        retryable_provider_failure_count=1,
        warning_count=2,
    )

    assert decision.job_status == JOB_STATUS_SUCCEEDED_WITH_WARNINGS
    assert decision.exit_code == JOB_EXIT_CODE_SUCCESS
    assert decision.failed_count == 0
    assert decision.warning_count == 3
    assert decision.all_provider_calls_failed is False


def test_resolve_provider_gated_job_run_status_fails_when_all_provider_calls_fail() -> None:
    decision = resolve_provider_gated_job_run_status(
        fatal_failure_count=0,
        provider_call_count=2,
        retryable_provider_failure_count=2,
    )

    assert decision.job_status == JOB_STATUS_FAILED
    assert decision.exit_code == JOB_EXIT_CODE_FAILURE
    assert decision.failed_count == 1
    assert decision.warning_count == 2
    assert decision.all_provider_calls_failed is True


def test_resolve_provider_gated_job_run_status_fails_on_fatal_failure() -> None:
    decision = resolve_provider_gated_job_run_status(
        fatal_failure_count=1,
        provider_call_count=3,
        retryable_provider_failure_count=0,
    )

    assert decision.job_status == JOB_STATUS_FAILED
    assert decision.exit_code == JOB_EXIT_CODE_FAILURE
    assert decision.failed_count == 1
    assert decision.warning_count == 0
    assert decision.all_provider_calls_failed is False


def test_resolve_provider_gated_job_run_status_fatal_plus_warnings() -> None:
    decision = resolve_provider_gated_job_run_status(
        fatal_failure_count=1,
        provider_call_count=3,
        retryable_provider_failure_count=1,
        warning_count=2,
    )

    assert decision.job_status == JOB_STATUS_FAILED
    assert decision.exit_code == JOB_EXIT_CODE_FAILURE
    assert decision.failed_count == 1
    assert decision.warning_count == 3
    assert decision.all_provider_calls_failed is False
