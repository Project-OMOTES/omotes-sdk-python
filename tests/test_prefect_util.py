# ruff: noqa: D103

import asyncio
from typing import cast

import pytest
from prefect.states import StateType
from pydantic import BaseModel, Field

from omotes_sdk.job_status import JobStatus
from omotes_sdk.prefect_util import (
    _get_required_file_extension,
    _memory_quantity_to_bytes,
    _resolve_artifact_data,
    _sanitize_for_minio,
    _version_sort_key,
    build_universal_job_vars,
    from_prefect_state_type_to_job_status,
    is_semantic_version,
)


class _ResultWithExtensions(BaseModel):
    report: str = Field(json_schema_extra={"file_extension": ".JSON"})


class _ResultWithoutExtensions(BaseModel):
    report: str


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Simple Name", "simple-name"),
        (" already--clean ", "already-clean"),
        ("***", "field"),
    ],
)
def test_sanitize_for_minio(raw: str, expected: str) -> None:
    assert _sanitize_for_minio(raw) == expected


def test_get_required_file_extension_returns_normalized_extension() -> None:
    result = _ResultWithExtensions(report="{}")

    assert _get_required_file_extension(result, "report") == ".json"


def test_get_required_file_extension_raises_when_missing() -> None:
    result = _ResultWithoutExtensions(report="{}")

    with pytest.raises(ValueError, match="must define"):
        _get_required_file_extension(result, "report")


@pytest.mark.parametrize(
    ("memory_limit", "expected_bytes"),
    [
        ("512Mi", 512 * 1024 * 1024),
        ("2Gi", 2 * 1024 * 1024 * 1024),
        ("1.5G", int(1.5 * 10**9)),
        ("1000", 1000),
    ],
)
def test_memory_quantity_to_bytes(memory_limit: str, expected_bytes: int) -> None:
    assert _memory_quantity_to_bytes(memory_limit) == expected_bytes


def test_memory_quantity_to_bytes_rejects_unsupported_format() -> None:
    with pytest.raises(ValueError, match="Unsupported memory quantity"):
        _memory_quantity_to_bytes("12XYZ")


def test_build_universal_job_vars_with_memory_and_base_vars() -> None:
    base_vars = {"existing": "value"}

    result = build_universal_job_vars(memory_limit="512Mi", base_vars=base_vars)

    assert result is not None
    assert result["existing"] == "value"
    assert result["memory_limit"] == "512Mi"
    assert result["memory_request"] == "512Mi"
    assert result["mem_limit"] == 512 * 1024 * 1024
    assert base_vars == {"existing": "value"}


def test_build_universal_job_vars_returns_none_when_empty() -> None:
    assert build_universal_job_vars() is None


@pytest.mark.parametrize(
    ("version_name", "expected"),
    [
        ("1.2.3", True),
        ("1.2.3-beta", True),
        ("1.2.3-beta+exp.sha.5114f85", True),
        ("latest", False),
        ("v1.2.3", False),
    ],
)
def test_is_semantic_version(version_name: str, expected: bool) -> None:
    assert is_semantic_version(version_name) is expected


def test_version_sort_key_orders_semver_before_non_semver_and_stable_before_prerelease() -> None:
    versions = ["latest", "1.2.3-beta", "1.10.0", "1.2.3", "dev", "2.0.0"]

    sorted_versions = sorted(versions, key=_version_sort_key, reverse=True)

    assert sorted_versions == ["2.0.0", "1.10.0", "1.2.3", "1.2.3-beta", "latest", "dev"]


@pytest.mark.parametrize(
    ("prefect_state", "expected_status"),
    [
        (StateType.PENDING, JobStatus.ENQUEUED),
        (StateType.SCHEDULED, JobStatus.RUNNING),
        (StateType.RUNNING, JobStatus.RUNNING),
        (StateType.PAUSED, JobStatus.RUNNING),
        (StateType.COMPLETED, JobStatus.SUCCEEDED),
        (StateType.FAILED, JobStatus.ERROR),
        (StateType.CRASHED, JobStatus.ERROR),
        (StateType.CANCELLED, JobStatus.CANCELLED),
        (StateType.CANCELLING, JobStatus.CANCELLED),
    ],
)
def test_from_prefect_state_type_to_job_status(prefect_state: StateType, expected_status: JobStatus) -> None:
    assert from_prefect_state_type_to_job_status(prefect_state) == expected_status


def test_from_prefect_state_type_to_job_status_raises_on_unexpected() -> None:
    unknown_state = cast(StateType, "UNKNOWN")

    with pytest.raises(ValueError, match="Unexpected prefect StateType"):
        from_prefect_state_type_to_job_status(unknown_state)


def test_resolve_artifact_data_non_string_passthrough() -> None:
    assert asyncio.run(_resolve_artifact_data(123)) == 123


def test_resolve_artifact_data_parses_json_strings() -> None:
    resolved = asyncio.run(_resolve_artifact_data('{"a": 1, "b": [2, 3]}'))

    assert resolved == {"a": 1, "b": [2, 3]}


def test_resolve_artifact_data_returns_original_for_non_json_string() -> None:
    raw_data = "not json"

    assert asyncio.run(_resolve_artifact_data(raw_data)) == raw_data
