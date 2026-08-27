# ruff: noqa: D103

import asyncio
from typing import cast

import pytest
from prefect.states import StateType
from pydantic import BaseModel, Field

from omotes_sdk.job_status import JobStatus
from omotes_sdk.memory_quantity import _memory_quantity_to_bytes
from omotes_sdk.prefect_util import (
    _build_universal_job_vars,
    _get_required_file_extension,
    _is_semantic_version,
    _resolve_artifact_data,
    _sanitize_for_minio,
    _to_docker_mem_limit,
    _version_sort_key,
    create_flow_progress_updater,
    from_prefect_state_type_to_job_status,
    get_runs,
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

    result = _build_universal_job_vars(memory_limit="512Mi", base_vars=base_vars)

    assert result is not None
    assert result["existing"] == "value"
    assert result["memory_limit"] == "512Mi"
    assert result["memory_request"] == "512Mi"
    assert result["mem_limit"] == "536870912b"
    assert base_vars == {"existing": "value"}


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [
        (512 * 1024 * 1024, "536870912b"),
        ("8G", "8g"),
        ("2Gi", "2147483648b"),
        ("750M", "750m"),
        ("1024m", "1024m"),
    ],
)
def test_to_docker_mem_limit(raw_value: str | int, expected: str) -> None:
    assert _to_docker_mem_limit(raw_value) == expected


def test_build_universal_job_vars_normalizes_existing_mem_limit() -> None:
    base_vars = {"mem_limit": "8G", "keep": "yes"}

    result = _build_universal_job_vars(base_vars=base_vars)

    assert result == {"mem_limit": "8g", "keep": "yes"}
    assert base_vars == {"mem_limit": "8G", "keep": "yes"}


def test_build_universal_job_vars_returns_none_when_empty() -> None:
    assert _build_universal_job_vars() is None


def test_create_flow_progress_updater_is_noop_outside_prefect_context(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level("WARNING"):
        updater = create_flow_progress_updater()
        updater(0.5, "local run")

    assert "outside a Prefect flow context" in caplog.text


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
    assert _is_semantic_version(version_name) is expected


def test_version_sort_key_orders_semver_before_non_semver_and_stable_before_prerelease() -> None:
    versions = ["latest", "1.2.3-beta", "1.10.0", "1.2.3", "dev", "2.0.0"]

    sorted_versions = sorted(versions, key=_version_sort_key, reverse=True)

    assert sorted_versions == [
        "2.0.0",
        "1.10.0",
        "1.2.3",
        "1.2.3-beta",
        "latest",
        "dev",
    ]


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
    assert asyncio.run(_resolve_artifact_data(123, "minio.example.com", 9000, "key", "secret")) == 123


def test_resolve_artifact_data_parses_json_strings() -> None:
    resolved = asyncio.run(_resolve_artifact_data('{"a": 1, "b": [2, 3]}', "minio.example.com", 9000, "key", "secret"))

    assert resolved == {"a": 1, "b": [2, 3]}


def test_resolve_artifact_data_reads_minio_url(monkeypatch: pytest.MonkeyPatch) -> None:
    class _MinioBlock:
        basepath = "s3://prefect-results/flow-results"
        object_path: str | None = None

        async def read_path(self, object_path: str) -> bytes:
            self.object_path = object_path
            return b'{"progress": 0.5}'

    minio_block = _MinioBlock()
    monkeypatch.setattr("omotes_sdk.prefect_util._build_minio_result_storage", lambda *args: minio_block)

    resolved = asyncio.run(
        _resolve_artifact_data(
            "http://minio.example.com:9000/prefect-results/flow-results/oo3-7314720b/output-esdl-7314720b.esdl?X-Amz-Signature=expired",
            "minio.example.com",
            9000,
            "key",
            "secret",
        )
    )

    assert minio_block.object_path == "oo3-7314720b/output-esdl-7314720b.esdl"
    assert resolved == {"progress": 0.5}


def test_resolve_artifact_data_returns_original_for_non_json_string() -> None:
    raw_data = "not json"

    assert asyncio.run(_resolve_artifact_data(raw_data, "minio.example.com", 9000, "key", "secret")) == raw_data


def test_get_runs_returns_all_flow_runs(monkeypatch: pytest.MonkeyPatch) -> None:
    expected_runs = ["run-1", "run-2"]

    class _Client:
        async def read_flow_runs(self) -> list[str]:
            return expected_runs

    class _ClientContext:
        async def __aenter__(self) -> _Client:
            return _Client()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
            return False

    monkeypatch.setattr("omotes_sdk.prefect_util.get_client", lambda: _ClientContext())

    assert asyncio.run(get_runs()) == expected_runs


def test_get_runs_returns_empty_list_when_no_flow_runs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        async def read_flow_runs(self) -> list[str]:
            return []

    class _ClientContext:
        async def __aenter__(self) -> _Client:
            return _Client()

        async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
            return False

    monkeypatch.setattr("omotes_sdk.prefect_util.get_client", lambda: _ClientContext())

    assert asyncio.run(get_runs()) == []
