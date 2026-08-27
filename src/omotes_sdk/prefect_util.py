import json
import logging
import os
import re
from collections.abc import Callable
from typing import Any, NoReturn, cast
from urllib.parse import urlsplit
from uuid import UUID

import httpx
from prefect.artifacts import (
    create_link_artifact,
    create_progress_artifact,
    update_progress_artifact,
)
from prefect.blocks.system import Secret
from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    ArtifactFilter,
    ArtifactFilterFlowRunId,
    DeploymentFilter,
    DeploymentFilterName,
    LogFilter,
)
from prefect.client.schemas.objects import FlowRun
from prefect.client.schemas.sorting import DeploymentSort
from prefect.context import get_run_context
from prefect.exceptions import ObjectNotFound, PrefectHTTPStatusError
from prefect.filesystems import RemoteFileSystem
from prefect.runtime import flow_run
from prefect.states import StateType
from pydantic import BaseModel

from omotes_sdk.job_status import JobStatus
from omotes_sdk.memory_quantity import (
    MemoryLimit,
    _to_docker_mem_limit,
)


def _build_minio_result_storage(
    minio_host: str,
    minio_port: str | int,
    access_key: str,
    secret_key: str,
    bucket: str = "prefect-results",
    prefix: str = "flow-results",
) -> RemoteFileSystem:
    """Create MinIO-backed Prefect result storage block when env vars are available.

    Returns:
        RemoteFileSystem: Configured Prefect result storage block.

    """
    endpoint_url = f"http://{minio_host}:{minio_port}"

    return RemoteFileSystem(
        basepath=f"s3://{bucket}/{prefix}",
        settings={
            "key": access_key,
            "secret": secret_key,
            "client_kwargs": {"endpoint_url": endpoint_url},
        },
    )


for _noisy in (
    "httpcore",
    "httpx",
    "websockets",
    "asyncio",
    "hpack",
    "botocore",
    "aiobotocore",
    "s3fs",
    "fsspec",
    "urllib3",
):
    logging.getLogger(_noisy).setLevel(logging.WARNING)


def _raise_prefect_api_error(
    exc: PrefectHTTPStatusError | httpx.RequestError,
) -> NoReturn:
    """Raise a user-facing error for Prefect API authentication/connectivity failures.

    Raises:
        RuntimeError: Describes unauthorized, upstream, or unavailable Prefect API errors.

    """
    prefect_api_url = os.getenv("PREFECT_API_URL", "<unset PREFECT_API_URL>")

    if isinstance(exc, PrefectHTTPStatusError):
        if exc.response.status_code == 401:
            raise RuntimeError(
                "Unauthorized: Invalid or missing authentication for Prefect server at "
                f"{prefect_api_url}. Check PREFECT_API_AUTH_STRING setting."
            ) from exc

        raise RuntimeError(
            f"Prefect server error (HTTP {exc.response.status_code}): {exc.response.reason_phrase}"
        ) from exc

    raise RuntimeError(
        f"Prefect server is unavailable at {prefect_api_url}. "
        "Start Prefect server or update PREFECT_API_URL, then try again."
    ) from exc


def in_prefect_flow_context() -> bool:
    """Check whether execution is inside a Prefect flow context.

    Returns:
        bool: True when called in a Prefect flow context, otherwise False.

    """
    try:
        get_run_context()
        return True
    except Exception:
        return False


def _get_flow_run_id_first_part() -> str:
    """Get first part of flow run id.

    For local runs a default value is returned.

    Returns:
        str: First 8 characters of the flow run id, or a default value for local runs.

    """
    return flow_run.id[:8] if flow_run and flow_run.id else "12345678"


def load_gurobi_license() -> None:
    """Load Gurobi license content from Prefect Secret and write it to disk."""
    target_dir = "/app/gurobi"
    target_file = os.path.join(target_dir, "gurobi.lic")
    os.makedirs(target_dir, exist_ok=True)

    # get license content from Prefect Secret block
    secret_block = cast(Secret[Any], Secret.load("gurobi-wls-secret"))
    license_content = secret_block.get()

    with open(target_file, "w") as f:
        f.write(license_content)

    logging.info(f"Successfully generated {target_file}")


def _sanitize_for_minio(value: str) -> str:
    """Sanitize a value for Prefect artifact keys.

    Returns:
        str: Lower-case alpha-numeric and dash-only string.

    """
    sanitized = re.sub(r"[^a-z0-9-]+", "-", value.lower().strip())
    sanitized = re.sub(r"-+", "-", sanitized).strip("-")
    return sanitized or "field"


def _create_minio_presigned_url(
    minio_block: RemoteFileSystem,
    object_path: str,
    expires_seconds: int = 7 * 24 * 60 * 60,
) -> str | None:
    """Create a presigned GET URL for an object written via minio_block.

    Returns:
        str | None: The generated URL or None when URL generation fails.

    """
    try:
        resolved_path = minio_block._resolve_path(object_path)
        return cast(str, minio_block.filesystem.sign(resolved_path, expiration=expires_seconds))
    except Exception:
        logging.exception("Failed to create presigned URL for MinIO object")
        return None


def _get_required_file_extension(result: BaseModel, field_name: str) -> str:
    """Return the configured file extension for a BaseModel field.

    Returns:
        str: Lower-cased file extension for the field.

    Raises:
        ValueError: If the field has no valid file_extension entry.

    """
    field_info = type(result).model_fields.get(field_name)
    extension = None
    if field_info is not None and isinstance(field_info.json_schema_extra, dict):
        extension = field_info.json_schema_extra.get("file_extension")

    if not isinstance(extension, str) or not extension.startswith("."):
        raise ValueError(
            f"Field '{field_name}' in model '{type(result).__name__}' must define "
            "json_schema_extra={'file_extension': '.ext'}"
        )

    return extension.lower()


def write_flow_return_artifact_to_minio(
    flow_result: BaseModel,
    minio_host: str,
    minio_port: str,
    access_key: str,
    secret_key: str,
) -> str | None:
    """Persist flow return fields to MinIO and publish Prefect links to those objects.

    Returns:
        str | None: Run folder path in MinIO, or None if not in flow context.

    """
    if not in_prefect_flow_context():
        return None

    minio_block = _build_minio_result_storage(minio_host, minio_port, access_key, secret_key)
    run_folder_path = _sanitize_for_minio(f"{flow_run.get_name()}-{_get_flow_run_id_first_part()}")

    for field_name, field_value in flow_result:
        if field_value is None:
            continue

        artifact_key = _sanitize_for_minio(f"{field_name}-{_get_flow_run_id_first_part()}")
        field_extension = _get_required_file_extension(flow_result, field_name)
        field_object_path = f"{run_folder_path}/{artifact_key}{field_extension}"

        field_bytes = (
            field_value.encode("utf-8")
            if isinstance(field_value, str)
            else json.dumps(field_value, indent=2, default=str).encode("utf-8")
        )

        try:
            minio_block.write_path(path=field_object_path, content=field_bytes)
        except Exception:
            logging.exception("Failed to persist flow return field '%s' to MinIO", field_name)
            continue

        presigned_url = _create_minio_presigned_url(minio_block, field_object_path)
        if presigned_url is None:
            continue

        create_link_artifact(
            link=presigned_url,
            link_text=f"Download '{field_name}' from MinIO",
            key=artifact_key,
            description=f"MinIO object: {field_object_path}",
        )

    return run_folder_path


def create_flow_progress_artifact(
    key: str,
    start_progress: float = 0.0,
    start_description: str | None = None,
) -> UUID | None:
    """Create a flow progress artifact and return its id.

    Returns:
        UUID | None: Created artifact id, or None if unavailable.

    """
    if not in_prefect_flow_context():
        return None

    try:
        return cast(
            UUID,
            create_progress_artifact(
                key=key,
                progress=start_progress,
                description=start_description,
            ),
        )
    except Exception:
        return None


def create_flow_progress_updater(
    start_progress_fraction: float = 0.0,
    start_description: str | None = None,
) -> Callable[[float, str | None], None]:
    """Create a progress artifact and return a safe updater function.

    Returns:
        Callable[[float, str | None], None]: Function that updates progress artifacts.

    """
    if not in_prefect_flow_context():
        logging.warning("Progress updates are unavailable outside a Prefect flow context; returning a no-op updater.")
        return lambda progress_fraction, description=None: None

    progress_key = f"progress-{_get_flow_run_id_first_part()}"
    artifact_id = create_flow_progress_artifact(
        key=progress_key,
        start_progress=start_progress_fraction * 100.0,
        start_description=start_description,
    )

    def _update(progress_fraction: float, description: str | None = None) -> None:
        if artifact_id is None:
            logging.error(f"Error logging progress update for run '{flow_run.get_name()}': artifact creation failed")
        else:
            update_progress_artifact(
                artifact_id=artifact_id,
                progress=progress_fraction * 100.0,
                description=description,
            )

    return _update


def _build_universal_job_vars(
    memory_limit: MemoryLimit | str | None = None, base_vars: dict | None = None
) -> dict | None:
    """Build a job_variables payload for Kubernetes-style memory input.

    Kubernetes workers use memory_request and memory_limit directly.
    Docker workers require mem_limit in Docker parse_bytes format. We emit a
    byte-string value (e.g. "8589934592b") so it also satisfies schemas that
    require a string.

    Returns:
        dict | None: Job variables suitable for worker deployment or None if unchanged.

    """
    job_vars = dict(base_vars or {})

    # Normalize provided mem_limit to Docker-compatible string.
    if "mem_limit" in job_vars and job_vars["mem_limit"] is not None:
        job_vars["mem_limit"] = _to_docker_mem_limit(job_vars["mem_limit"])

    if memory_limit:
        normalized_memory_limit = MemoryLimit(str(memory_limit))
        job_vars["memory_limit"] = str(normalized_memory_limit)
        job_vars["memory_request"] = str(normalized_memory_limit)
        job_vars["mem_limit"] = _to_docker_mem_limit(normalized_memory_limit)

    return job_vars or None


async def deploy_flow(
    flow_function: object,
    deployment_name: str,
    image_name: str,
    job_variables: dict,
    prefect_work_pool_name: str,
    max_concurrent_runs: int | None = None,
) -> None:
    """Deploy prefect flow with variables.

    Will raise an error when trying to overwrite an existing semantic version (x.x.x).
    Note: the 'flow' object cannot be annotated properly since @flow is dynamically typed.

    Args:
        flow_function: Flow object exposing a deploy method.
        deployment_name: Name of the deployment.
        image_name: Name of the Docker image.
        job_variables: Job variables.
        prefect_work_pool_name: Name of the Prefect work pool.
        max_concurrent_runs: Maximum number of concurrent flow runs for this deployment.

    Raises:
        RuntimeError: If a semantic-versioned deployment already exists.
        ValueError: If max_concurrent_runs is provided and is less than 1.

    """
    version_name = image_name.rsplit(":", 1)[-1]
    if max_concurrent_runs is not None and max_concurrent_runs < 1:
        raise ValueError("max_concurrent_runs must be at least 1")

    try:
        async with get_client() as client:
            deployments = await client.read_deployments(
                deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=[deployment_name])),
                sort=DeploymentSort.CREATED_DESC,
            )
            if deployments and _is_semantic_version(version_name):
                raise RuntimeError(f"Prefect flow cannot be overwritten for semantic version '{deployment_name}'")
    except (PrefectHTTPStatusError, httpx.RequestError) as exc:
        _raise_prefect_api_error(exc)

    deployable_flow = cast(Any, flow_function)
    await deployable_flow.deploy(
        name=deployment_name,
        work_pool_name=prefect_work_pool_name,
        image=image_name,
        build=False,
        job_variables=job_variables,
        concurrency_limit=max_concurrent_runs,
    )


# Support full semantic versions like 1.2.3-beta+exp.sha.5114f85
SEMVER_REGEX = re.compile(
    r"""
    ^
    (0|[1-9]\d*)\.                # major
    (0|[1-9]\d*)\.                # minor
    (0|[1-9]\d*)                  # patch
    (?:-([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))?  # prerelease (optional)
    (?:\+([0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*))? # build metadata (optional)
    $
    """,
    re.VERBOSE,
)


def _is_semantic_version(version_name: str) -> bool:
    """Return whether a version string follows Semantic Versioning.

    Returns:
        bool: True for semantic version strings, otherwise False.

    """
    return bool(SEMVER_REGEX.match(version_name))


async def get_flow_versions_by_name(flow_names: list[str]) -> dict[str, list[str]]:
    """Return Prefect deployment versions keyed by flow name.

    Reads deployments once and groups version suffixes by the deployment
    base name before the `:` separator.
    """
    requested_flow_names = {flow_name for flow_name in flow_names if flow_name}
    if not requested_flow_names:
        return {}

    try:
        async with get_client() as client:
            deployments = await client.read_deployments(
                sort=DeploymentSort.CREATED_DESC,
            )
    except (PrefectHTTPStatusError, httpx.RequestError) as exc:
        _raise_prefect_api_error(exc)

    result: dict[str, list[str]] = {flow_name: [] for flow_name in requested_flow_names}
    for deployment in deployments:
        if ":" not in deployment.name:
            continue

        name_part, version_part = deployment.name.split(":", 1)
        if name_part in requested_flow_names:
            result[name_part].append(version_part)

    for flow_name, versions in result.items():
        result[flow_name] = sorted(versions, key=_version_sort_key, reverse=True)

    return result


def _version_sort_key(version_name: str) -> tuple[int, tuple[int, int, int], int, str]:
    """Sort versions with semantic versions first, then other strings.

    Semantic versions are sorted by newest version. Non-semantic version
    strings are kept sortable and come after semantic versions.

    Returns:
        tuple[int, tuple[int, int, int], int, str]: Sort key that prefers
        semantic versions and keeps non-semantic versions stable.
    """
    match = SEMVER_REGEX.match(version_name)
    if match is None:
        return (0, (0, 0, 0), 0, version_name)

    major, minor, patch = (int(match.group(index)) for index in range(1, 4))
    prerelease = match.group(4)
    return (1, (major, minor, patch), 1 if prerelease is None else 0, prerelease or "")


def from_prefect_state_type_to_job_status(prefect_state_type: StateType) -> JobStatus:
    """Map a Prefect state type to the corresponding job status.

    Returns:
        JobStatus: Mapped job status.

    Raises:
        ValueError: If the Prefect state type is unexpected.
    """
    if prefect_state_type in [StateType.PENDING]:
        return JobStatus.ENQUEUED
    elif prefect_state_type in [
        StateType.SCHEDULED,
        StateType.RUNNING,
        StateType.PAUSED,
    ]:
        return JobStatus.RUNNING
    elif prefect_state_type in [StateType.COMPLETED]:
        return JobStatus.SUCCEEDED
    elif prefect_state_type in [StateType.FAILED, StateType.CRASHED]:
        return JobStatus.ERROR
    elif prefect_state_type in [StateType.CANCELLED, StateType.CANCELLING]:
        return JobStatus.CANCELLED
    else:
        raise ValueError(f"Unexpected prefect StateType: '{prefect_state_type}'.")


async def get_flow_run_status_and_results(
    flow_run_id: str | UUID,
    minio_host: str,
    minio_port: str,
    access_key: str,
    secret_key: str,
) -> tuple[str, StateType, dict[str, Any], dict[str, str], dict[str, dict[str, Any]], str]:
    """Fetch detailed information for a specific Prefect flow run by ID.

    Args:
        flow_run_id: The UUID or string ID of the flow run.
        minio_host: MinIO host for direct authenticated artifact access.
        minio_port: MinIO port for direct authenticated artifact access.
        access_key: MinIO access key for authenticated access.
        secret_key: MinIO secret key for authenticated access.

    Returns:
        tuple[str, StateType, dict[str, Any], dict[str, str], dict[str, dict[str, Any]], str]:
        Flow run name, state, parameters, tags, artifacts, and logs.

    Raises:
        RuntimeError: If the flow run has no state.
    """
    flow_run_uuid = flow_run_id if isinstance(flow_run_id, UUID) else UUID(str(flow_run_id))
    try:
        async with get_client() as client:
            flow_run = await client.read_flow_run(flow_run_uuid)
            if flow_run.state is None:
                raise RuntimeError(f"Prefect flow run '{flow_run_uuid}' does not have a state")

            run_state_type = flow_run.state.type

            artifacts = await client.read_artifacts(
                artifact_filter=ArtifactFilter(
                    flow_run_id=ArtifactFilterFlowRunId(any_=[flow_run_uuid]),
                    # key=ArtifactFilterKey(any_=["metadata"]),
                )
            )

            tags_by_key: dict[str, str] = {}
            for tag in flow_run.tags or []:
                if ":" in tag:
                    tag_key, tag_value = tag.split(":", 1)
                    tags_by_key[tag_key] = tag_value
                else:
                    tags_by_key[tag] = ""

            artifacts_by_key: dict[str, dict[str, Any]] = {}
            for artifact in artifacts:
                artifact_key = artifact.key or str(artifact.id)
                artifact_data = await _resolve_artifact_data(
                    artifact.data,
                    minio_host=minio_host,
                    minio_port=minio_port,
                    access_key=access_key,
                    secret_key=secret_key,
                )

                artifacts_by_key[artifact_key] = {
                    "data": artifact_data,
                    "description": artifact.description,
                }

            log_filter = LogFilter(flow_run_id={"any_": [flow_run.id]})
            logs = await client.read_logs(log_filter=log_filter)
            log_lines: list[str] = []
            for log in logs:
                level_value = getattr(log, "level", None)
                if isinstance(level_value, int):
                    level_name = logging.getLevelName(level_value)
                elif isinstance(level_value, str):
                    level_name = level_value
                else:
                    level_name = "unknown"

                log_lines.append(
                    f"{getattr(log, 'timestamp', '')} "
                    f"[{str(level_name).lower()}]: "
                    f"{getattr(log, 'message', '')} "
                    f"[{getattr(log, 'name', 'unknown')}]"
                )
            log_str = "\n".join(log_lines)

            logging.debug(f"Prefect run found with id '{flow_run.id}' in state '{run_state_type}'.")

            return (
                flow_run.name,
                run_state_type,
                flow_run.parameters,
                tags_by_key,
                artifacts_by_key,
                log_str,
            )
    except (PrefectHTTPStatusError, httpx.RequestError) as exc:
        _raise_prefect_api_error(exc)


_MINIO_MARKDOWN_LINK_RE = re.compile(r"^\[(?P<label>.*?)\]\((?P<url>https?://[^)]+)\)$")
_MINIO_URL_RE = re.compile(r"^['\"]?(?P<url>https?://[^'\"]+)['\"]?$")


async def _resolve_artifact_data(
    data: object,
    minio_host: str,
    minio_port: str | int,
    access_key: str,
    secret_key: str,
) -> object:
    """Resolve artifact data using authenticated MinIO access.

    JSON strings are decoded: MinIO links are read directly without using the auth signature in the presigned URL which
    can expire.

    Returns:
        object: Resolved artifact data (JSON dict, string, or raw object).
    """
    if not isinstance(data, str):
        return data

    stripped_data = data.strip()
    markdown_match = _MINIO_MARKDOWN_LINK_RE.match(stripped_data)
    artifact_value = markdown_match.group("url") if markdown_match is not None else stripped_data

    url_match = _MINIO_URL_RE.match(artifact_value)
    if url_match is None:
        try:
            return json.loads(stripped_data)
        except json.JSONDecodeError:
            return data

    url = url_match.group("url")
    minio_block = _build_minio_result_storage(minio_host, minio_port, access_key, secret_key)
    storage_url = urlsplit(minio_block.basepath)
    storage_path = "/".join(part for part in (storage_url.netloc, storage_url.path.strip("/")) if part)
    object_path = urlsplit(url).path.removeprefix(f"/{storage_path}/")
    read_result = minio_block.read_path(object_path)
    downloaded_bytes = read_result if isinstance(read_result, bytes) else await read_result
    downloaded_data = downloaded_bytes.decode("utf-8")
    try:
        return json.loads(downloaded_data)
    except json.JSONDecodeError:
        return downloaded_data


async def trigger_flow_run(
    run_name: str,
    deployment_base_name: str,
    deployment_version: str | None = None,
    parameters: dict | None = None,
    memory_limit: MemoryLimit | str | None = None,
    job_variables: dict | None = None,
    run_tags: list[str] | None = None,
) -> UUID:
    """Create a Prefect flow run from a deployment and return the new run id.

    Returns:
        UUID: The created flow run id.

    Raises:
        RuntimeError: If the requested deployment does not exist.
    """
    try:
        async with get_client() as client:
            if deployment_version is None:
                deployments = await client.read_deployments(sort=DeploymentSort.CREATED_DESC)
                matching_deployments = [
                    deployment for deployment in deployments if deployment.name.startswith(f"{deployment_base_name}:")
                ]
                if not matching_deployments:
                    raise RuntimeError(f"Prefect deployment '{deployment_base_name}' not found for run '{run_name}'")

                selected_deployment = max(
                    matching_deployments,
                    key=lambda deployment: _version_sort_key(deployment.name.split(":", 1)[1]),
                )
                deployment_name = selected_deployment.name
                deployment_version = deployment_name.split(":", 1)[1]
            else:
                deployment_name = f"{deployment_base_name}:{deployment_version}"
                deployments = await client.read_deployments(
                    deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=[deployment_name])),
                    sort=DeploymentSort.CREATED_DESC,
                )

                if not deployments:
                    raise RuntimeError(f"Prefect deployment '{deployment_name}' not found for run '{run_name}'")

                selected_deployment = deployments[0]

            resolved_run_tags = [f"version:{deployment_version}"]
            if run_tags:
                resolved_run_tags.extend(run_tags)

            deployment_id = selected_deployment.id
            flow_run = await client.create_flow_run_from_deployment(
                deployment_id=deployment_id,
                parameters=parameters or {},
                name=run_name,
                tags=resolved_run_tags,
                job_variables=_build_universal_job_vars(memory_limit=memory_limit, base_vars=job_variables),
            )
            logging.info(
                f"✅ Created flow run '{run_name}' from '{deployment_name}' → Run ID: {flow_run.id}; "
                f"tags={resolved_run_tags}"
            )
            return flow_run.id
    except (PrefectHTTPStatusError, httpx.RequestError) as exc:
        _raise_prefect_api_error(exc)


async def get_runs() -> list[FlowRun]:
    """Get all Prefect flow runs.

    Returns:
        list[FlowRun]: List of Prefect flow runs.
    """
    try:
        async with get_client() as client:
            return await client.read_flow_runs()
    except (PrefectHTTPStatusError, httpx.RequestError) as exc:
        _raise_prefect_api_error(exc)


async def delete_run(flow_run_id: UUID) -> bool:
    """Delete a Prefect flow run by ID.

    Returns:
        bool: True when the flow run was deleted, False when it did not exist.
    """
    try:
        async with get_client() as client:
            try:
                await client.delete_flow_run(flow_run_id)
            except ObjectNotFound:
                return False
    except (PrefectHTTPStatusError, httpx.RequestError) as exc:
        _raise_prefect_api_error(exc)

    return True
