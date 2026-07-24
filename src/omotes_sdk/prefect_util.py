import json
import logging
import os
import re
from collections.abc import Callable
from typing import Any, cast
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
from prefect.client.schemas.sorting import DeploymentSort
from prefect.context import (
    get_run_context,
)
from prefect.exceptions import ObjectNotFound
from prefect.filesystems import RemoteFileSystem
from prefect.runtime import flow_run
from prefect.states import StateType
from pydantic import BaseModel

from omotes_sdk.job_status import JobStatus


def _build_minio_result_storage(minio_host: str, access_key: str, secret_key: str) -> RemoteFileSystem:
    """Create MinIO-backed Prefect result storage block when env vars are available.

    Returns:
        RemoteFileSystem: Configured Prefect result storage block.

    """
    bucket = "prefect-results"
    prefix = "flow-results"
    endpoint_url = f"http://{minio_host}"

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


def get_flow_run_id_first_part() -> str:
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
    minio_block: RemoteFileSystem, object_path: str, expires_seconds: int = 7 * 24 * 60 * 60
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
    flow_result: BaseModel, minio_host: str, access_key: str, secret_key: str
) -> str | None:
    """Persist flow return fields to MinIO and publish Prefect links to those objects.

    Returns:
        str | None: Run folder path in MinIO, or None if not in flow context.

    """
    if not in_prefect_flow_context():
        return None

    minio_block = _build_minio_result_storage(minio_host, access_key, secret_key)
    run_folder_path = _sanitize_for_minio(f"{flow_run.get_name()}-{get_flow_run_id_first_part()}")

    for field_name, field_value in flow_result:
        if field_value is None:
            continue

        artifact_key = _sanitize_for_minio(f"{field_name}-{get_flow_run_id_first_part()}")
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
    progress_key = f"progress-{get_flow_run_id_first_part()}"
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
                artifact_id=artifact_id, progress=progress_fraction * 100.0, description=description
            )

    return _update


def _memory_quantity_to_bytes(memory_limit: str) -> int:
    """Convert a Kubernetes-style memory quantity into bytes for Docker.

    Returns:
        int: Memory quantity in bytes.

    Raises:
        ValueError: If the input memory quantity has an unsupported format.

    """
    normalized = memory_limit.strip()
    match = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)([kmgtpe]i?|)", normalized)
    if not match:
        raise ValueError(f"Unsupported memory quantity: {memory_limit!r}")

    value = float(match.group(1))
    suffix = match.group(2).lower()
    binary_factors = {
        "": 1,
        "k": 10**3,
        "m": 10**6,
        "g": 10**9,
        "t": 10**12,
        "p": 10**15,
        "e": 10**18,
        "ki": 1024,
        "mi": 1024**2,
        "gi": 1024**3,
        "ti": 1024**4,
        "pi": 1024**5,
        "ei": 1024**6,
    }
    return int(value * binary_factors[suffix])


def build_universal_job_vars(memory_limit: str | None = None, base_vars: dict | None = None) -> dict | None:
    """Build a job_variables payload for Kubernetes-style memory input.

    Kubernetes workers use memory_request and memory_limit directly.
    Docker workers use mem_limit, so we convert the Kubernetes quantity to bytes in Python.

    Returns:
        dict | None: Job variables suitable for worker deployment or None if unchanged.

    """
    job_vars = dict(base_vars or {})

    if memory_limit:
        job_vars["memory_limit"] = memory_limit
        job_vars["memory_request"] = memory_limit
        job_vars["mem_limit"] = _memory_quantity_to_bytes(memory_limit)

    return job_vars or None


async def deploy_flow(
    flow_function: object,
    deployment_name: str,
    image_name: str,
    job_variables: dict,
    prefect_work_pool_name: str,
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

    Raises:
        RuntimeError: If a semantic-versioned deployment already exists.

    """
    version_name = image_name.rsplit(":", 1)[-1]

    async with get_client() as client:
        deployments = await client.read_deployments(
            deployment_filter=DeploymentFilter(name=DeploymentFilterName(any_=[deployment_name])),
            sort=DeploymentSort.CREATED_DESC,
        )
        if deployments and is_semantic_version(version_name):
            raise RuntimeError(f"Prefect flow cannot be overwritten for semantic version '{deployment_name}'")

    deployable_flow = cast(Any, flow_function)
    await deployable_flow.deploy(
        name=deployment_name,
        work_pool_name=prefect_work_pool_name,
        image=image_name,
        build=False,
        job_variables=job_variables,
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


def is_semantic_version(version_name: str) -> bool:
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

    async with get_client() as client:
        deployments = await client.read_deployments(
            sort=DeploymentSort.CREATED_DESC,
        )

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


async def get_flows_with_versions(identifier: str | None = None) -> list[dict]:
    """Return deployments grouped by flow name and version list.

    Returns:
        list[dict]: Flow names with their available version names.
    """
    async with get_client() as client:
        deployments = await client.read_deployments(
            sort=DeploymentSort.CREATED_DESC,
        )
        result = {}
        for deployment in deployments:
            if (not identifier or identifier in deployment.name) and ":" in deployment.name:
                name_part, version_part = deployment.name.split(":", 1)
                result.setdefault(name_part, []).append(version_part)

        return [{"name": k, "version_names": v} for k, v in result.items()]


def from_prefect_state_type_to_job_status(prefect_state_type: StateType) -> JobStatus:
    """Map a Prefect state type to the corresponding job status.

    Returns:
        JobStatus: Mapped job status.

    Raises:
        ValueError: If the Prefect state type is unexpected.
    """
    if prefect_state_type in [StateType.PENDING]:
        return JobStatus.ENQUEUED
    elif prefect_state_type in [StateType.SCHEDULED, StateType.RUNNING, StateType.PAUSED]:
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
) -> tuple[str, StateType, dict[str, Any], dict[str, str], dict[str, dict[str, Any]], str]:
    """Fetch detailed information for a specific Prefect flow run by ID.

    Returns:
        tuple[str, StateType, dict[str, Any], dict[str, str], dict[str, dict[str, Any]], str]:
        Flow run name, state, parameters, tags, artifacts, and logs.

    Raises:
        RuntimeError: If the flow run has no state.
    """
    flow_run_uuid = flow_run_id if isinstance(flow_run_id, UUID) else UUID(str(flow_run_id))
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
            artifact_data = await _resolve_artifact_data(artifact.data)

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

        return flow_run.name, run_state_type, flow_run.parameters, tags_by_key, artifacts_by_key, log_str


_MINIO_MARKDOWN_LINK_RE = re.compile(r"^\[(?P<label>.*?)\]\((?P<url>https?://[^)]+)\)$")
_MINIO_URL_RE = re.compile(r"^['\"]?(?P<url>https?://[^'\"]+)['\"]?$")


async def _resolve_artifact_data(data: object) -> object:
    if not isinstance(data, str):
        return data

    stripped_data = data.strip()
    markdown_match = _MINIO_MARKDOWN_LINK_RE.match(stripped_data)
    if markdown_match is not None:
        stripped_data = markdown_match.group("url")

    url_match = _MINIO_URL_RE.match(stripped_data)
    if url_match is not None:
        url = url_match.group("url")
        async with httpx.AsyncClient(follow_redirects=True, timeout=60.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            downloaded_data = response.text

        try:
            return json.loads(downloaded_data)
        except json.JSONDecodeError:
            return downloaded_data

    try:
        return json.loads(stripped_data)
    except json.JSONDecodeError:
        return data


async def trigger_flow_run(
    run_name: str,
    deployment_base_name: str,
    deployment_version: str | None = None,
    parameters: dict | None = None,
    memory_limit: str | None = None,
    job_variables: dict | None = None,
    run_tags: list[str] | None = None,
) -> UUID:
    """Create a Prefect flow run from a deployment and return the new run id.

    Returns:
        UUID: The created flow run id.

    Raises:
        RuntimeError: If the requested deployment does not exist.
    """
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
            job_variables=build_universal_job_vars(memory_limit=memory_limit, base_vars=job_variables),
        )
        logging.info(
            f"✅ Created flow run '{run_name}' from '{deployment_name}' → Run ID: {flow_run.id}; "
            f"tags={resolved_run_tags}"
        )
        return flow_run.id


async def delete_run(flow_run_id: UUID) -> bool:
    """Delete a Prefect flow run by ID.

    Returns:
        bool: True when the flow run was deleted, False when it did not exist.
    """
    async with get_client() as client:
        try:
            await client.delete_flow_run(flow_run_id)
        except ObjectNotFound:
            return False

    return True
