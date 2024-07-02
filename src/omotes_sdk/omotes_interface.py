import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Optional
from google.protobuf.struct_pb2 import Struct

from omotes_sdk.internal.common.broker_interface import BrokerInterface
from omotes_sdk.config import RabbitMQConfig
from omotes_sdk_protocol.job_pb2 import (
    JobResult,
    JobProgressUpdate,
    JobStatusUpdate,
    JobSubmission,
    JobCancel,
)
from omotes_sdk_protocol.workflow_pb2 import AvailableWorkflows, RequestAvailableWorkflows
from omotes_sdk.job import Job
from omotes_sdk.queue_names import OmotesQueueNames
from omotes_sdk.types import ParamsDict
from omotes_sdk.workflow_type import WorkflowType, WorkflowTypeManager

logger = logging.getLogger("omotes_sdk")


@dataclass
class JobSubmissionCallbackHandler:
    """Handler for updates on a submitted job."""

    job: Job
    """The job to listen to."""
    callback_on_finished: Callable[[Job, JobResult], None]
    """Handler which is called when a job finishes."""
    callback_on_progress_update: Optional[Callable[[Job, JobProgressUpdate], None]]
    """Handler which is called on a job progress update."""
    callback_on_status_update: Optional[Callable[[Job, JobStatusUpdate], None]]
    """Handler which is called on a job status update."""
    auto_disconnect_on_result_handler: Optional[Callable[[Job], None]]
    """Handler to remove/disconnect from all queues pertaining to this job once the result is
    received and handled without exceptions through `callback_on_finished`."""

    def callback_on_finished_wrapped(self, message: bytes) -> None:
        """Parse a serialized JobResult message and call handler.

        :param message: Serialized message.
        """
        job_result = JobResult()
        job_result.ParseFromString(message)
        self.callback_on_finished(self.job, job_result)

        if self.auto_disconnect_on_result_handler:
            self.auto_disconnect_on_result_handler(self.job)

    def callback_on_progress_update_wrapped(self, message: bytes) -> None:
        """Parse a serialized JobProgressUpdate message and call handler.

        :param message: Serialized message.
        """
        progress_update = JobProgressUpdate()
        progress_update.ParseFromString(message)
        if self.callback_on_progress_update:
            self.callback_on_progress_update(self.job, progress_update)

    def callback_on_status_update_wrapped(self, message: bytes) -> None:
        """Parse a serialized JobStatusUpdate message and call handler.

        :param message: Serialized message.
        """
        status_update = JobStatusUpdate()
        status_update.ParseFromString(message)
        if self.callback_on_status_update:
            self.callback_on_status_update(self.job, status_update)


class UndefinedWorkflowsException(Exception):
    """Thrown if the workflows are needed but not defined yet."""

    ...


class UnknownWorkflowException(Exception):
    """Thrown if a job is submitted using an unknown workflow type."""

    ...


class OmotesInterface:
    """SDK interface for other applications to communicate with OMOTES."""

    broker_if: BrokerInterface
    """Interface to RabbitMQ broker."""
    workflow_type_manager: WorkflowTypeManager | None
    """All available workflow types."""
    _workflow_config_received: threading.Event
    """Event triggered when workflow configuration is received."""

    def __init__(
        self,
        rabbitmq_config: RabbitMQConfig,
    ):
        """Create the OMOTES interface.

        NOTE: Needs to be started separately.

        :param rabbitmq_config: RabbitMQ configuration how to connect to OMOTES.
        """
        self.broker_if = BrokerInterface(rabbitmq_config)
        self.workflow_type_manager = None
        self._workflow_config_received = threading.Event()

    def start(self) -> None:
        """Start any other interfaces and request available workflows."""
        self.broker_if.start()
        self.connect_to_available_workflows_updates()
        self.request_available_workflows()

        while not self._workflow_config_received.is_set():
            logger.info("Waiting for workflow definitions to be received from the orchestrator...")
            self._workflow_config_received.wait(timeout=5)

    def stop(self) -> None:
        """Stop any other interfaces."""
        self.broker_if.stop()

    def disconnect_from_submitted_job(self, job: Job) -> None:
        """Disconnect from the submitted job and delete all queues on the broker.

        :param job: Job to disconnect from.
        """
        self.broker_if.remove_queue_next_message_subscription(
            OmotesQueueNames.job_results_queue_name(job)
        )
        self.broker_if.remove_queue_subscription(OmotesQueueNames.job_progress_queue_name(job))
        self.broker_if.remove_queue_subscription(OmotesQueueNames.job_status_queue_name(job))

    def connect_to_submitted_job(
        self,
        job: Job,
        callback_on_finished: Callable[[Job, JobResult], None],
        callback_on_progress_update: Optional[Callable[[Job, JobProgressUpdate], None]],
        callback_on_status_update: Optional[Callable[[Job, JobStatusUpdate], None]],
        auto_disconnect_on_result: bool,
    ) -> None:
        """(Re)connect to the running job.

        Useful when the application using this SDK restarts and needs to reconnect to already
        running jobs. Assumes that the job exists otherwise the callbacks will never be called.

        :param job: The job to reconnect to.
        :param callback_on_finished: Called when the job has a result.
        :param callback_on_progress_update: Called when there is a progress update for the job.
        :param callback_on_status_update: Called when there is a status update for the job.
        :param auto_disconnect_on_result: Remove/disconnect from all queues pertaining to this job
        once the result is received and handled without exceptions through `callback_on_finished`.
        """
        if auto_disconnect_on_result:
            logger.info("Connecting to update for job %s with auto disconnect on result", job.id)
            auto_disconnect_handler = self.disconnect_from_submitted_job
        else:
            logger.info("Connecting to update for job %s and expect manual disconnect", job.id)
            auto_disconnect_handler = None

        callback_handler = JobSubmissionCallbackHandler(
            job,
            callback_on_finished,
            callback_on_progress_update,
            callback_on_status_update,
            auto_disconnect_handler,
        )

        self.broker_if.receive_next_message(
            queue_name=OmotesQueueNames.job_results_queue_name(job),
            timeout=None,
            callback_on_message=callback_handler.callback_on_finished_wrapped,
            callback_on_no_message=None,
        )
        if callback_on_progress_update:
            self.broker_if.add_queue_subscription(
                queue_name=OmotesQueueNames.job_progress_queue_name(job),
                callback_on_message=callback_handler.callback_on_progress_update_wrapped,
            )
        if callback_on_status_update:
            self.broker_if.add_queue_subscription(
                queue_name=OmotesQueueNames.job_status_queue_name(job),
                callback_on_message=callback_handler.callback_on_status_update_wrapped,
            )

    def submit_job(
        self,
        esdl: str,
        params_dict: ParamsDict,
        workflow_type: WorkflowType,
        job_timeout: Optional[timedelta],
        callback_on_finished: Callable[[Job, JobResult], None],
        callback_on_progress_update: Optional[Callable[[Job, JobProgressUpdate], None]],
        callback_on_status_update: Optional[Callable[[Job, JobStatusUpdate], None]],
        auto_disconnect_on_result: bool,
    ) -> Job:
        """Submit a new job and connect to progress and status updates and the job result.

        :param esdl: String containing the XML that make up the ESDL.
        :param params_dict: Dictionary containing any job-specific, non-ESDL, configuration
            parameters. Dictionary supports:
            str, Union[Struct, ListValue, str, float, bool, None, Mapping[str, Any], Sequence]
        :param workflow_type: Type of the workflow to start.
        :param job_timeout: How long the job may take before it is considered to be timeout.
        :param callback_on_finished: Callback which is called with the job result once the job is
            done.
        :param callback_on_progress_update: Callback which is called with any progress updates.
        :param callback_on_status_update: Callback which is called with any status updates.
        :param auto_disconnect_on_result: Remove/disconnect from all queues pertaining to this job
            once the result is received and handled without exceptions through
            `callback_on_finished`.
        :raises UnknownWorkflowException: If `workflow_type` is unknown as a possible workflow in
            this interface.
        :return: The job handle which is created. This object needs to be saved persistently by the
            program using this SDK in order to resume listening to jobs in progress after a restart.
        """
        if not self.workflow_type_manager or not self.workflow_type_manager.workflow_exists(
            workflow_type
        ):
            raise UnknownWorkflowException()

        job = Job(id=uuid.uuid4(), workflow_type=workflow_type)
        logger.info("Submitting job %s", job.id)
        self.connect_to_submitted_job(
            job,
            callback_on_finished,
            callback_on_progress_update,
            callback_on_status_update,
            auto_disconnect_on_result,
        )

        timeout_ms = round(job_timeout.total_seconds() * 1000) if job_timeout else None
        params_dict_struct = Struct()
        params_dict_struct.update(params_dict)
        job_submission_msg = JobSubmission(
            uuid=str(job.id),
            timeout_ms=timeout_ms,
            workflow_type=workflow_type.workflow_type_name,
            esdl=esdl,
            params_dict=params_dict_struct,
        )
        self.broker_if.send_message_to(
            OmotesQueueNames.job_submission_queue_name(workflow_type),
            message=job_submission_msg.SerializeToString(),
        )
        logger.debug("Done submitting job %s", job.id)

        return job

    def cancel_job(self, job: Job) -> None:
        """Cancel a job.

        If this succeeds or not will be send as a job status update through the
        `callback_on_status_update` handler. This method will not disconnect from the submitted job
        events. This will need to be done separately using `disconnect_from_submitted_job`.

        :param job: The job to cancel.
        """
        logger.info("Cancelling job %s", job.id)
        cancel_msg = JobCancel(uuid=str(job.id))
        self.broker_if.send_message_to(
            OmotesQueueNames.job_cancel_queue_name(), message=cancel_msg.SerializeToString()
        )

    def connect_to_available_workflows_updates(self) -> None:
        """Connect to updates of the available workflows."""
        self.broker_if.add_queue_subscription(
            queue_name=OmotesQueueNames.available_workflows_queue_name(),
            callback_on_message=self.callback_on_update_available_workflows,
        )

    def callback_on_update_available_workflows(self, message: bytes) -> None:
        """Parse a serialized AvailableWorkflows message and update workflow type manager.

        :param message: Serialized message.
        """
        available_workflows_pb = AvailableWorkflows()
        available_workflows_pb.ParseFromString(message)
        self.workflow_type_manager = WorkflowTypeManager.from_pb_message(available_workflows_pb)
        self._workflow_config_received.set()
        logger.info("Updated the available workflows")

    def request_available_workflows(self) -> None:
        """Request the available workflows from the orchestrator."""
        request_available_workflows_pb = RequestAvailableWorkflows()
        self.broker_if.send_message_to(
            OmotesQueueNames.request_available_workflows_queue_name(),
            request_available_workflows_pb.SerializeToString(),
        )

    def get_workflow_type_manager(self) -> WorkflowTypeManager:
        """Get the available workflows."""
        if self.workflow_type_manager:
            return self.workflow_type_manager
        else:
            raise UndefinedWorkflowsException()
