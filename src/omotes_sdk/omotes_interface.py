import logging
import threading
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Optional, Union

from omotes_sdk.internal.common.broker_interface import (
    BrokerInterface,
    AMQPQueueType,
    QueueTTLArguments,
)
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
from omotes_sdk.workflow_type import (
    WorkflowType,
    WorkflowTypeManager,
    convert_params_dict_to_struct,
)

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
    workflow_type_manager: Union[WorkflowTypeManager, None]
    """All available workflow types."""
    _workflow_config_received: threading.Event
    """Event triggered when workflow configuration is received."""
    client_id: str
    """Identifier of this SDK instance. Should be unique from other SDKs."""
    timeout_on_initial_workflow_definitions: timedelta
    """How long the SDK should wait for the first reply when requesting the current workflow
    definitions from the orchestrator."""

    JOB_QUEUES_TTL: timedelta = timedelta(hours=48)
    """Default value of job result, progress, and status queue TTL."""

    def __init__(
        self,
        rabbitmq_config: RabbitMQConfig,
        client_id: str,
        timeout_on_initial_workflow_definitions: timedelta = timedelta(minutes=1),  # noqa: B008
    ):
        """Create the OMOTES interface.

        NOTE: Needs to be started separately.

        :param rabbitmq_config: RabbitMQ configuration how to connect to OMOTES.
        :param client_id: Identifier of this SDK instance. Should be unique from other SDKs.
        :param timeout_on_initial_workflow_definitions: How long the SDK should wait for the first
            reply when requesting the current workflow definitions from the orchestrator.
        """
        self.broker_if = BrokerInterface(rabbitmq_config)
        self.workflow_type_manager = None
        self._workflow_config_received = threading.Event()
        self.client_id = client_id
        self.timeout_on_initial_workflow_definitions = timeout_on_initial_workflow_definitions

    def start(self) -> None:
        """Start any other interfaces and request available workflows."""
        self.broker_if.start()
        self.broker_if.declare_exchange(OmotesQueueNames.omotes_exchange_name())
        # Declare job submissions queue just in case the orchestrator isn't life yet so we do not
        # lose work.
        self.broker_if.declare_queue(
            queue_name=OmotesQueueNames.job_submission_queue_name(),
            queue_type=AMQPQueueType.DURABLE,
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
        )
        self.connect_to_available_workflows_updates()
        self.request_available_workflows()

        logger.info("Waiting for workflow definitions to be received from the orchestrator...")
        if not self._workflow_config_received.wait(
            timeout=self.timeout_on_initial_workflow_definitions.total_seconds()
        ):
            raise RuntimeError(
                "The orchestrator did not send the available workflows within "
                "the expected time. Is the orchestrator online and connected "
                "to the broker?"
            )

    def stop(self) -> None:
        """Stop any other interfaces."""
        self.broker_if.stop()

    def disconnect_from_submitted_job(self, job: Job) -> None:
        """Disconnect from the submitted job and delete all queues on the broker.

        :param job: Job to disconnect from.
        """
        self.broker_if.remove_queue_subscription(OmotesQueueNames.job_results_queue_name(job.id))
        self.broker_if.remove_queue_subscription(OmotesQueueNames.job_progress_queue_name(job.id))
        self.broker_if.remove_queue_subscription(OmotesQueueNames.job_status_queue_name(job.id))

    def _autodelete_progres_status_queues_on_result(self, job: Job) -> None:
        """Disconnect and delete the progress and status queues for some job."""
        self.broker_if.remove_queue_subscription(OmotesQueueNames.job_progress_queue_name(job.id))
        self.broker_if.remove_queue_subscription(OmotesQueueNames.job_status_queue_name(job.id))

    def connect_to_submitted_job(
        self,
        job: Job,
        callback_on_finished: Callable[[Job, JobResult], None],
        callback_on_progress_update: Optional[Callable[[Job, JobProgressUpdate], None]],
        callback_on_status_update: Optional[Callable[[Job, JobStatusUpdate], None]],
        auto_disconnect_on_result: bool,
        auto_cleanup_after_ttl: Optional[timedelta] = JOB_QUEUES_TTL,
        reconnect: bool = True,
    ) -> None:
        """(Re)connect to the running job.

        Useful when the application using this SDK restarts and needs to reconnect to already
        running jobs. Assumes that the job exists otherwise the callbacks will never be called.

        :param job: The job to reconnect to.
        :param callback_on_finished: Called when the job has a result.
        :param callback_on_progress_update: Called when there is a progress update for the job.
        :param callback_on_status_update: Called when there is a status update for the job.
        :param auto_disconnect_on_result: Remove/disconnect from all queues pertaining to this job
            once the result is received and handled without exceptions through
            `callback_on_finished`.
        :param auto_cleanup_after_ttl: When erroneous situations occur (e.g. client is offline),
            all queues pertaining to this job will be removed after the given TTL.
            Default to 48 hours if unset. Set to `None` to turn off auto clean up,
            but be aware this may lead to leftover messages and queues to be stored
            in RabbitMQ indefinitely (which uses up memory & disk space).
        :param reconnect: When True, first check the job queues status and raise an error if not
            exist. Default to True.
        """
        job_results_queue_name = OmotesQueueNames.job_results_queue_name(job.id)
        job_progress_queue_name = OmotesQueueNames.job_progress_queue_name(job.id)
        job_status_queue_name = OmotesQueueNames.job_status_queue_name(job.id)

        if reconnect:
            logger.info(
                "Reconnect to the submitted job %s is set to True. "
                + "Checking job queues status...",
                job.id,
            )
            if not self.broker_if.queue_exists(job_results_queue_name):
                raise RuntimeError(
                    f"The {job_results_queue_name} queue does not exist or is removed. "
                    "Abort reconnecting to the queue."
                )
            if callback_on_progress_update and not self.broker_if.queue_exists(
                job_progress_queue_name
            ):
                raise RuntimeError(
                    f"The {job_progress_queue_name} queue does not exist or is removed. "
                    "Abort reconnecting to the queue."
                )
            if callback_on_status_update and not self.broker_if.queue_exists(job_status_queue_name):
                raise RuntimeError(
                    f"The {job_status_queue_name} queue does not exist or is removed. "
                    "Abort reconnecting to the queue."
                )

        if auto_disconnect_on_result:
            logger.info("Connecting to update for job %s with auto disconnect on result", job.id)
            auto_disconnect_handler = self._autodelete_progres_status_queues_on_result
        else:
            logger.info("Connecting to update for job %s and expect manual disconnect", job.id)
            auto_disconnect_handler = None

        if auto_cleanup_after_ttl is not None:
            queue_ttl = auto_cleanup_after_ttl
            logger.info(
                "Auto job queues clean up on error after TTL is set. "
                + "The leftover job messages will be dropped, "
                + "and leftover job queues will be discarded after %s.",
                queue_ttl,
            )
            job_queue_ttl = QueueTTLArguments(queue_ttl=queue_ttl)
        else:
            logger.info(
                "Auto job queues clean up on error after TTL is not set. "
                + "Manual cleanup on leftover job queues and messages might be required."
            )
            job_queue_ttl = None

        callback_handler = JobSubmissionCallbackHandler(
            job,
            callback_on_finished,
            callback_on_progress_update,
            callback_on_status_update,
            auto_disconnect_handler,
        )

        if callback_on_progress_update:
            self.broker_if.declare_queue_and_add_subscription(
                queue_name=job_progress_queue_name,
                callback_on_message=callback_handler.callback_on_progress_update_wrapped,
                queue_type=AMQPQueueType.DURABLE,
                exchange_name=OmotesQueueNames.omotes_exchange_name(),
                queue_ttl=job_queue_ttl,
            )
        if callback_on_status_update:
            self.broker_if.declare_queue_and_add_subscription(
                queue_name=job_status_queue_name,
                callback_on_message=callback_handler.callback_on_status_update_wrapped,
                queue_type=AMQPQueueType.DURABLE,
                exchange_name=OmotesQueueNames.omotes_exchange_name(),
                queue_ttl=job_queue_ttl,
            )
        # Declares the job result queue last to ensure the job progress
        # and status queues are already declared and exist. This order allows
        # them to be properly removed by _autodelete_progres_status_queues_on_result()
        self.broker_if.declare_queue_and_add_subscription(
            queue_name=job_results_queue_name,
            callback_on_message=callback_handler.callback_on_finished_wrapped,
            queue_type=AMQPQueueType.DURABLE,
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
            delete_after_messages=1,
            queue_ttl=job_queue_ttl,
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
        job_reference: Optional[str] = None,
        auto_cleanup_after_ttl: Optional[timedelta] = JOB_QUEUES_TTL,
    ) -> Job:
        """Submit a new job and connect to progress and status updates and the job result.

        :param esdl: String containing the XML that make up the ESDL.
        :param params_dict: Dictionary containing any job-specific, non-ESDL, configuration
            parameters. Dictionary supports:
            str, Union[Struct, ListValue, str, float, bool, None, Mapping[str, Any], Sequence]
        :param workflow_type: Type of the workflow to start.
        :param job_timeout: How long the job may take before it is considered to be timeout.
            If None is given, the job is immune from the periodic timeout job cancellation task.
        :param callback_on_finished: Callback which is called with the job result once the job is
            done.
        :param callback_on_progress_update: Callback which is called with any progress updates.
        :param callback_on_status_update: Callback which is called with any status updates.
        :param auto_disconnect_on_result: Remove/disconnect from all queues pertaining to this job
            once the result is received and handled without exceptions through
            `callback_on_finished`.
        :param job_reference: An optional reference to the submitted job which is used in the
            name of the output ESDL as well as in internal logging of OMOTES.
        :param auto_cleanup_after_ttl: When erroneous situations occur (e.g. client is offline),
            all queues pertaining to this job will be removed after the given TTL.
            Default to 48 hours if unset. Set to `None` to turn off auto clean up,
            but be aware this may lead to leftover messages and queues to be stored
            in RabbitMQ indefinitely (which uses up memory & disk space).
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
        reconnect = False
        logger.info("Submitting job %s with reference %s", job.id, job_reference)
        self.connect_to_submitted_job(
            job,
            callback_on_finished,
            callback_on_progress_update,
            callback_on_status_update,
            auto_disconnect_on_result,
            auto_cleanup_after_ttl,
            reconnect,
        )

        if job_timeout is not None:
            timeout_ms = round(job_timeout.total_seconds() * 1000)
        else:
            timeout_ms = None

        job_submission_msg = JobSubmission(
            uuid=str(job.id),
            timeout_ms=timeout_ms,
            workflow_type=workflow_type.workflow_type_name,
            esdl=esdl,
            params_dict=convert_params_dict_to_struct(workflow_type, params_dict),
            job_reference=job_reference,
        )
        self.broker_if.send_message_to(
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
            routing_key=OmotesQueueNames.job_submission_queue_name(),
            message=job_submission_msg.SerializeToString(),
        )
        logger.debug("Done submitting job %s with reference %s", job.id, job_reference)

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
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
            routing_key=OmotesQueueNames.job_cancel_queue_name(),
            message=cancel_msg.SerializeToString(),
        )

    def connect_to_available_workflows_updates(self) -> None:
        """Connect to updates of the available workflows."""
        self.broker_if.declare_queue_and_add_subscription(
            queue_name=OmotesQueueNames.available_workflows_queue_name(self.client_id),
            callback_on_message=self.callback_on_update_available_workflows,
            queue_type=AMQPQueueType.EXCLUSIVE,
            bind_to_routing_key=OmotesQueueNames.available_workflows_routing_key(),
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
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
            exchange_name=OmotesQueueNames.omotes_exchange_name(),
            routing_key=OmotesQueueNames.request_available_workflows_queue_name(),
            message=request_available_workflows_pb.SerializeToString(),
        )

    def get_workflow_type_manager(self) -> WorkflowTypeManager:
        """Get the available workflows."""
        if self.workflow_type_manager:
            return self.workflow_type_manager
        else:
            raise UndefinedWorkflowsException()
