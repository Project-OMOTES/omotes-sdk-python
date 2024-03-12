import logging
import uuid
from dataclasses import dataclass
from typing import Callable

from omotes_sdk_protocol.job_pb2 import (
    JobSubmission,
    JobProgressUpdate,
    JobStatusUpdate,
    JobResult,
    JobCancel,
)
from omotes_sdk.internal.common.broker_interface import BrokerInterface
from omotes_sdk.config import RabbitMQConfig
from omotes_sdk.job import Job
from omotes_sdk.queue_names import OmotesQueueNames
from omotes_sdk.workflow_type import WorkflowType, WorkflowTypeManager

logger = logging.getLogger("omotes_sdk_internal")


@dataclass
class JobSubmissionCallbackHandler:
    """Handler to setup and around callbacks associated with receiving a submitted job.

    A `JobSubmissionCallbackHandler` is created per job submission queue.
    """

    workflow_type: WorkflowType
    """Messages received from this job submission queue are expected to have this workflow type."""
    callback_on_new_job: Callable[[JobSubmission, Job], None]
    """Callback to handle any jobs that are submitted."""

    def callback_on_new_job_wrapped(self, message: bytes) -> None:
        """Prepare the `Job` and `JobSubmission` messages before passing them to the callback.

        Expected workflow type is confirmed before passing it to handler.

        :param message: Serialized AMQP message containing a new job submission.
        """
        submitted_job = JobSubmission()
        submitted_job.ParseFromString(message)

        if self.workflow_type.workflow_type_name == submitted_job.workflow_type:
            job = Job(uuid.UUID(submitted_job.uuid), self.workflow_type)
            self.callback_on_new_job(submitted_job, job)
        else:
            logger.error(
                "Received a job submission (id: %s) that was meant for workflow type %s but found "
                "it on queue %s. Dropping message.",
                submitted_job.uuid,
                submitted_job.workflow_type,
                self.workflow_type,
            )


@dataclass
class JobCancellationHandler:
    callback_on_cancel_job: Callable[[JobCancel], None]

    def callback_on_job_cancelled_wrapped(self, message: bytes) -> None:
        """Prepare the `Job` and `JobSubmission` messages before passing them to the callback.

        :param message: Serialized AMQP message containing a job cancellation.
        """
        cancelled_job = JobCancel()
        cancelled_job.ParseFromString(message)

        self.callback_on_cancel_job(cancelled_job)


class OrchestratorInterface:
    """RabbitMQ interface specifically for the orchestrator."""

    broker_if: BrokerInterface
    """Interface to RabbitMQ."""
    workflow_type_manager: WorkflowTypeManager
    """All available workflow types."""

    def __init__(
        self, omotes_rabbitmq_config: RabbitMQConfig, workflow_type_manager: WorkflowTypeManager
    ):
        """Create the orchestrator interface.

        :param omotes_rabbitmq_config: How to connect to RabbitMQ as the orchestrator.
        :param workflow_type_manager: All available workflow types.
        """
        self.broker_if = BrokerInterface(omotes_rabbitmq_config)
        self.workflow_type_manager = workflow_type_manager

    def start(self) -> None:
        """Start the orchestrator interface."""
        self.broker_if.start()

    def stop(self) -> None:
        """Stop the orchestrator interface."""
        self.broker_if.stop()

    def connect_to_job_submissions(
        self, callback_on_new_job: Callable[[JobSubmission, Job], None]
    ) -> None:
        """Connect to the job submission queue for each workflow type.

        :param callback_on_new_job: Callback to handle any new job submission.
        """
        for workflow_type in self.workflow_type_manager.get_all_workflows():
            callback_handler = JobSubmissionCallbackHandler(workflow_type, callback_on_new_job)
            self.broker_if.add_queue_subscription(
                OmotesQueueNames.job_submission_queue_name(workflow_type),
                callback_on_message=callback_handler.callback_on_new_job_wrapped,
            )

    def connect_to_job_cancellations(
        self, callback_on_job_cancel: Callable[[JobCancel], None]
    ) -> None:
        """Connect to the job cancellations queue.

        :param callback_on_job_cancel: Callback to handle any new job cancellations.
        """
        callback_handler = JobCancellationHandler(callback_on_job_cancel)
        self.broker_if.add_queue_subscription(
            OmotesQueueNames.job_cancel_queue_name(),
            callback_on_message=callback_handler.callback_on_job_cancelled_wrapped,
        )

    def send_job_progress_update(self, job: Job, progress_update: JobProgressUpdate) -> None:
        """Send a job progress update to the SDK.

        :param job: Job handle for which a progress update is send.
        :param progress_update: Current progress for the job.
        """
        self.broker_if.send_message_to(
            OmotesQueueNames.job_progress_queue_name(job), progress_update.SerializeToString()
        )

    def send_job_status_update(self, job: Job, status_update: JobStatusUpdate) -> None:
        """Send a job status update to the SDK.

        :param job: Job handle for which a status update is send.
        :param status_update: Current status for the job.
        """
        self.broker_if.send_message_to(
            OmotesQueueNames.job_status_queue_name(job), status_update.SerializeToString()
        )

    def send_job_result(self, job: Job, result: JobResult) -> None:
        """Send the job result to the SDK.

        :param job: Job to which the result belongs.
        :param result: The job result.
        """
        self.broker_if.send_message_to(
            OmotesQueueNames.job_results_queue_name(job), result.SerializeToString()
        )
