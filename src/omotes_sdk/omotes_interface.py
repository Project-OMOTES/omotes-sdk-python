import logging
import uuid
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, Optional

from omotes_sdk.broker_interface import BrokerInterface
from omotes_sdk.config import RabbitMQConfig
from omotes_sdk_protocol.job_pb2 import (
    JobResult,
    JobProgressUpdate,
    JobStatusUpdate,
    JobSubmission,
    JobCancel,
)
from omotes_sdk.job import Job
from omotes_sdk.queue_names import OmotesQueueNames
from omotes_sdk.workflow_type import WorkflowType

logger = logging.getLogger("omotes_sdk")


@dataclass
class JobSubmissionCallbackHandler:
    job: Job
    callback_on_finished: Callable[[Job, JobResult], None]
    callback_on_progress_update: Callable[[Job, JobProgressUpdate], None]
    callback_on_status_update: Callable[[Job, JobStatusUpdate], None]

    def callback_on_finished_wrapped(self, message: bytes) -> None:
        job_result = JobResult().ParseFromString(message)
        self.callback_on_finished(self.job, job_result)

    def callback_on_progress_update_wrapped(self, message: bytes) -> None:
        progress_update = JobProgressUpdate().ParseFromString(message)
        self.callback_on_progress_update(self.job, progress_update)

    def callback_on_status_update_wrapped(self, message: bytes) -> None:
        status_update = JobStatusUpdate().ParseFromString(message)
        self.callback_on_status_update(self.job, status_update)


class OmotesInterface:
    broker_if: BrokerInterface

    def __init__(self, rabbitmq_config: RabbitMQConfig):
        self.broker_if = BrokerInterface(rabbitmq_config)

    def start(self):
        self.broker_if.start()

    def stop(self):
        self.broker_if.stop()

    def connect_to_submitted_job(
        self,
        job: Job,
        callback_on_finished: Callable[[Job, JobResult], None],
        callback_on_progress_update: Optional[Callable[[Job, JobProgressUpdate], None]],
        callback_on_status_update: Optional[Callable[[Job, JobStatusUpdate], None]],
    ) -> None:
        """(Re)connect to the running job.

        Useful when the application using this SDK restarts and needs to reconnect to already running jobs.
        Assumes that the job exists otherwise the callbacks will never be called.

        :param job: The job to reconnect to.
        :param callback_on_finished: Called when the job has a result.
        :param callback_on_progress_update: Called when there is a progress update for the job.
        :param callback_on_status_update: Called when there is a status update for the job.
        :return:
        """
        callback_handler = JobSubmissionCallbackHandler(
            job, callback_on_finished, callback_on_progress_update, callback_on_status_update
        )

        self.broker_if.receive_next_message(
            queue_name=OmotesQueueNames.job_results_queue_name(job),
            timeout=None,
            callback_on_message=callback_handler.callback_on_finished_wrapped,
            callback_on_no_message=None,
        )
        self.broker_if.add_queue_subscription(
            queue_name=OmotesQueueNames.job_progress_queue_name(job),
            callback_on_message=callback_handler.callback_on_progress_update_wrapped,
        )
        self.broker_if.add_queue_subscription(
            queue_name=OmotesQueueNames.job_status_queue_name(job),
            callback_on_message=callback_handler.callback_on_status_update_wrapped,
        )

    def submit_job(
        self,
        esdl: str,
        job_config: dict,
        workflow_type: WorkflowType,
        job_timeout: Optional[timedelta],
        callback_on_finished: Callable[[Job, JobResult], None],
        callback_on_progress_update: Optional[Callable[[Job, JobProgressUpdate], None]],
        callback_on_status_update: Optional[Callable[[Job, JobStatusUpdate], None]],
    ) -> Job:
        """Submit a new job and connect to progress and status updates and the job result.

        :param esdl: String containing the XML that make up the ESDL.
        :param job_config: Any job-specific configuration parameters.
        :param workflow_type: Type of the workflow to start.
        :param job_timeout: How long the job may take before it is considered to be timeout.
        :param callback_on_finished: Callback which is called with the job result once the job is done.
        :param callback_on_progress_update: Callback which is called with any progress updates.
        :param callback_on_status_update: Callback which is called with any status updates.
        :return: The job handle which is created. This object needs to be saved persistently by the program using this
            SDK in order to resume listening to jobs in progress after a restart.
        """
        job = Job(id=uuid.uuid4(), workflow_type=workflow_type)

        self.connect_to_submitted_job(job, callback_on_finished, callback_on_progress_update, callback_on_status_update)

        timeout_ms = round(job_timeout.total_seconds() * 1000) if job_timeout else None
        job_submission_msg = JobSubmission(
            uuid=str(job.id),
            timeout_ms=timeout_ms,
            workflow_type=workflow_type.workflow_type_name,
            esdl=esdl.encode(),
        )
        self.broker_if.send_message_to(
            OmotesQueueNames.job_submission_queue_name(workflow_type), message=job_submission_msg.SerializeToString()
        )

        return job

    def cancel_job(self, job: Job, callback_on_finished: Callable[[], None]) -> None:
        # TODO Figure out a way to resume listening for the cancellation confirmation after a reboot of the SDK.
        self.broker_if.receive_next_message(
            queue_name=OmotesQueueNames.job_cancel_response_queue_name(job),
            timeout=None,
            callback_on_message=callback_on_finished,
            callback_on_no_message=None,
        )

        cancel_msg = JobCancel(uuid=str(job.id))
        self.broker_if.send_message_to(
            OmotesQueueNames.job_cancel_queue_name(job), message=cancel_msg.SerializeToString()
        )


# broker_if = BrokerInterface(RabbitMQConfig(username="omotes", password="somepass1", virtual_host="omotes"))
# broker_if.start()
#
# broker_if.add_queue_subscription("test_queue", print)
#
# broker_if.receive_next_message("test_queue", None, print, lambda: print("No message on queue"))
# broker_if.receive_next_message("test_queue2", 10, print, lambda: print("No message on queue2"))
# broker_if.receive_next_message("test_queue3", None, print, lambda: print("No message on queue3"))
#
# broker_if.send_message_to("test_queue", b"BLABLABLA")
# broker_if.send_message_to("test_queue2", b"Whatsie")
#
# broker_if.join()


#
# broker_if.add_queue_subscription("test_queue", print)
#
# broker_if.receive_next_message("test_queue", None, print, lambda: print("No message on queue"))
# broker_if.receive_next_message("test_queue2", 10, print, lambda: print("No message on queue2"))
# broker_if.receive_next_message("test_queue3", None, print, lambda: print("No message on queue3"))
#
# broker_if.send_message_to("test_queue", b"BLABLABLA")
# broker_if.send_message_to("test_queue2", b"Whatsie")
#
# broker_if.join()
