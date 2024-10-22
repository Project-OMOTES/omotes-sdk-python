import uuid


class OmotesQueueNames:
    """Container for OMOTES SDK to Orchestrator queue names, routing keys and exchange names."""

    @staticmethod
    def omotes_exchange_name() -> str:
        """Generate the name of the omotes exchange.

        :return:  The exchange name.
        """
        return "omotes_exchange"

    @staticmethod
    def job_submission_queue_name() -> str:
        """Generate the job submission queue name.

        :return: The queue name.
        """
        return "job_submissions"

    @staticmethod
    def job_results_queue_name(job_uuid: uuid.UUID) -> str:
        """Generate the job results queue name given the job id.

        :param job_uuid: The identifier of the job.
        :return: The queue name.
        """
        return f"jobs.{job_uuid}.result"

    @staticmethod
    def job_progress_queue_name(job_uuid: uuid.UUID) -> str:
        """Generate the job progress update queue name given the job id.

        :param job_uuid: The identifier of the job.
        :return: The queue name.
        """
        return f"jobs.{job_uuid}.progress"

    @staticmethod
    def job_status_queue_name(job_uuid: uuid.UUID) -> str:
        """Generate the job status update queue name given the job id.

        :param job_uuid: The identifier of the job.
        :return: The queue name.
        """
        return f"jobs.{job_uuid}.status"

    @staticmethod
    def job_cancel_queue_name() -> str:
        """Generate the job cancellation queue name.

        :return: The queue name.
        """
        return "job_cancellations"

    @staticmethod
    def available_workflows_routing_key() -> str:
        """Generate the available work flows routing key.

        All available_workflows queues are expected to be bound to this routing key.

        :return: The routing key.
        """
        return "available_workflows"

    @staticmethod
    def available_workflows_queue_name(client_id: str) -> str:
        """Generate the available work flows queue name.

        :param client_id: The client id of the SDK that subscribes to this queue.

        :return: The queue name.
        """
        return f"available_workflows.{client_id}"

    @staticmethod
    def request_available_workflows_queue_name() -> str:
        """Generate the request available work flows queue name.

        :return: The queue name.
        """
        return "request_available_workflows"
