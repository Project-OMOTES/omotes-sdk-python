from omotes_sdk.job import Job
from omotes_sdk.workflow_type import WorkflowType


class OmotesQueueNames:
    """Container for definition of OMOTES SDK to Orchestrator queue names."""

    @staticmethod
    def job_submission_queue_name(workflow_type: WorkflowType) -> str:
        """Generate the job submission queue name given the workflow type.

        :param workflow_type: Workflow type.
        :return: The queue name.
        """
        return f"job_submissions.{workflow_type.workflow_type_name}"

    @staticmethod
    def job_results_queue_name(job: Job) -> str:
        """Generate the job results queue name given the job.

        :param job: The job.
        :return: The queue name.
        """
        return f"jobs.{job.id}.result"

    @staticmethod
    def job_progress_queue_name(job: Job) -> str:
        """Generate the job progress update queue name given the job.

        :param job: The job.
        :return: The queue name.
        """
        return f"jobs.{job.id}.progress"

    @staticmethod
    def job_status_queue_name(job: Job) -> str:
        """Generate the job status update queue name given the job.

        :param job: The job.
        :return: The queue name.
        """
        return f"jobs.{job.id}.status"

    @staticmethod
    def job_cancel_queue_name() -> str:
        """Generate the job cancellation queue name.

        :return: The queue name.
        """
        return "job_cancellations"

    @staticmethod
    def available_workflows_queue_name() -> str:
        """Generate the available work flows queue name.

        :return: The queue name.
        """
        return "available_workflows"

    @staticmethod
    def request_available_workflows_queue_name() -> str:
        """Generate the request available work flows queue name.

        :return: The queue name.
        """
        return "request_available_workflows"
