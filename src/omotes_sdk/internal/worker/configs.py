import os
from dataclasses import dataclass

from omotes_sdk.config import RabbitMQConfig
from omotes_sdk.internal.common.config import (
    EnvRabbitMQConfig,
)


@dataclass
class WorkerConfig:
    """Base configuration for an OMOTES Worker.

    Values are retrieved from environment variables.
    """

    rabbitmq_config: RabbitMQConfig
    """How to connect to the OMOTES Celery RabbitMQ."""
    task_result_queue_name: str
    """Name of the queue to which the task result should be send."""
    task_progress_queue_name: str
    """Name of the queue to which progress updates for the task should be send."""
    log_level: str
    """Log level for any logging in the worker."""
    max_tasks_before_restart: int
    """The number of tasks (runs) after which the celery worker will be restarted."""

    def __init__(self) -> None:
        """Create the worker config and retrieve values from environment variables."""
        self.rabbitmq_config = EnvRabbitMQConfig()
        self.task_result_queue_name = os.environ.get(
            "TASK_RESULT_QUEUE_NAME", "omotes_task_result_events"
        )
        self.task_progress_queue_name = os.environ.get(
            "TASK_PROGRESS_QUEUE_NAME", "omotes_task_progress_events"
        )
        self.log_level = os.environ.get("LOG_LEVEL", "INFO")
        self.max_tasks_before_restart = int(os.environ.get("MAX_TASKS_BEFORE_RESTART", 0))
