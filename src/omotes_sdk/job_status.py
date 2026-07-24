from enum import Enum, auto


class JobStatus(str, Enum):  # noqa: UP042
    """Possible job status."""

    @staticmethod
    def _generate_next_value_(name: str, start: int, count: int, last_values: list[object]) -> str:
        return name

    REGISTERED = auto()
    """Job is registered but not yet submitted ."""
    ENQUEUED = auto()
    """Job is submitted but not yet started."""
    RUNNING = auto()
    """Job is started and waiting to complete."""
    SUCCEEDED = auto()
    """Job is finished successfully."""
    CANCELLED = auto()
    """Job was cancelled."""
    TIMEOUT = auto()
    """Job ended due to a timeout."""
    ERROR = auto()
    """Job ended due to an error."""
