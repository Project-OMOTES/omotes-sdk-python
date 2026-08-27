"""ESDL feedback message models and severity definitions."""

from enum import Enum

from pydantic import BaseModel


class MessageSeverity(Enum):
    """Message severity options."""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class EsdlMessage(BaseModel):
    """Esdl feedback message, optionally related to a specific object (asset)."""

    technical_message: str
    """Technical message."""
    severity: MessageSeverity
    """Message severity."""
    esdl_object_id: str | None = None
    """Optional esdl object id, None implies a general energy system message."""
