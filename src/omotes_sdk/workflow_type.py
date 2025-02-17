import json
import logging
import pprint
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Union, Any, Type, TypeVar, cast, Literal
from typing_extensions import Self, override

from omotes_sdk_protocol.workflow_pb2 import (
    AvailableWorkflows,
    Workflow,
    WorkflowParameter as WorkflowParameterPb,
    StringParameter as StringParameterPb,
    StringEnum as StringEnumPb,
    BooleanParameter as BooleanParameterPb,
    IntegerParameter as IntegerParameterPb,
    FloatParameter as FloatParameterPb,
    DateTimeParameter as DateTimeParameterPb,
    DurationParameter as DurationParameterPb,
    ParameterRelation,
)
from google.protobuf.struct_pb2 import Struct

from omotes_sdk.types import ParamsDictValues, PBStructCompatibleTypes, ParamsDict, ProtobufDict

logger = logging.getLogger("omotes_sdk")


class WrongFieldTypeException(Exception):
    """Thrown when param_dict contains a value of the wrong type for some parameter."""

    ...  # pragma: no cover


class MissingFieldException(Exception):
    """Thrown when param_dict does not contain the value for some parameter."""

    ...  # pragma: no cover


@dataclass(eq=True, frozen=True)
class WorkflowParameter(ABC):
    """Define a workflow parameter this SDK supports."""

    key_name: str = field(hash=True, compare=True)
    """Key name for the parameter."""
    title: Optional[str] = field(default=None, hash=True, compare=True)
    """Optionally override the 'snake_case to text' 'key_name' (displayed above the input field)."""
    description: Optional[str] = field(default=None, hash=True, compare=True)
    """Optional description (displayed below the input field)."""
    type_name: str = ""
    """Parameter type name, set in child class."""

    @staticmethod
    @abstractmethod
    def get_pb_protocol_equivalent() -> Type[
        Union[
            StringParameterPb,
            BooleanParameterPb,
            IntegerParameterPb,
            FloatParameterPb,
            DateTimeParameterPb,
            DurationParameterPb,
        ]
    ]:
        """Abstract function to link this parameter to the protobuf parameter description.

        This link is required for sharing the workflow definitions and not for params_dict
        conversions. It is used to convert the workflow parameter from the Python description
        to the protobuf definition in AvailableWorkflows.
        """
        ...  # pragma: no cover

    @abstractmethod
    def to_pb_message(self) -> Union[
        StringParameterPb,
        BooleanParameterPb,
        IntegerParameterPb,
        FloatParameterPb,
        DateTimeParameterPb,
        DurationParameterPb,
    ]:
        """Abstract function to generate a protobuf message from this class.

        :return: Protobuf message representation.
        """
        ...  # pragma: no cover

    @classmethod
    @abstractmethod
    def from_pb_message(
        cls,
        parameter_pb: WorkflowParameterPb,
        parameter_type_pb: Any,
    ) -> Self:
        """Abstract function to create a class instance from a protobuf message.

        :param parameter_pb: protobuf message containing the base parameters.
        :param parameter_type_pb: protobuf message containing the parameter type parameters.
        :return: class instance.
        """
        ...  # pragma: no cover

    @classmethod
    @abstractmethod
    def from_json_config(cls, json_config: Dict) -> Self:
        """Abstract function to create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        ...  # pragma: no cover

    @staticmethod
    @abstractmethod
    def from_pb_value(value: PBStructCompatibleTypes) -> ParamsDictValues:
        """Abstract function to deserialize the value from a protobuf struct to its original type.

        Protobuf structs do not support int, datetime or timestamps natively. This function is
        used to unpack the value from a protobuf-compatible datatype.
        """
        ...  # pragma: no cover

    @staticmethod
    @abstractmethod
    def to_pb_value(value: ParamsDictValues) -> PBStructCompatibleTypes:
        """Abstract function to serialize the value to a protobuf-struct-compatible value.

        Protobuf structs do not support int, datetime or timestamps natively. This function is
        used to unpack the value to a protobuf-compatible datatype.
        """
        ...  # pragma: no cover


@dataclass(eq=True, frozen=True)
class StringEnumOption:
    """Define a key display pair this SDK supports."""

    key_name: str = field(hash=True, compare=True)
    """Key name."""
    display_name: str = field(hash=True, compare=True)
    """Display name."""


@dataclass(eq=True, frozen=True)
class StringParameter(WorkflowParameter):
    """Define a string parameter this SDK supports."""

    type_name: str = "string"
    """Parameter type name."""
    default: Optional[str] = field(default=None, hash=False, compare=False)
    """Optional default value."""
    enum_options: Optional[List[StringEnumOption]] = field(default=None, hash=False, compare=False)
    """Optional multiple choice values."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[StringParameterPb]:
        """Link the StringParameter description to the Protobuf StringParameter class.

        :return: StringParameterPb class.
        """
        return StringParameterPb

    @override
    def to_pb_message(self) -> StringParameterPb:
        """Generate a protobuf message from this class.

        :return: Protobuf message representation.
        """
        parameter_type_pb = StringParameterPb(default=self.default)
        if self.enum_options:
            for _string_enum in self.enum_options:
                parameter_type_pb.enum_options.extend(
                    [
                        StringEnumPb(
                            key_name=_string_enum.key_name,
                            display_name=_string_enum.display_name,
                        )
                    ]
                )
        return parameter_type_pb

    @classmethod
    @override
    def from_pb_message(
        cls, parameter_pb: WorkflowParameterPb, parameter_type_pb: StringParameterPb
    ) -> Self:
        """Create a class instance from a protobuf message.

        :param parameter_pb: protobuf message containing the base parameters.
        :param parameter_type_pb: protobuf message containing the parameter type parameters.
        :return: class instance.
        """
        parameter = cls(
            key_name=parameter_pb.key_name,
            title=parameter_pb.title,
            description=parameter_pb.description,
            default=parameter_type_pb.default,
            enum_options=[],
        )
        for enum_option_pb in parameter_type_pb.enum_options:
            if parameter_type_pb.enum_options and parameter.enum_options is not None:
                parameter.enum_options.append(
                    StringEnumOption(
                        key_name=enum_option_pb.key_name,
                        display_name=enum_option_pb.display_name,
                    )
                )
        return parameter

    @classmethod
    @override
    def from_json_config(cls, json_config: Dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        if "default" in json_config and not isinstance(json_config["default"], str):
            raise TypeError("'default' for StringParameter must be in 'str' format")

        if "enum_options" in json_config and not isinstance(json_config["enum_options"], List):
            raise TypeError("'enum_options' for StringParameter must be a 'list'")

        if "enum_options" in json_config:
            enum_options = []
            for enum_option in json_config["enum_options"]:
                enum_keys = ["key_name", "display_name"]
                for enum_key in enum_keys:
                    if enum_key not in enum_option:
                        raise TypeError(f"A string enum option must contain a '{enum_key}'")
                    if enum_key in json_config and not isinstance(json_config[enum_key], str):
                        raise TypeError(
                            f"'{enum_key}' for a string enum option must be in 'str' format:"
                            f" '{json_config[enum_key]}"
                        )
                enum_options.append(
                    StringEnumOption(
                        key_name=enum_option["key_name"],
                        display_name=enum_option["display_name"],
                    )
                )
            json_config.pop("enum_options")
            return cls(**json_config, enum_options=enum_options)
        else:
            return cls(**json_config)

    @staticmethod
    def from_pb_value(value: PBStructCompatibleTypes) -> str:
        """Parse protobuf string to Python string.

        :param value: The protobuf string value to parse.
        :return: Parsed string
        """
        if isinstance(value, str):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB value as the type is {type(value)} while'
                f" a string was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> str:
        """Pack a Python string into a protobuf string.

        :param value: Python string.
        :return: The protobuf packed string.
        """
        if isinstance(value, str):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" to a PB-compatible value as the type is '
                f"{type(value)} while a string was expected."
            )


@dataclass(eq=True, frozen=True)
class BooleanParameter(WorkflowParameter):
    """Define a boolean parameter this SDK supports."""

    type_name: str = "boolean"
    """Parameter type name."""
    default: Union[bool, None] = field(default=None, hash=False, compare=False)
    """Optional default value."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[BooleanParameterPb]:
        """Link the BooleanParameter description to the Protobuf BooleanParameter class.

        :return: BooleanParameterPb class.
        """
        return BooleanParameterPb

    @override
    def to_pb_message(self) -> BooleanParameterPb:
        """Generate a protobuf message from this class.

        :return: Protobuf message representation.
        """
        return BooleanParameterPb(default=self.default)

    @classmethod
    @override
    def from_pb_message(
        cls, parameter_pb: WorkflowParameterPb, parameter_type_pb: BooleanParameterPb
    ) -> Self:
        """Create a class instance from a protobuf message.

        :param parameter_pb: protobuf message containing the base parameters.
        :param parameter_type_pb: protobuf message containing the parameter type parameters.
        :return: class instance.
        """
        return cls(
            key_name=parameter_pb.key_name,
            title=parameter_pb.title,
            description=parameter_pb.description,
            default=parameter_type_pb.default,
        )

    @classmethod
    @override
    def from_json_config(cls, json_config: Dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        if "default" in json_config and not isinstance(json_config["default"], bool):
            raise TypeError(
                f"'default' for BooleanParameter must be in 'bool' format:"
                f" '{json_config['default']}'"
            )
        return cls(**json_config)

    @staticmethod
    def from_pb_value(value: PBStructCompatibleTypes) -> bool:
        """Unpack the protobuf boolean value to a Python boolean value.

        :param value: The protobuf boolean.
        :return: Python boolean value.
        """
        if isinstance(value, bool):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB value as the type is {type(value)} '
                f"while a bool was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> bool:
        """Pack the Python boolean value into a protobuf-compatible boolean value.

        :param value: The Python boolean.
        :return: Protobuf-compatible boolean.
        """
        if isinstance(value, bool):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" to a PB-compatible value as the type is '
                f"{type(value)} while a bool was expected."
            )


@dataclass(eq=True, frozen=True)
class IntegerParameter(WorkflowParameter):
    """Define an integer parameter this SDK supports."""

    type_name: str = "integer"
    """Parameter type name."""
    default: Optional[int] = field(default=None, hash=False, compare=False)
    """Optional default value."""
    minimum: Optional[int] = field(default=None, hash=False, compare=False)
    """Optional minimum allowed value."""
    maximum: Optional[int] = field(default=None, hash=False, compare=False)
    """Optional maximum allowed value."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[IntegerParameterPb]:
        """Link the IntegerParameter description to the Protobuf IntegerParameter class.

        :return: IntegerParameterPb class.
        """
        return IntegerParameterPb

    @override
    def to_pb_message(self) -> IntegerParameterPb:
        """Generate a protobuf message from this class.

        :return: Protobuf message representation.
        """
        return IntegerParameterPb(default=self.default, minimum=self.minimum, maximum=self.maximum)

    @classmethod
    @override
    def from_pb_message(
        cls, parameter_pb: WorkflowParameterPb, parameter_type_pb: IntegerParameterPb
    ) -> Self:
        """Create a class instance from a protobuf message.

        :param parameter_pb: protobuf message containing the base parameters.
        :param parameter_type_pb: protobuf message containing the parameter type parameters.
        :return: class instance.
        """
        return cls(
            key_name=parameter_pb.key_name,
            title=parameter_pb.title,
            description=parameter_pb.description,
            default=parameter_type_pb.default,
            minimum=(
                parameter_type_pb.minimum if parameter_type_pb.HasField("minimum") else None
            ),  # protobuf has '0' default value for int instead of None
            maximum=(
                parameter_type_pb.maximum if parameter_type_pb.HasField("maximum") else None
            ),  # protobuf has '0' default value for int instead of None
        )

    @classmethod
    @override
    def from_json_config(cls, json_config: Dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        int_params = ["default", "minimum", "maximum"]
        for int_param in int_params:
            if int_param in json_config and not isinstance(json_config[int_param], int):
                raise TypeError(
                    f"'{int_param}' for IntegerParameter must be in 'int' format:"
                    f" '{json_config[int_param]}'"
                )
        return cls(**json_config)

    @staticmethod
    def from_pb_value(value: PBStructCompatibleTypes) -> int:
        """Unpack the protobuf float value into a Python integer value.

        :param value: The protobuf float value.
        :return: The Python integer value.
        """
        if isinstance(value, float):
            result = round(value)
            if value != result:
                logger.warning(
                    "A field was passed in workflow configuration but as a float value with "
                    "decimal instead of a rounded float. Rounding the field value from %s to %s.",
                    value,
                    result,
                )
            return result
        elif isinstance(value, int):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB-compatible int value '
                f"as the type is {type(value)} while an int or float was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> float:
        """Pack the Python integer into a protobuf float.

        :param value: Python integer.
        :return: Protobuf float.
        """
        if isinstance(value, int):
            return float(value)
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" to a PB-compatible value as the type is '
                f"{type(value)} while an int was expected."
            )


@dataclass(eq=True, frozen=True)
class FloatParameter(WorkflowParameter):
    """Define a float parameter this SDK supports."""

    type_name: str = "float"
    """Parameter type name."""
    default: Optional[float] = field(default=None, hash=False, compare=False)
    """Optional default value."""
    minimum: Optional[float] = field(default=None, hash=False, compare=False)
    """Optional minimum allowed value."""
    maximum: Optional[float] = field(default=None, hash=False, compare=False)
    """Optional maximum allowed value."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[FloatParameterPb]:
        """Link this FloatParameter class to the FloatParameter protobuf class.

        :return: The FloatParameterPb class.
        """
        return FloatParameterPb

    @override
    def to_pb_message(self) -> FloatParameterPb:
        """Generate a protobuf message from this class.

        :return: Protobuf message representation.
        """
        return FloatParameterPb(default=self.default, minimum=self.minimum, maximum=self.maximum)

    @classmethod
    @override
    def from_pb_message(
        cls, parameter_pb: WorkflowParameterPb, parameter_type_pb: FloatParameterPb
    ) -> Self:
        """Create a class instance from a protobuf message.

        :param parameter_pb: protobuf message containing the base parameters.
        :param parameter_type_pb: protobuf message containing the parameter type parameters.
        :return: class instance.
        """
        return cls(
            key_name=parameter_pb.key_name,
            title=parameter_pb.title,
            description=parameter_pb.description,
            default=parameter_type_pb.default,
            minimum=(
                parameter_type_pb.minimum if parameter_type_pb.HasField("minimum") else None
            ),  # protobuf has '0' default value for int instead of None
            maximum=(
                parameter_type_pb.maximum if parameter_type_pb.HasField("maximum") else None
            ),  # protobuf has '0' default value for int instead of None
        )

    @classmethod
    @override
    def from_json_config(cls, json_config: Dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        float_params = ["default", "minimum", "maximum"]
        for float_param in float_params:
            if (
                float_param in json_config
                and not isinstance(json_config[float_param], float)
                and not isinstance(json_config[float_param], int)
            ):
                raise TypeError(
                    f"'{float_param}' for FloatParameter must be in 'float' format:"
                    f" '{json_config[float_param]}'"
                )

        return cls(**json_config)

    @staticmethod
    def from_pb_value(value: PBStructCompatibleTypes) -> float:
        """Unpack the Python float from a protobuf float.

        :param value: Protobuf float.
        :return: Python float.
        """
        if isinstance(value, float):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB value as the type is {type(value)} '
                f"while a float was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> float:
        """Pack the Python float into a protobuf float.

        :param value: Python float.
        :return: Protobuf float.
        """
        if isinstance(value, float):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" to a PB value as the type is '
                f"{type(value)} while an int was expected."
            )


@dataclass(eq=True, frozen=True)
class DateTimeParameter(WorkflowParameter):
    """Define a datetime parameter this SDK supports."""

    type_name: str = "datetime"
    """Parameter type name."""
    default: Optional[datetime] = field(default=None, hash=False, compare=False)
    """Optional default value."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[DateTimeParameterPb]:
        """Link the DateTimeParameter class to the protobuf DateTimeParameter class.

        :return: The DateTimeParameterPb class.
        """
        return DateTimeParameterPb

    @override
    def to_pb_message(self) -> DateTimeParameterPb:
        """Generate a protobuf message from this class.

        :return: Protobuf message representation.
        """
        if self.default is None:
            default_value = None
        else:
            default_value = self.default.isoformat()
        return DateTimeParameterPb(default=default_value)

    @classmethod
    @override
    def from_pb_message(
        cls, parameter_pb: WorkflowParameterPb, parameter_type_pb: DateTimeParameterPb
    ) -> Self:
        """Create a class instance from a protobuf message.

        :param parameter_pb: protobuf message containing the base parameters.
        :param parameter_type_pb: protobuf message containing the parameter type parameters.
        :return: class instance.
        """
        if parameter_type_pb.HasField("default"):
            try:
                default = datetime.fromisoformat(parameter_type_pb.default)
            except TypeError:
                raise TypeError(
                    f"Invalid default datetime format, should be a string in ISO format:"
                    f" {parameter_type_pb.default}"
                )
        else:
            default = None
        return cls(
            key_name=parameter_pb.key_name,
            title=parameter_pb.title,
            description=parameter_pb.description,
            default=default,
        )

    @classmethod
    @override
    def from_json_config(cls, json_config: Dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        if "default" in json_config:
            try:
                default = datetime.fromisoformat(json_config["default"])
            except TypeError:
                raise TypeError(
                    f"Invalid default datetime format, should be a string in ISO format:"
                    f" '{json_config['default']}'"
                )
            json_config["default"] = default

        return cls(**json_config)

    @staticmethod
    def from_pb_value(value: PBStructCompatibleTypes) -> datetime:
        """Unpack a Python datetime from a protobuf float.

        :param value: The protobuf float which is a packed Python datetime.
        :return: The Python datetime.
        """
        if isinstance(value, float):
            return datetime.fromtimestamp(value)
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB value as the type is {type(value)} '
                f"while a float was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> float:
        """Pack the Python datetime into a protobuf-compatible float.

        :param value: The Python datetime.
        :return: The packed datetime as a protobuf-compatible float.
        """
        if isinstance(value, datetime):
            return value.timestamp()
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" to a PB value as the type is '
                f"{type(value)} while a datetime was expected."
            )


@dataclass(eq=True, frozen=True)
class DurationParameter(WorkflowParameter):
    """Define a datetime parameter this SDK supports."""

    type_name: str = "duration"
    """Parameter type name."""
    default: Optional[timedelta] = field(default=None, hash=False, compare=False)
    """Optional default value."""
    minimum: Optional[timedelta] = field(default=None, hash=False, compare=False)
    """Optional minimum allowed value."""
    maximum: Optional[timedelta] = field(default=None, hash=False, compare=False)
    """Optional maximum allowed value."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[DurationParameterPb]:
        """Link the DurationParameter class to the protobuf DurationParameter class.

        :return: The DurationParameterPb class.
        """
        return DurationParameterPb

    @override
    def to_pb_message(self) -> DurationParameterPb:
        """Generate a protobuf message from this class.

        :return: Protobuf message representation.
        """
        return DurationParameterPb(
            default=None if self.default is None else self.default.total_seconds(),
            minimum=None if self.minimum is None else self.minimum.total_seconds(),
            maximum=None if self.maximum is None else self.maximum.total_seconds(),
        )

    @classmethod
    @override
    def from_pb_message(
        cls, parameter_pb: WorkflowParameterPb, parameter_type_pb: DurationParameterPb
    ) -> Self:
        """Create a class instance from a protobuf message.

        :param parameter_pb: protobuf message containing the base parameters.
        :param parameter_type_pb: protobuf message containing the parameter type parameters.
        :return: class instance.
        """
        return cls(
            key_name=parameter_pb.key_name,
            title=parameter_pb.title,
            description=parameter_pb.description,
            default=(
                timedelta(seconds=parameter_type_pb.default)
                if parameter_type_pb.HasField("default")
                else None
            ),
            minimum=(
                timedelta(seconds=parameter_type_pb.minimum)
                if parameter_type_pb.HasField("minimum")
                else None
            ),
            maximum=(
                timedelta(seconds=parameter_type_pb.maximum)
                if parameter_type_pb.HasField("maximum")
                else None
            ),
        )

    @classmethod
    @override
    def from_json_config(cls, json_config: Dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        duration_params = ["default", "minimum", "maximum"]
        args = {
            "key_name": json_config["key_name"],
            "title": json_config.get("title"),
            "description": json_config.get("description"),
        }
        for duration_param in duration_params:
            if duration_param in json_config and not isinstance(
                json_config[duration_param], (int, float)
            ):
                raise TypeError(
                    f"'{duration_param}' for DurationParameter must be a number in seconds:"
                    f" '{json_config[duration_param]}'"
                )
            elif duration_param in json_config:
                args[duration_param] = timedelta(seconds=json_config[duration_param])

        return cls(**args)

    @staticmethod
    def from_pb_value(value: PBStructCompatibleTypes) -> timedelta:
        """Unpack a Python timedelta from a protobuf float.

        :param value: The protobuf int which is a packed Python timedelta.
        :return: The Python timedelta.
        """
        if isinstance(value, (float, int)):
            return timedelta(seconds=value)
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB value as the type is {type(value)} '
                f"while a float or int was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> float:
        """Pack the Python timedelta into a protobuf-compatible float.

        :param value: The timedelta datetime.
        :return: The packed timedelta as a protobuf-compatible float.
        """
        if isinstance(value, timedelta):
            return value.total_seconds()
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" to a PB value as the type is '
                f"{type(value)} while a timedelta was expected."
            )


PARAMETER_CLASS_TO_PB_CLASS: Dict[
    Type[WorkflowParameter],
    Union[
        Type[StringParameterPb],
        Type[BooleanParameterPb],
        Type[IntegerParameterPb],
        Type[FloatParameterPb],
        Type[DateTimeParameterPb],
        Type[DurationParameterPb],
    ],
] = {
    parameter: parameter.get_pb_protocol_equivalent()  # type: ignore[type-abstract]
    for parameter in WorkflowParameter.__subclasses__()
}

PB_CLASS_TO_PARAMETER_CLASS: Dict[
    Union[
        Type[StringParameterPb],
        Type[BooleanParameterPb],
        Type[IntegerParameterPb],
        Type[FloatParameterPb],
        Type[DateTimeParameterPb],
        Type[DurationParameterPb],
    ],
    Type[WorkflowParameter],
] = {
    parameter.get_pb_protocol_equivalent(): parameter  # type: ignore[type-abstract]
    for parameter in WorkflowParameter.__subclasses__()
}

List["ParamsDictValues"], "ParamsDict", None, float, int, str, bool, datetime, timedelta


def check_parameter_relation(
    value1: ParamsDictValues, value2: ParamsDictValues, check: ParameterRelation
) -> Literal[True]:
    """Check if the values adhere to the parameter relation.

    :param value1: The left-hand value to be checked.
    :param value2: The right-hand value to the checked.
    :param check: The parameter relation to check between `value1` and `value2`
    :return: Always true if the function returns noting the parameter relation is adhered to.
    :raises RuntimeError: In case the parameter relation is not adhered to.
    """
    supported_types = (float, int, datetime, timedelta)
    if not isinstance(value1, supported_types) or not isinstance(value2, supported_types):
        raise RuntimeError(
            f"Values {value1}, {value2} are of a type that are not supported "
            f"by parameter relation {check}"
        )

    same_type_required = (datetime, timedelta)
    if (isinstance(value1, same_type_required) or isinstance(value2, same_type_required)) and type(
        value1
    ) is not type(value2):
        raise RuntimeError(
            f"Values {value1}, {value2} are required to be of the same type to be"
            f"supported by parameter relation {check}"
        )

    if check.relation == ParameterRelation.RelationType.GREATER:
        result = value1 > value2  # type: ignore[operator]
    elif check.relation == ParameterRelation.RelationType.GREATER_OR_EQ:
        result = value1 >= value2  # type: ignore[operator]
    elif check.relation == ParameterRelation.RelationType.SMALLER:
        result = value1 < value2  # type: ignore[operator]
    elif check.relation == ParameterRelation.RelationType.SMALLER_OR_EQ:
        result = value1 <= value2  # type: ignore[operator]
    else:
        raise RuntimeError("Unknown parameter relation. Please implement.")

    if not result:
        raise RuntimeError(
            f"Check failed for relation {check.relation} with "
            f"{check.key_1}: {value1} and  {check.key_2}: {value2}"
        )
    return result


def convert_str_to_parameter_relation(
    parameter_relation_name: str,
) -> ParameterRelation.RelationType.ValueType:
    """Translate the name of a parameter relation to the relevant enum.

    :param parameter_relation_name: String name of the parameter relation.
    :return: The parameter relation as an enum value of `ParameterRelation.RelationType`
    :raises RuntimeError: In case the parameter relation name is unknown.
    """
    normalized_relation_name = parameter_relation_name.lower()
    if normalized_relation_name == "greater":
        result = ParameterRelation.RelationType.GREATER
    elif normalized_relation_name == "greater_or_eq":
        result = ParameterRelation.RelationType.GREATER_OR_EQ
    elif normalized_relation_name == "smaller":
        result = ParameterRelation.RelationType.SMALLER
    elif normalized_relation_name == "smaller_or_eq":
        result = ParameterRelation.RelationType.SMALLER_OR_EQ
    else:
        raise RuntimeError(f"Unknown parameter relation name {parameter_relation_name}")

    return result


def convert_json_to_parameter_relation(parameter_relation_json: dict) -> ParameterRelation:
    """Convert a json document containing a parameter relation definition to a `ParameterRelation`.

    :param parameter_relation_json: The json document which contains the parameter relation
        definition.
    :return: The converted parameter relation definition.
    """
    return ParameterRelation(
        key_1=parameter_relation_json["key_1"],
        key_2=parameter_relation_json["key_2"],
        relation=convert_str_to_parameter_relation(parameter_relation_json["relation"]),
    )


@dataclass(eq=True, frozen=True)
class WorkflowType:
    """Define a type of workflow this SDK supports."""

    workflow_type_name: str = field(hash=True, compare=True)
    """Technical name for the workflow."""
    workflow_type_description_name: str = field(hash=False, compare=False)
    """Human-readable name for the workflow."""
    workflow_parameters: Optional[List[WorkflowParameter]] = field(
        default=None, hash=False, compare=False
    )
    """Optional list of non-ESDL workflow parameters."""
    parameter_relations: Optional[List[ParameterRelation]] = field(
        default=None, hash=False, compare=False
    )


class WorkflowTypeManager:
    """Container for all possible workflows."""

    _workflows: Dict[str, WorkflowType]
    """The possible workflows this SDK supports."""

    def __init__(self, possible_workflows: List[WorkflowType]):
        """Create the workflow type manager.

        :param possible_workflows: The workflows to manage.
        """
        self._workflows = {workflow.workflow_type_name: workflow for workflow in possible_workflows}

    def get_workflow_by_name(self, name: str) -> Optional[WorkflowType]:
        """Find the workflow type using the name.

        :param name: Name of the workflow type to find.
        :return: The workflow type if it exists.
        """
        return self._workflows.get(name)

    def get_all_workflows(self) -> List[WorkflowType]:
        """List all workflows.

        :return: The workflows.
        """
        return list(self._workflows.values())

    def workflow_exists(self, workflow: WorkflowType) -> bool:
        """Check if the workflow exists within this manager.

        :param workflow: Check if this workflow exists within the manager.
        :return: If the workflow exists.
        """
        return workflow.workflow_type_name in self._workflows

    def to_pb_message(self) -> AvailableWorkflows:
        """Generate a protobuf message containing the available workflows.

        :return: AvailableWorkflows protobuf message.
        """
        available_workflows_pb = AvailableWorkflows()
        for _workflow in self._workflows.values():
            workflow_pb = Workflow(
                type_name=_workflow.workflow_type_name,
                type_description=_workflow.workflow_type_description_name,
                relations=_workflow.parameter_relations,
            )
            if _workflow.workflow_parameters:
                for _parameter in _workflow.workflow_parameters:
                    parameter_pb = WorkflowParameterPb(
                        key_name=_parameter.key_name,
                        title=_parameter.title,
                        description=_parameter.description,
                    )
                    parameter_type_to_pb_type_oneof = {
                        StringParameter: parameter_pb.string_parameter,
                        BooleanParameter: parameter_pb.boolean_parameter,
                        IntegerParameter: parameter_pb.integer_parameter,
                        FloatParameter: parameter_pb.float_parameter,
                        DateTimeParameter: parameter_pb.datetime_parameter,
                        DurationParameter: parameter_pb.duration_parameter,
                    }
                    for (
                        parameter_type_class,
                        parameter_type_oneof,
                    ) in parameter_type_to_pb_type_oneof.items():
                        if isinstance(_parameter, parameter_type_class):
                            parameter_type_oneof.CopyFrom(_parameter.to_pb_message())
                            break
                    workflow_pb.parameters.extend([parameter_pb])
            available_workflows_pb.workflows.extend([workflow_pb])
        return available_workflows_pb

    @classmethod
    def from_pb_message(cls, available_workflows_pb: AvailableWorkflows) -> Self:
        """Create a WorkflowTypeManager instance from a protobuf message.

        :param available_workflows_pb: protobuf message containing the available workflows.
        :return: WorkflowTypeManager instance.
        """
        workflow_types = []
        workflow_pb: Workflow
        for workflow_pb in available_workflows_pb.workflows:
            workflow_parameters: List[WorkflowParameter] = []
            for parameter_pb in workflow_pb.parameters:
                parameter_type_name = parameter_pb.WhichOneof("parameter_type")
                if parameter_type_name is None:
                    raise TypeError(f"Parameter protobuf message with invalid type: {parameter_pb}")
                else:
                    one_of_parameter_type_pb = getattr(parameter_pb, parameter_type_name)

                parameter_class = PB_CLASS_TO_PARAMETER_CLASS.get(type(one_of_parameter_type_pb))

                if parameter_class:
                    parameter = parameter_class.from_pb_message(
                        parameter_pb, one_of_parameter_type_pb
                    )
                    workflow_parameters.append(parameter)
                else:
                    raise RuntimeError(f"Unknown PB class {type(one_of_parameter_type_pb)}")

            if workflow_pb.relations is None:
                parameter_relations = []
            else:
                parameter_relations = list(workflow_pb.relations)

            workflow_types.append(
                WorkflowType(
                    workflow_type_name=workflow_pb.type_name,
                    workflow_type_description_name=workflow_pb.type_description,
                    workflow_parameters=workflow_parameters,
                    parameter_relations=parameter_relations,
                )
            )
        return cls(workflow_types)

    @classmethod
    def from_json_config_file(cls, json_config_file_path: str) -> Self:
        """Create a WorkflowTypeManager instance from a json configuration file.

        :param json_config_file_path: path to the json workflow configuration file.
        :return: WorkflowTypeManager instance.
        """
        with open(json_config_file_path, "r") as f:
            json_config_dict = json.load(f)
        logger.debug("Loading workflow config: %s", pprint.pformat(json_config_dict))
        workflow_types = []
        for _workflow in json_config_dict:
            workflow_parameters = []
            for parameter_config in _workflow.get("workflow_parameters", []):
                parameter_type_name = parameter_config["parameter_type"]
                parameter_config.pop("parameter_type")

                for parameter_type_class in PARAMETER_CLASS_TO_PB_CLASS:
                    if parameter_type_class.type_name == parameter_type_name:
                        workflow_parameters.append(
                            parameter_type_class.from_json_config(parameter_config)
                        )
                        break

            parameter_relations = []
            for relation_json in _workflow.get("parameter_relations", []):
                parameter_relations.append(convert_json_to_parameter_relation(relation_json))

            workflow_types.append(
                WorkflowType(
                    workflow_type_name=_workflow["workflow_type_name"],
                    workflow_type_description_name=_workflow["workflow_type_description_name"],
                    workflow_parameters=workflow_parameters,
                    parameter_relations=parameter_relations,
                )
            )
        return cls(workflow_types)


def convert_params_dict_to_struct(workflow: WorkflowType, params_dict: ParamsDict) -> Struct:
    """Convert all values to Struct-compatible value types.

    If a value is already a Struct-compatible type, then it isn't convert.

    :param workflow: The description of the workflow which contains the description of the
        expected params_dict.
    :param params_dict: The params dict to convert.
    :return: The protobuf Struct loaded with converted values.
    """
    normalized_dict: Dict[str, PBStructCompatibleTypes] = {}
    workflow_parameters = (
        [] if workflow.workflow_parameters is None else workflow.workflow_parameters
    )
    for parameter in workflow_parameters:
        param_value = params_dict.get(parameter.key_name)

        if param_value is None:
            raise MissingFieldException(
                f'Param with key "{parameter.key_name}" is missing in params_dict.'
            )

        normalized_dict[parameter.key_name] = parameter.to_pb_value(param_value)

    if workflow.parameter_relations is not None:
        for relation in workflow.parameter_relations:
            value1 = params_dict[relation.key_1]
            value2 = params_dict[relation.key_2]

            check_parameter_relation(value1, value2, relation)

    params_dict_struct = Struct()
    params_dict_struct.update(normalized_dict)

    return params_dict_struct


V = TypeVar("V", bound=ParamsDictValues)


def parse_workflow_config_parameter(
    workflow_config: ProtobufDict,
    field_key: str,
    expected_type: Type[WorkflowParameter],
    default_value: Optional[V],
) -> V:
    """Parse the workflow config parameter according to the expected key and type.

    If either the key is missing or the value has the wrong type, the default value is used
    if available.

    :param workflow_config: The workflow config to parse the field from.
    :param field_key: The key or name of the variable in workflow_config.
    :param expected_type: The expected workflow parameter type of the value.
    :param default_value: In case the key is missing or cannot be parsed properly, this value is
        used instead.
    :raises WrongFieldTypeException: If the key is available but has the wrong type and no default
        value is available, this exception is thrown.
    :raises MissingFieldException: If the key is missing and no default value is available,
        this exception is thrown.
    :return: The value for the key or the default value.
    """
    maybe_value = workflow_config.get(field_key)
    of_type = type(maybe_value)

    parsed_value: V

    if maybe_value is None:
        if default_value is not None:
            logger.warning(
                "%s field was missing in workflow configuration. Using default value %s",
                field_key,
                default_value,
            )
            parsed_value = default_value
        else:
            logger.error(
                "%s field was missing in workflow configuration. No default available.", field_key
            )
            raise MissingFieldException()
    else:
        try:
            parsed_value = cast(V, expected_type.from_pb_value(maybe_value))
        except WrongFieldTypeException:
            if default_value is not None:
                logger.warning(
                    "%s field was passed in workflow configuration but as a %s instead of %s. "
                    "Using default value %d",
                    field_key,
                    of_type,
                    expected_type,
                    default_value,
                )
                parsed_value = default_value
            else:
                logger.error(
                    "%s field was passed in workflow configuration but as a %s instead of %s. "
                    "No default available.",
                    field_key,
                    of_type,
                    expected_type,
                )
                raise

    return parsed_value
