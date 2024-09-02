import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Union, Any, Type, TypeVar, cast
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
)
from google.protobuf.struct_pb2 import Struct

from omotes_sdk.types import ParamsDictValues, PBStructCompatibleTypes, ParamsDict


logger = logging.getLogger("omotes_sdk")


class WrongFieldTypeException(Exception):
    """Thrown when param_dict contains a value of the wrong type for some parameter."""

    ...


class MissingFieldException(Exception):
    """Thrown when param_dict does not contain the value for some parameter."""

    ...


@dataclass(eq=True, frozen=True)
class WorkflowParameter(ABC):
    """Define a workflow parameter this SDK supports."""

    key_name: str = field(hash=True, compare=True)
    """Key name for the parameter."""
    title: Union[str, None] = field(default=None, hash=True, compare=True)
    """Optionally override the 'snake_case to text' 'key_name' (displayed above the input field)."""
    description: Union[str, None] = field(default=None, hash=True, compare=True)
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
        ]
    ]:
        pass

    @abstractmethod
    def to_pb_message(self) -> Union[
        StringParameterPb,
        BooleanParameterPb,
        IntegerParameterPb,
        FloatParameterPb,
        DateTimeParameterPb,
    ]:
        """Abstract function to generate a protobuf message from this class.

        :return: Protobuf message representation.
        """
        pass

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
        pass

    @classmethod
    @abstractmethod
    def from_json_config(cls, json_config: Dict) -> Self:
        """Abstract function to create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        pass

    @staticmethod
    @abstractmethod
    def from_pb_value(value: PBStructCompatibleTypes) -> ParamsDictValues:
        pass

    @staticmethod
    @abstractmethod
    def to_pb_value(value: ParamsDictValues) -> PBStructCompatibleTypes:
        pass


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
    default: Union[str, None] = field(default=None, hash=False, compare=False)
    """Optional default value."""
    enum_options: Union[List[StringEnumOption], None] = field(
        default=None, hash=False, compare=False
    )
    """Optional multiple choice values."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[
        Union[
            StringParameterPb,
            BooleanParameterPb,
            IntegerParameterPb,
            FloatParameterPb,
            DateTimeParameterPb,
        ]
    ]:
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
        if isinstance(value, str):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB value as the type is {type(value)} while'
                f" a string was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> str:
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
    def get_pb_protocol_equivalent() -> Type[
        Union[
            StringParameterPb,
            BooleanParameterPb,
            IntegerParameterPb,
            FloatParameterPb,
            DateTimeParameterPb,
        ]
    ]:
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
        if isinstance(value, bool):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB value as the type is {type(value)} '
                f"while a bool was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> bool:
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
    default: Union[int, None] = field(default=None, hash=False, compare=False)
    """Optional default value."""
    minimum: Union[int, None] = field(default=None, hash=False, compare=False)
    """Optional minimum allowed value."""
    maximum: Union[int, None] = field(default=None, hash=False, compare=False)
    """Optional maximum allowed value."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[
        Union[
            StringParameterPb,
            BooleanParameterPb,
            IntegerParameterPb,
            FloatParameterPb,
            DateTimeParameterPb,
        ]
    ]:
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
        if isinstance(value, float):
            result = round(value)
            if value != result:
                logger.warning(
                    "A field was passed in workflow configuration but as a float value with decimal "
                    "instead of a rounded float. Rounding the field value from %s to %s.",
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
    default: Union[float, None] = field(default=None, hash=False, compare=False)
    """Optional default value."""
    minimum: Union[float, None] = field(default=None, hash=False, compare=False)
    """Optional minimum allowed value."""
    maximum: Union[float, None] = field(default=None, hash=False, compare=False)
    """Optional maximum allowed value."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[
        Union[
            StringParameterPb,
            BooleanParameterPb,
            IntegerParameterPb,
            FloatParameterPb,
            DateTimeParameterPb,
        ]
    ]:
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
        if isinstance(value, float):
            return value
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB value as the type is {type(value)} '
                f"while a float was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> float:
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
    default: Union[datetime, None] = field(default=None, hash=False, compare=False)
    """Optional default value."""

    @staticmethod
    def get_pb_protocol_equivalent() -> Type[
        Union[
            StringParameterPb,
            BooleanParameterPb,
            IntegerParameterPb,
            FloatParameterPb,
            DateTimeParameterPb,
        ]
    ]:
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
        if isinstance(value, float):
            return datetime.fromtimestamp(value)
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" from a PB value as the type is {type(value)} '
                f"while a float was expected."
            )

    @staticmethod
    def to_pb_value(value: ParamsDictValues) -> float:
        if isinstance(value, datetime):
            return value.timestamp()
        else:
            raise WrongFieldTypeException(
                f'Cannot convert value "{value}" to a PB value as the type is '
                f"{type(value)} while a datetime was expected."
            )


PARAMETER_CLASS_TO_PB_CLASS: Dict[
    Type[WorkflowParameter],
    Union[
        Type[StringParameterPb],
        Type[BooleanParameterPb],
        Type[IntegerParameterPb],
        Type[FloatParameterPb],
        Type[DateTimeParameterPb],
    ],
] = {
    parameter: parameter.get_pb_protocol_equivalent()
    for parameter in WorkflowParameter.__subclasses__()
}

PB_CLASS_TO_PARAMETER_CLASS: Dict[
    Union[
        Type[StringParameterPb],
        Type[BooleanParameterPb],
        Type[IntegerParameterPb],
        Type[FloatParameterPb],
        Type[DateTimeParameterPb],
    ],
    Type[WorkflowParameter],
] = {
    parameter.get_pb_protocol_equivalent(): parameter
    for parameter in WorkflowParameter.__subclasses__()
}


@dataclass(eq=True, frozen=True)
class WorkflowType:
    """Define a type of workflow this SDK supports."""

    workflow_type_name: str = field(hash=True, compare=True)
    """Technical name for the workflow."""
    workflow_type_description_name: str = field(hash=False, compare=False)
    """Human-readable name for the workflow."""
    workflow_parameters: Union[List[WorkflowParameter], None] = field(
        default=None, hash=False, compare=False
    )
    """Optional list of non-ESDL workflow parameters."""


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
            workflow_types.append(
                WorkflowType(
                    workflow_type_name=workflow_pb.type_name,
                    workflow_type_description_name=workflow_pb.type_description,
                    workflow_parameters=workflow_parameters,
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
        workflow_types = []
        for _workflow in json_config_dict:
            workflow_parameters = []
            if "workflow_parameters" in _workflow:
                for parameter_config in _workflow["workflow_parameters"]:
                    parameter_type_name = parameter_config["parameter_type"]
                    parameter_config.pop("parameter_type")

                    for parameter_type_class in PARAMETER_CLASS_TO_PB_CLASS:
                        if parameter_type_class.type_name == parameter_type_name:
                            workflow_parameters.append(
                                parameter_type_class.from_json_config(parameter_config)
                            )
                            break

            workflow_types.append(
                WorkflowType(
                    workflow_type_name=_workflow["workflow_type_name"],
                    workflow_type_description_name=_workflow["workflow_type_description_name"],
                    workflow_parameters=workflow_parameters,
                )
            )
        return cls(workflow_types)


def convert_params_dict_to_struct(workflow: WorkflowType, params_dict: ParamsDict) -> Struct:
    """Convert all values to Struct-compatible value types.

    If a value is already a Struct-compatible type, then it isn't convert.

    :param params_dict: The params dict to convert.
    :return: The protobuf Struct loaded with converted values.
    """
    normalized_dict: Dict[str, PBStructCompatibleTypes] = {}

    for parameter in workflow.workflow_parameters:
        param_value = params_dict.get(parameter.key_name)

        if param_value is None:
            raise MissingFieldException(
                f'Param with key "{parameter.key}" is missing in params_dict.'
            )

        normalized_dict[parameter.key_name] = parameter.to_pb_value(param_value)

    params_dict_struct = Struct()
    params_dict_struct.update(normalized_dict)

    return params_dict_struct


V = TypeVar("V", bound=ParamsDictValues)


def parse_workflow_config_parameter(
    workflow_config: ParamsDict,
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
    is_present = field_key in workflow_config

    result: V

    if not is_present:
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
