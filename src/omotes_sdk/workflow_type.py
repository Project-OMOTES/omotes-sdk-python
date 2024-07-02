import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict, Union, Any
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


@dataclass(eq=True, frozen=True)
class WorkflowParameter(ABC):
    """Define a workflow parameter this SDK supports."""

    key_name: str = field(hash=True, compare=True)
    """Key name for the parameter."""
    title: str | None = field(default=None, hash=True, compare=True)
    """Optionally override the 'snake_case to text' 'key_name' (displayed above the input field)."""
    description: str | None = field(default=None, hash=True, compare=True)
    """Optional description (displayed below the input field)."""
    type_name: str = ""
    """Parameter type name, set in child class."""

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
    def from_json_config(cls, json_config: dict) -> Self:
        """Abstract function to create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        pass

    def __post_init__(self) -> None:
        """Check parameter format."""
        for name, field_type in self.__annotations__.items():
            # Do not check dataclasses (like StringEnumOption)
            # TODO better way of checking that 'field_type' is dataclass?
            if "StringEnumOption" not in str(field_type) and not isinstance(
                self.__dict__[name], field_type
            ):
                current_type = type(self.__dict__[name])
                raise TypeError(
                    f"The field `{name}` was assigned by `{current_type}` ('{self.__dict__[name]}')"
                    f" instead of `{field_type}`"
                )


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
    default: str | None = field(default=None, hash=False, compare=False)
    """Optional default value."""
    enum_options: list[StringEnumOption] | None = field(default=None, hash=False, compare=False)
    """Optional multiple choice values."""

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
    def from_json_config(cls, json_config: dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        if "enum_options" in json_config:
            enum_options = []
            for enum_option in json_config["enum_options"]:
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


@dataclass(eq=True, frozen=True)
class BooleanParameter(WorkflowParameter):
    """Define a boolean parameter this SDK supports."""

    type_name: str = "boolean"
    """Parameter type name."""
    default: bool | None = field(default=None, hash=False, compare=False)
    """Optional default value."""

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
    def from_json_config(cls, json_config: dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        return cls(**json_config)


@dataclass(eq=True, frozen=True)
class IntegerParameter(WorkflowParameter):
    """Define an integer parameter this SDK supports."""

    type_name: str = "integer"
    """Parameter type name."""
    default: int | None = field(default=None, hash=False, compare=False)
    """Optional default value."""
    minimum: int | None = field(default=None, hash=False, compare=False)
    """Optional minimum allowed value."""
    maximum: int | None = field(default=None, hash=False, compare=False)
    """Optional maximum allowed value."""

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
    def from_json_config(cls, json_config: dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        return cls(**json_config)


@dataclass(eq=True, frozen=True)
class FloatParameter(WorkflowParameter):
    """Define a float parameter this SDK supports."""

    type_name: str = "float"
    """Parameter type name."""
    default: float | None = field(default=None, hash=False, compare=False)
    """Optional default value."""
    minimum: float | None = field(default=None, hash=False, compare=False)
    """Optional minimum allowed value."""
    maximum: float | None = field(default=None, hash=False, compare=False)
    """Optional maximum allowed value."""

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
    def from_json_config(cls, json_config: dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        return cls(**json_config)


@dataclass(eq=True, frozen=True)
class DateTimeParameter(WorkflowParameter):
    """Define a datetime parameter this SDK supports."""

    type_name: str = "datetime"
    """Parameter type name."""
    default: datetime | None = field(default=None, hash=False, compare=False)
    """Optional default value."""

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
                    f"Invalid default datetime format, should be string:"
                    f" {parameter_type_pb.default}"
                )
            except ValueError:
                raise ValueError(f"Invalid default datetime value: {parameter_type_pb.default}")
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
    def from_json_config(cls, json_config: dict) -> Self:
        """Create a class instance from json configuration.

        :param json_config: dictionary with configuration.
        :return: class instance.
        """
        return cls(**json_config)


PARAMETER_CLASS_TO_PB_CLASS: dict[
    type[WorkflowParameter],
    Union[
        type[StringParameterPb],
        type[BooleanParameterPb],
        type[IntegerParameterPb],
        type[FloatParameterPb],
        type[DateTimeParameterPb],
    ],
] = {
    StringParameter: StringParameterPb,
    BooleanParameter: BooleanParameterPb,
    IntegerParameter: IntegerParameterPb,
    FloatParameter: FloatParameterPb,
    DateTimeParameter: DateTimeParameterPb,
}


@dataclass(eq=True, frozen=True)
class WorkflowType:
    """Define a type of workflow this SDK supports."""

    workflow_type_name: str = field(hash=True, compare=True)
    """Technical name for the workflow."""
    workflow_type_description_name: str = field(hash=False, compare=False)
    """Human-readable name for the workflow."""
    workflow_parameters: List[WorkflowParameter] | None = field(
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
            workflow_parameters: list[WorkflowParameter] = []
            for parameter_pb in workflow_pb.parameters:
                parameter_type_name = parameter_pb.WhichOneof("parameter_type")
                if parameter_type_name is None:
                    raise TypeError(f"Parameter protobuf message with invalid type: {parameter_pb}")
                else:
                    one_of_parameter_type_pb = getattr(parameter_pb, parameter_type_name)

                parameter = None
                for parameter_class, parameter_pb_class in PARAMETER_CLASS_TO_PB_CLASS.items():
                    if isinstance(one_of_parameter_type_pb, parameter_pb_class):
                        parameter = parameter_class.from_pb_message(
                            parameter_pb, one_of_parameter_type_pb
                        )
                        break
                if parameter:
                    workflow_parameters.append(parameter)
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
