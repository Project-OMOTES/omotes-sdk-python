import unittest

from omotes_sdk.workflow_type import WorkflowType, WorkflowTypeManager


class WorkflowTypeTest(unittest.TestCase):
    def test__eq__same_id_but_different_description_should_still_be_same(self) -> None:
        # Arrange
        type_1 = WorkflowType("some-name", "some descript 1")
        type_2 = WorkflowType("some-name", "some descript 2")

        # Act
        is_same = type_1 == type_2

        # Assert
        self.assertTrue(is_same)

    def test__eq__different_id_but_same_description_should_not_be_same(self) -> None:
        # Arrange
        type_1 = WorkflowType("some-name-1", "some descript")
        type_2 = WorkflowType("some-name-2", "some descript")

        # Act
        is_same = type_1 == type_2

        # Assert
        self.assertFalse(is_same)

    def test__hash__same_id_but_different_descript_should_have_same_hash(self) -> None:
        # Arrange
        type_1 = WorkflowType("some-name", "some descript-1")
        type_2 = WorkflowType("some-name", "some descript-2")

        # Act
        hash_1 = hash(type_1)
        hash_2 = hash(type_2)

        # Assert
        self.assertEqual(hash_1, hash_2)

    def test__hash__same_descript_but_different_id_should_have_different_hash(self) -> None:
        # Arrange
        type_1 = WorkflowType("some-name-1", "some descript")
        type_2 = WorkflowType("some-name-2", "some descript")

        # Act
        hash_1 = hash(type_1)
        hash_2 = hash(type_2)

        # Assert
        self.assertNotEqual(hash_1, hash_2)

    def test__from_json_config_file__happy(self) -> None:
        # Arrange
        workflow_json_file = "./unit_test/test_config/workflow_config_happy.json"

        # Act
        workflow_type_manager = WorkflowTypeManager.from_json_config_file(workflow_json_file)

        # Assert
        self.assertEqual(len(workflow_type_manager.get_all_workflows()), 2)

    def test__from_json_config_file__integer_minimum_as_float(self) -> None:
        # Arrange
        workflow_json_file = "./unit_test/test_config/workflow_config_int_min_as_float.json"

        # Act / Assert
        with self.assertRaises(TypeError):
            WorkflowTypeManager.from_json_config_file(workflow_json_file)

    def test__to_pb_message__happy(self) -> None:
        # Arrange
        workflow_json_file = "./unit_test/test_config/workflow_config_happy.json"

        # Act
        workflow_type_manager = WorkflowTypeManager.from_json_config_file(workflow_json_file)
        pb_message = workflow_type_manager.to_pb_message()

        # Assert
        self.assertEqual(pb_message.workflows[0].type_name, "workflow_1")

    def test__from_pb_message__happy(self) -> None:
        # Arrange
        workflow_json_file = "./unit_test/test_config/workflow_config_happy.json"

        # Act
        workflow_type_manager = WorkflowTypeManager.from_json_config_file(workflow_json_file)
        pb_message = workflow_type_manager.to_pb_message()
        workflow_type_manager_from_pb = WorkflowTypeManager.from_pb_message(pb_message)

        # Assert
        self.assertEqual(len(workflow_type_manager_from_pb.get_all_workflows()), 2)
