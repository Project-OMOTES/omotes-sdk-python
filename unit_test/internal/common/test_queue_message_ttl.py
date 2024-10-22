import unittest
from datetime import timedelta
from omotes_sdk.internal.common.broker_interface import QueueTTLArguments


class TestQueueMessageTTLArguments(unittest.TestCase):
    def test__to_argument__no_arguments(self) -> None:
        # Arrange / Act
        args = QueueTTLArguments()

        # Assert
        self.assertEqual(args.to_argument(), {})

    def test__to_argument__queue_ttl(self) -> None:
        # Arrange
        q_ttl = timedelta(seconds=60)

        # Act
        args = QueueTTLArguments(queue_ttl=q_ttl)

        # Assert
        self.assertEqual(args.to_argument(), {"x-expires": 60000})

    def test__to_argument__negative_queue_ttl(self) -> None:
        # Arrange
        q_ttl = timedelta(seconds=-60)

        # Act / Assert
        with self.assertRaises(ValueError):
            QueueTTLArguments(queue_ttl=q_ttl).to_argument()

    def test__to_argument__zero_queue_ttl(self) -> None:
        # Arrange
        q_ttl = timedelta(seconds=0)

        # Act / Assert
        with self.assertRaises(ValueError):
            QueueTTLArguments(queue_ttl=q_ttl).to_argument()
