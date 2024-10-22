import asyncio
import logging
from asyncio import Task
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum
from functools import partial
import threading
from types import TracebackType
from typing import Callable, Optional, Dict, Type, TypedDict, cast
from datetime import timedelta

from aio_pika import connect_robust, Message, DeliveryMode
from aio_pika.abc import (
    AbstractRobustConnection,
    AbstractChannel,
    AbstractQueue,
    AbstractIncomingMessage,
    AbstractExchange,
)
from aio_pika.exceptions import ChannelClosed
from pamqp.common import Arguments

from omotes_sdk.config import RabbitMQConfig

logger = logging.getLogger("omotes_sdk")


@dataclass
class QueueSubscriptionConsumer:
    """Consumes a queue until stopped and forwards received messages using a callback."""

    queue: AbstractQueue
    """The queue to which is subscribed."""
    delete_after_messages: Optional[int]
    """Delete the subscription & queue after successfully processing this amount of messages"""
    callback_on_message: Callable[[bytes], None]
    """Callback which is called on each message."""

    async def run(self) -> None:
        """Retrieve messages from the AMQP queue and run callback on each message.

        Requeue in case the handler generates an exception. Callback is a synchronous
        function which is executed from the executor threadpool.
        """
        number_of_messages_processed = 0
        delete_queue = False
        async with self.queue.iterator() as queue_iter:
            message: AbstractIncomingMessage
            async for message in queue_iter:
                async with message.process(requeue=True):
                    logger.debug(
                        "Received with queue subscription on queue %s: %s",
                        self.queue.name,
                        message.body,
                    )
                    await asyncio.get_running_loop().run_in_executor(
                        None, partial(self.callback_on_message, message.body)
                    )
                    number_of_messages_processed += 1

                    if (
                        self.delete_after_messages is not None
                        and number_of_messages_processed >= self.delete_after_messages
                    ):
                        logger.debug(
                            "Successful message threshold %s reached for queue %s. "
                            "Stopping subscription and deleting queue.",
                            self.delete_after_messages,
                            self.queue.name,
                        )
                        delete_queue = True
                        break
        logger.debug(
            "Stopping subscription on queue %s. Processed %s messages.",
            self.queue.name,
            number_of_messages_processed,
        )

        if delete_queue:
            logger.debug("Deleting queue %s", self.queue.name)
            await self.queue.delete()


class AioPikaQueueTypeArguments(TypedDict, total=False):
    """The keyword arguments which may be used in aio-pika `AbstractChannel.declare_queue`."""

    exclusive: bool
    auto_delete: bool
    durable: bool


class AMQPQueueType(Enum):
    """The RabbitMQ AMQP queue types."""

    EXCLUSIVE = "exclusive"
    AUTO_DELETE = "auto_delete"
    DURABLE = "durable"

    def to_argument(self) -> AioPikaQueueTypeArguments:
        """Convert the RabbitMQ AMQP queue type to the aio-pika `declare_queue` keyword arguments.

        :return: The keyword arguments which may be used in `AbstractChannel.declare_queue` of the
            aio-pika library.
        """
        result = AioPikaQueueTypeArguments()
        if self == AMQPQueueType.EXCLUSIVE:
            result["exclusive"] = True
        elif self == AMQPQueueType.AUTO_DELETE:
            result["auto_delete"] = True
        elif self == AMQPQueueType.DURABLE:
            result["durable"] = True
        else:
            raise RuntimeError(f"Unknown AMQP queue type {self}")

        return result


@dataclass()
class QueueTTLArguments():
    """Construct additional time-to-live arguments when declaring a queue."""

    queue_ttl: Optional[timedelta] = None
    """Expires and deletes the queue after a period of time when it is unused.
    The timedelta must be convertible into a positive integer.
    Ref: https://www.rabbitmq.com/docs/ttl#queue-ttl"""

    def to_argument(self) -> Arguments:
        """Convert the time-to-live variables to the aio-pika `declare_queue` keyword arguments.

        :return: The time-to-live keyword arguments in AMQP method arguments data type.
        """
        arguments: Arguments = {}
        # Ensure this is not None to avoid typecheck error.
        arguments = cast(dict, arguments)

        if self.queue_ttl is not None:
            if self.queue_ttl <= timedelta(0):
                raise ValueError("queue_ttl must be a positive value, "
                                 + f"{self.queue_ttl} received.")
            arguments["x-expires"] = int(self.queue_ttl.total_seconds() * 1000)

        return arguments


class BrokerInterface(threading.Thread):
    """Interface to RabbitMQ using aiopika."""

    TIMEOUT_ON_STOP_SECONDS: int = 5
    """How long to wait till the broker connection has stopped before killing it non-gracefully."""

    config: RabbitMQConfig
    """The broker configuration."""

    _loop: asyncio.AbstractEventLoop
    """The eventloop in which all broker-related async traffic is run."""
    _connection: AbstractRobustConnection
    """AMQP connection."""
    _channel: AbstractChannel
    """AMQP channel."""
    _exchanges: Dict[str, AbstractExchange]
    """Exchanges declared in this connection."""
    _queue_subscription_consumer_by_name: Dict[str, QueueSubscriptionConsumer]
    """Task to consume messages when they are received ordered by queue name."""
    _queue_subscription_tasks: Dict[str, Task]
    """Reference to the queue subscription task by queue name."""
    _ready_for_processing: threading.Event
    """Thread-safe check which is set once the AMQP connection is up and running."""
    _stopping_lock: threading.Lock
    """Lock to make sure only a single thread is stopping this interface."""
    _stopping: bool
    """Value to check if a thread is stopping this interface."""
    _stopped: bool
    """Value to check if this interface has successfully stopped."""

    def __init__(self, config: RabbitMQConfig):
        """Create the BrokerInterface.

        :param config: Configuration to connect to RabbitMQ.
        """
        super().__init__()
        self.config = config

        self._exchanges = {}
        self._queue_subscription_consumer_by_name = {}
        self._queue_subscription_tasks = {}
        self._ready_for_processing = threading.Event()
        self._stopping_lock = threading.Lock()
        self._stopping = False
        self._stopped = False

    def __enter__(self) -> "BrokerInterface":
        """Start the interface when it is called as a context manager."""
        self.start()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Stop the interface when it is called as a context manager."""
        self.stop()

    async def _declare_exchange(self, exchange_name: str) -> None:
        """Declare an exchange on which messages may be published and routed to queues.

        :param exchange_name: Name of the exchange.
        """
        new_exchange = await self._channel.declare_exchange(exchange_name)
        self._exchanges[exchange_name] = new_exchange

    async def _send_message_to(
        self, exchange_name: Optional[str], routing_key: str, message: bytes
    ) -> None:
        """Publish a message to a specific routing_key.

        :param exchange_name: The name of the exchange on which to publish the message. Must be
            declared before publishing is possible. If None, the default exchange is used which is
            already declared at startup
        :param routing_key: The routing key to publish the message to. This may be a specific
            routing key to which multiple queues are bound or the queue name (default routing key).
        :param message: The message to publish.
        """
        amqp_message = Message(body=message, delivery_mode=DeliveryMode.PERSISTENT)

        if exchange_name is None:
            logger.debug(
                "Sending a message to default exchange using routing key %s containing: %s",
                routing_key,
                message,
            )
            await self._channel.default_exchange.publish(amqp_message, routing_key=routing_key)
        elif exchange_name not in self._exchanges:
            raise RuntimeError(
                f"Unable to send message with routing key {routing_key} to exchange "
                f"{exchange_name} as it hasn't been declared yet"
            )
        else:
            logger.debug(
                "Sending a message to exchange %s using routing key %s containing: %s",
                exchange_name,
                routing_key,
                message,
            )
            await self._exchanges[exchange_name].publish(amqp_message, routing_key=routing_key)

    async def _declare_queue(
        self,
        queue_name: str,
        queue_type: AMQPQueueType,
        bind_to_routing_key: Optional[str] = None,
        exchange_name: Optional[str] = None,
        queue_ttl: Optional[QueueTTLArguments] = None
    ) -> AbstractQueue:
        """Declare an AMQP queue.

        :param queue_name: Name of the queue to declare.
        :param queue_type: Declare the queue using one of the known queue types.
        :param bind_to_routing_key: Bind the queue to this routing key next to the default routing
            key of the queue name. If none, the queue is only bound to the name of the queue.
            If not none, then the exchange_name must be set as well.
        :param exchange_name: Name of the exchange on which the messages will be published.
        :param queue_ttl: Additional queue TTL arguments.
        """
        if bind_to_routing_key is not None and exchange_name is None:
            raise RuntimeError(
                f"Routing key for binding was set to {bind_to_routing_key} but no "
                f"exchange name was provided."
            )

        if queue_ttl is not None:
            ttl_arguments = queue_ttl.to_argument()
        else:
            ttl_arguments = None

        logger.info("Declaring queue %s as %s with arguments as %s",
                    queue_name,
                    queue_type,
                    ttl_arguments)
        queue = await self._channel.declare_queue(queue_name,
                                                  **queue_type.to_argument(),
                                                  arguments=ttl_arguments)

        if exchange_name is not None:
            if exchange_name not in self._exchanges:
                raise RuntimeError(
                    f"Exchange {exchange_name} was not yet declared by this connection."
                )
            exchange = self._exchanges[exchange_name]
            logger.info("Binding queue %s to routing key %s", queue_name, bind_to_routing_key)
            await queue.bind(exchange=exchange, routing_key=bind_to_routing_key)

        return queue

    async def _declare_queue_and_add_subscription(
        self,
        queue_name: str,
        callback_on_message: Callable[[bytes], None],
        queue_type: AMQPQueueType,
        bind_to_routing_key: Optional[str] = None,
        exchange_name: Optional[str] = None,
        delete_after_messages: Optional[int] = None,
        queue_ttl: Optional[QueueTTLArguments] = None
    ) -> None:
        """Declare an AMQP queue and subscribe to the messages.

        :param queue_name: Name of the queue to declare.
        :param callback_on_message: Callback which is called from a separate thread to process the
            message body.
        :param queue_type: Declare the queue using one of the known queue types.
        :param bind_to_routing_key: Bind the queue to this routing key next to the default routing
            key of the queue name. If none, the queue is only bound to the name of the queue.
            If not none, then the exchange_name must be set as well.
        :param exchange_name: Name of the exchange on which the messages will be published.
        :param delete_after_messages: Delete the subscription & queue after this limit of messages
            have been successfully processed.
        :param queue_ttl: Additional queue TTL arguments.
        """
        if queue_name in self._queue_subscription_consumer_by_name:
            logger.error(
                "Attempting to declare a subscription on %s but a "
                "subscription on this queue already exists."
            )
            raise RuntimeError(f"Queue subscription for {queue_name} already exists.")

        queue = await self._declare_queue(
            queue_name, queue_type, bind_to_routing_key, exchange_name, queue_ttl
        )

        queue_consumer = QueueSubscriptionConsumer(
            queue, delete_after_messages, callback_on_message
        )
        self._queue_subscription_consumer_by_name[queue_name] = queue_consumer

        queue_subscription_task = asyncio.create_task(queue_consumer.run())
        queue_subscription_task.add_done_callback(
            partial(self._remove_queue_subscription_task, queue_name)
        )
        self._queue_subscription_tasks[queue_name] = queue_subscription_task

    async def _queue_exists(self, queue_name: str) -> bool:
        """Check if the queue exists.

        :param queue_name: Name of the queue to be checked.
        """
        try:
            await self._channel.get_queue(queue_name, ensure=True)
            logger.info("The %s queue exists", queue_name)
            return True
        except ChannelClosed as err:
            logger.warning(err)
            return False

    async def _remove_queue_subscription(self, queue_name: str) -> None:
        """Remove subscription from queue and delete the queue if one exists.

        :param queue_name: Name of the queue to unsubscribe from.
        """
        if queue_name in self._queue_subscription_tasks:
            logger.info("Stopping subscription to %s and remove queue", queue_name)
            self._queue_subscription_tasks[queue_name].cancel()
            await self._channel.queue_delete(queue_name)

    def _remove_queue_subscription_task(self, queue_name: str, future: Future) -> None:
        """Remove the queue subscription from the internal cache.

        :param queue_name: Name of the queue to which is subscribed.
        :param future: Required argument from Task.add_done_callback which also refers to the
            task running the subscription but as a `Future`.
        """
        if queue_name in self._queue_subscription_tasks:
            logger.debug("Queue subscription %s is done. Calling termination callback", queue_name)
            del self._queue_subscription_consumer_by_name[queue_name]
            del self._queue_subscription_tasks[queue_name]

    async def _setup_broker_interface(self) -> None:
        """Start the AMQP connection and channel."""
        logger.info(
            "Broker interface connecting to %s:%s as %s at %s",
            self.config.host,
            self.config.port,
            self.config.username,
            self.config.virtual_host,
        )

        self._connection = await connect_robust(
            host=self.config.host,
            port=self.config.port,
            login=self.config.username,
            password=self.config.password,
            virtualhost=self.config.virtual_host,
            loop=self._loop,
            fail_fast="false",  # aiormq requires this to be str and not bool
        )
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=1)
        self._ready_for_processing.set()

    async def _stop_broker_interface(self) -> None:
        """Cancel all subscriptions, close the channel and the connection."""
        logger.info("Stopping broker interface")
        tasks_to_cancel = list(self._queue_subscription_tasks.values())
        for queue_task in tasks_to_cancel:
            queue_task.cancel()
        if hasattr(self, "_channel") and self._channel:
            await self._channel.close()
        if hasattr(self, "_connection") and self._connection:
            await self._connection.close()
        logger.info("Stopped broker interface")

    def start(self) -> None:
        """Start the broker interface."""
        super().start()
        self._ready_for_processing.wait()

    def run(self) -> None:
        """Run the broker interface and start the AMQP connection.

        In a separate thread and starting a new, isolated eventloop. The AMQP connection and
        channel are started as its first task.
        """
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)

        setup_task = None
        try:
            setup_task = self._loop.create_task(self._setup_broker_interface())
            self._loop.run_forever()
        finally:
            # Setup task is destroyed if no reference to the task is kept. This is just to check
            # if the task was successful but also to keep the reference.
            if setup_task and not setup_task.done():
                logger.error("Setup task was not completed even though it was created.")
            elif setup_task is None:
                logger.error("Setup task for AMQP connection was not created.")
            self._loop.close()

    def declare_exchange(self, exchange_name: str) -> None:
        """Declare an exchange on which messages may be published and routed to queues.

        :param exchange_name: Name of the exchange.
        """
        asyncio.run_coroutine_threadsafe(self._declare_exchange(exchange_name), self._loop).result()

    def declare_queue(
        self,
        queue_name: str,
        queue_type: AMQPQueueType,
        bind_to_routing_key: Optional[str] = None,
        exchange_name: Optional[str] = None,
        queue_ttl: Optional[QueueTTLArguments] = None
    ) -> None:
        """Declare an AMQP queue.

        :param queue_name: Name of the queue to declare.
        :param queue_type: Declare the queue using one of the known queue types.
        :param bind_to_routing_key: Bind the queue to this routing key next to the default routing
            key of the queue name. If none, the queue is only bound to the name of the queue.
            If not none, then the exchange_name must be set as well.
        :param exchange_name: Name of the exchange on which the messages will be published.
        :param queue_ttl: Additional queue TTL arguments.
        """
        asyncio.run_coroutine_threadsafe(
            self._declare_queue(
                queue_name=queue_name,
                queue_type=queue_type,
                bind_to_routing_key=bind_to_routing_key,
                exchange_name=exchange_name,
                queue_ttl=queue_ttl,
            ),
            self._loop,
        ).result()

    def declare_queue_and_add_subscription(
        self,
        queue_name: str,
        callback_on_message: Callable[[bytes], None],
        queue_type: AMQPQueueType,
        bind_to_routing_key: Optional[str] = None,
        exchange_name: Optional[str] = None,
        delete_after_messages: Optional[int] = None,
        queue_ttl: Optional[QueueTTLArguments] = None
    ) -> None:
        """Declare an AMQP queue and subscribe to the messages.

        :param queue_name: Name of the queue to declare.
        :param callback_on_message: Callback which is called from a separate thread to process the
            message body.
        :param queue_type: Declare the queue using one of the known queue types.
        :param bind_to_routing_key: Bind the queue to this routing key next to the default routing
            key of the queue name. If none, the queue is only bound to the name of the queue.
        :param exchange_name: Name of the exchange on which the messages will be published.
        :param delete_after_messages: Delete the subscription & queue after this limit of messages
            have been successfully processed.
        :param queue_ttl: Additional queue TTL arguments.
        """
        asyncio.run_coroutine_threadsafe(
            self._declare_queue_and_add_subscription(
                queue_name=queue_name,
                callback_on_message=callback_on_message,
                queue_type=queue_type,
                bind_to_routing_key=bind_to_routing_key,
                exchange_name=exchange_name,
                delete_after_messages=delete_after_messages,
                queue_ttl=queue_ttl,
            ),
            self._loop,
        ).result()

    def queue_exists(self, queue_name: str) -> bool:
        """Check if the queue exists.

        :param queue_name: Name of the queue to be checked.
        """
        return asyncio.run_coroutine_threadsafe(
            self._queue_exists(queue_name=queue_name), self._loop
        ).result()

    def remove_queue_subscription(self, queue_name: str) -> None:
        """Remove subscription from queue and delete the queue if one exists.

        :param queue_name: Name of the queue to unsubscribe from.
        """
        asyncio.run_coroutine_threadsafe(
            self._remove_queue_subscription(queue_name), self._loop
        ).result()

    def send_message_to(
        self, exchange_name: Optional[str], routing_key: str, message: bytes
    ) -> None:
        """Publish a single message to the queue.

        :param exchange_name: The name of the exchange on which to publish the message. Must be
            declared before publishing is possible. If None, the default exchange is used which is
            already declared at startup
        :param routing_key: The routing key to publish the message to. This may be a specific
            routing key to which multiple queues are bound or the queue name (default routing key).
        :param message: The message to publish.
        """
        asyncio.run_coroutine_threadsafe(
            self._send_message_to(exchange_name, routing_key, message), self._loop
        ).result()

    def stop(self) -> None:
        """Stop the broker interface.

        By shutting down the AMQP connection and stopping the eventloop.
        """
        will_stop = False
        with self._stopping_lock:
            if not self._stopping:
                self._stopping = True
                will_stop = True

        if will_stop:
            future = asyncio.run_coroutine_threadsafe(self._stop_broker_interface(), self._loop)
            try:
                future.result(timeout=BrokerInterface.TIMEOUT_ON_STOP_SECONDS)
            except Exception:
                logger.exception("Could not stop the broker interface during shutdown.")
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._stopped = True
