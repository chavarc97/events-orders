from dataclasses import dataclass, field
import os
import time

import pika
from pika.exceptions import AMQPConnectionError

from common.events import Event, EventType
from common.logger import logger


@dataclass
class Broker:
    # Name of the service using this broker (used as `producer` and in trace logs)
    service_name: str
    # default_factory -> env vars are read when the instance is created, not at import time
    host: str = field(default_factory=lambda: os.getenv("RABBIT_HOST", "localhost"))
    port: int = field(default_factory=lambda: int(os.getenv("RABBIT_PORT", "5672")))
    exchange_name: str = "orders"

    # Internal state (not constructor arguments)
    connection: pika.BlockingConnection | None = field(default=None, init=False, repr=False)
    channel: pika.adapters.blocking_connection.BlockingChannel | None = field(default=None, init=False, repr=False)

    # ------------------------------------------------------------------ #
    # Connection
    # ------------------------------------------------------------------ #
    def connect(self, retries: int = 10, delay: int = 3) -> "Broker":
        """Open connection + channel (retrying while RabbitMQ boots) and declare the exchange."""
        params = pika.ConnectionParameters(host=self.host, port=self.port)

        for attempt in range(1, retries + 1):
            try:
                logger.info(f"[{self.service_name}] Connecting to RabbitMQ at {self.host}:{self.port} "
                            f"(attempt {attempt}/{retries})")
                self.connection = pika.BlockingConnection(params)
                break
            except AMQPConnectionError as e:
                logger.warning(f"[{self.service_name}] RabbitMQ not ready: {e!r}")
                if attempt == retries:
                    raise
                time.sleep(delay)

        self.channel = self.connection.channel()
        self._declare_exchange()
        logger.info(f"[{self.service_name}] Connected, exchange '{self.exchange_name}' ready")
        return self

    def _declare_exchange(self) -> None:
        """Idempotent: creates the topic exchange only if it doesn't exist yet."""
        self.channel.exchange_declare(
            exchange=self.exchange_name,
            exchange_type="topic",
            durable=True,
        )

    def close(self) -> None:
        if self.connection is not None and self.connection.is_open:
            self.connection.close()
            logger.info(f"[{self.service_name}] Connection closed")

    # ------------------------------------------------------------------ #
    # Producing
    # ------------------------------------------------------------------ #
    def publish(self, event: Event) -> Event:
        """Publish an Event to the exchange, using its event_type as routing key."""
        if self.channel is None:
            raise RuntimeError("Not connected to RabbitMQ, call connect() first")

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=event.event_type,
            body=event.to_json(),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # persistent
                message_id=event.event_id,
            ),
        )
        logger.info(f"{self.service_name} - produced - {event.event_type} - order={event.order_id}")
        return event

    # ------------------------------------------------------------------ #
    # Consuming
    # ------------------------------------------------------------------ #
    def consume(self, queue_name: str, routing_keys: list, handler) -> None:
        """
        Declare this service's queue, bind it to every routing key it cares about,
        and call `handler(event: Event)` for each message. Blocks forever.
        """
        if self.channel is None:
            self.connect()

        self.channel.queue_declare(queue=queue_name, durable=True)
        for key in routing_keys:
            self.channel.queue_bind(
                queue=queue_name,
                exchange=self.exchange_name,
                routing_key=EventType(key).value,
            )

        # Deliver one message at a time; next one only after we ack
        self.channel.basic_qos(prefetch_count=1)

        def on_message(ch, method, properties, body):
            try:
                event = Event.from_json(body)
            except (ValueError, KeyError) as e:
                logger.error(f"[{self.service_name}] Malformed message discarded: {e!r}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
                return

            logger.info(f"{self.service_name} - consumed - {event.event_type} - order={event.order_id}")
            try:
                handler(event)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"[{self.service_name}] Error handling {event.event_type}: {e!r}")
                # Don't requeue: a message that always fails would loop forever
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

        self.channel.basic_consume(queue=queue_name, on_message_callback=on_message)

        keys = [EventType(k).value for k in routing_keys]
        logger.info(f"[{self.service_name}] Waiting for {keys} on '{queue_name}'...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
        finally:
            self.close()