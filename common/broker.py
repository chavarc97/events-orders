from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
import os
import time
import uuid

import pika
from pika.exceptions import AMQPConnectionError

from common.logger import logger


class EventType(str, Enum):
    ORDER_PLACED    = "order.placed"
    PAYMENT_APPLIED = "payment.applied"
    PAYMENT_DENIED  = "payment.denied"
    ORDER_FULFILLED = "order.fulfilled"
    EMAIL_SENT      = "email.sent"


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
    def _build_event(self, event_type: str, payload: dict) -> dict:
        """Event envelope required by the spec."""
        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "producer": self.service_name,
            "payload": payload,
        }

    def publish(self, event_type: EventType | str, payload: dict) -> dict:
        """Wrap the payload in an event envelope and publish it, using event_type as routing key."""
        if self.channel is None:
            raise RuntimeError("Not connected to RabbitMQ, call connect() first")

        # Accepts EventType.X or "order.placed"; always ends up as the plain string value
        event_type = EventType(event_type).value
        event = self._build_event(event_type, payload)

        self.channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=event_type,
            body=json.dumps(event),
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=2,  # persistent
            ),
        )
        logger.info(f"{self.service_name} - produced - {event_type}")
        return event

    # ------------------------------------------------------------------ #
    # Consuming
    # ------------------------------------------------------------------ #
    def consume(self, queue_name: str, routing_keys: list, handler) -> None:
        """
        Declare this service's queue, bind it to every routing key it cares about,
        and call `handler(event)` for each message. Blocks forever.
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
            event = json.loads(body)
            logger.info(f"{self.service_name} - consumed - {event['event_type']}")
            try:
                handler(event)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                logger.error(f"[{self.service_name}] Error handling {event['event_type']}: {e!r}")
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