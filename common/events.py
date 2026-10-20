"""
Event definitions shared by every service.

An event is an immutable fact that already happened ("order.placed").
Every message on the broker uses the same envelope:

    {
        "event_id":   unique id (uuid4)
        "event_type": what happened, also used as the RabbitMQ routing key
        "timestamp":  when it was emitted (ISO-8601, UTC)
        "producer":   name of the service that emitted it
        "payload":    business data (the order)
    }
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
import json
import uuid


class EventType(str, Enum):
    """All event types in the flow. The value is the routing key."""
    ORDER_PLACED    = "order.placed"
    PAYMENT_APPLIED = "payment.applied"
    PAYMENT_DENIED  = "payment.denied"
    ORDER_FULFILLED = "order.fulfilled"
    EMAIL_SENT      = "email.sent"


def _new_id() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)  # frozen -> events are facts, they can't be modified after creation
class Event:
    event_type: str
    producer: str
    payload: dict
    event_id: str = field(default_factory=_new_id)
    timestamp: str = field(default_factory=_now)

    def __post_init__(self):
        # Accept EventType.X or "order.placed", store always the plain string.
        # Also validates: an unknown type raises ValueError.
        object.__setattr__(self, "event_type", EventType(self.event_type).value)

    # ---------- serialization (Python object <-> bytes on the wire) ---------- #
    def to_dict(self) -> dict:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_json(cls, raw: str | bytes) -> "Event":
        data = json.loads(raw)
        return cls(
            event_type=data["event_type"],
            producer=data["producer"],
            payload=data["payload"],
            event_id=data["event_id"],
            timestamp=data["timestamp"],
        )

    # ---------- helpers ---------- #
    @property
    def order_id(self) -> str | None:
        """Correlation id: lets you follow one order across every service's logs."""
        return self.payload.get("order_id")

    def derive(self, event_type: EventType | str, producer: str, **extra) -> "Event":
        """
        Build the NEXT event in the chain from this one.
        Copies the payload (so order_id travels along) and adds/overrides fields.
            e.g. event.derive(EventType.PAYMENT_DENIED, "payment-service", reason="insufficient funds")
        """
        return Event(event_type=event_type, producer=producer, payload={**self.payload, **extra})
