from dataclasses import dataclass

from common.broker import Broker
from common.events import Event, EventType


SERVICE_NAME = "order-service"


@dataclass
class Order:
    order_id: str
    customer_id: str
    customer_email: str
    items: list[object]
    total: float


class OrderService:
    def __init__(self):
        self.broker = Broker(service_name=SERVICE_NAME)

    def connect(self) -> None:
        self.broker.connect(retries=3, delay=2)

    def place_order(self, order: Order) -> Event:
        event = Event(
            event_type=EventType.ORDER_PLACED,
            producer=SERVICE_NAME,
            payload=order.__dict__,
        )
        self.broker.publish(event)

        return event

    def close(self) -> None:
        self.broker.close()
