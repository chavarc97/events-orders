from common.broker import Broker
from common.events import Event, EventType

SERVICE_NAME = "payment-service"

mock_wallet = {
    "cust-1": {"balance": 1000, "active": True},
    "cust-2": {"balance": 500, "active": False},
    "cust-3": {"balance": 5000, "active": True},
    "cust-4": {"balance": 1000, "active": True},
}


class payment_service:
    def __init__(self):
        self.broker = Broker(service_name=SERVICE_NAME)

    def handler(self, event: Event) -> Event:
        order_id = event.order_id
        amount = event.payload.get("total")
        customer_id = event.payload.get("customer_id")

        if customer_id not in mock_wallet or not mock_wallet[customer_id]["active"]:
            payment_ok = False
        else:
            payment_ok = mock_wallet[customer_id]["balance"] >= amount

        # next publish if payment is ok
        if payment_ok:
            next_event = event.derive(EventType.PAYMENT_APPLIED, SERVICE_NAME)
        else:
            next_event = event.derive(EventType.PAYMENT_DENIED, SERVICE_NAME)
        self.broker.publish(next_event)

    def listen_events(self) -> None:
        self.broker.consume(
            queue_name="payment-queue",
            routing_keys=[EventType.ORDER_PLACED],
            handler=self.handler,
        )


if __name__ == "__main__":
    service = payment_service()
    service.listen_events()
