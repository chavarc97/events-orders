from common.broker import Broker
from common.events import Event, EventType

SERVICE_NAME = "payment-service"

mock_wallet = {
    "cust-1": {"balance": 1000.0, "active": True},
    "cust-2": {"balance": 500.0, "active": False},
    "cust-3": {"balance": 5000.0, "active": True},
    "cust-4": {"balance": 1000.0, "active": True},
}


class PaymentService:
    def __init__(self):
        self.broker = Broker(service_name=SERVICE_NAME)

    def handler(self, event: Event) -> Event:
        amount = event.payload.get("total")
        customer_id = event.payload.get("customer_id")
        reason = None

        print(f"\n💳 [PAYMENT SERVICE] Processing payment for Order '{event.order_id}'")
        print(f"   Customer: {customer_id}")
        print(f"   Order Total: ${amount if amount is not None else 0.0:.2f}")

        if amount is None or amount <= 0:
            payment_ok = False
            reason = "invalid_or_missing_amount"
        elif customer_id not in mock_wallet:
            payment_ok = False
            reason = f"unknown_customer: {customer_id}"
        elif not mock_wallet[customer_id]["active"]:
            payment_ok = False
            reason = f"account_inactive (Customer '{customer_id}' account is disabled)"
        elif mock_wallet[customer_id]["balance"] < amount:
            payment_ok = False
            reason = f"insufficient_funds (Available: ${mock_wallet[customer_id]['balance']:.2f}, Required: ${amount:.2f})"
        else:
            payment_ok = True
            # Deduct balance to simulate real transaction
            mock_wallet[customer_id]["balance"] -= amount

        if payment_ok:
            print(f"   ✅ PAYMENT APPROVED! New wallet balance: ${mock_wallet[customer_id]['balance']:.2f}")
            next_event = event.derive(EventType.PAYMENT_APPLIED, SERVICE_NAME)
        else:
            print(f"   ❌ PAYMENT DENIED! Reason: {reason}")
            next_event = event.derive(
                EventType.PAYMENT_DENIED,
                SERVICE_NAME,
                reason=reason,
            )

        return self.broker.publish(next_event)

    def listen_events(self) -> None:
        self.broker.consume(
            queue_name="payment-queue",
            routing_keys=[EventType.ORDER_PLACED],
            handler=self.handler,
        )


if __name__ == "__main__":
    service = PaymentService()
    service.listen_events()
