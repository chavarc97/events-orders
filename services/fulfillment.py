"""
Fulfillment service.

    consumes:  payment.applied   (only paid orders get shipped)
    produces:  order.fulfilled   (always - there is no "fulfillment failed" event in the flow)
"""
from datetime import datetime, timedelta, timezone
import uuid

from common.broker import Broker
from common.events import Event, EventType


SERVICE_NAME = "fulfillment-service"
CARRIER = "DHL Express"
DELIVERY_DAYS = 3


class FulfillmentService:
    def __init__(self):
        self.broker = Broker(service_name=SERVICE_NAME)

    def handler(self, event: Event) -> Event:
        """Called by Broker.consume() once per `payment.applied` message."""
        order_id = event.order_id
        items = event.payload.get("items", [])
        customer_email = event.payload.get("customer_email")

        # 1. Simulate warehouse packing and dispatch
        tracking_number = f"TRK-{uuid.uuid4().hex[:8].upper()}"
        estimated_delivery = (datetime.now(timezone.utc) + timedelta(days=DELIVERY_DAYS)).date().isoformat()

        print(f"\n📦 [FULFILLMENT SERVICE] Fulfilling order '{order_id}'")
        print(f"   Recipient:  {customer_email}")
        print(f"   Items ({len(items)}): {[item.get('book_id') for item in items]}")
        print(f"   Carrier:    {CARRIER} (Tracking: {tracking_number})")
        print(f"   Delivery:   {estimated_delivery}")
        print("   ✅ Package dispatched to carrier!")

        # 2. Build NEXT event
        next_event = event.derive(
            EventType.ORDER_FULFILLED,
            SERVICE_NAME,
            tracking_number=tracking_number,
            carrier=CARRIER,
            estimated_delivery=estimated_delivery,
        )

        # 3. Publish
        return self.broker.publish(next_event)

    def listen_events(self) -> None:
        """Bind 'fulfillment-queue' to payment.applied and block forever."""
        self.broker.consume(
            queue_name="fulfillment-queue",
            routing_keys=[EventType.PAYMENT_APPLIED],
            handler=self.handler,
        )


if __name__ == "__main__":
    FulfillmentService().listen_events()
