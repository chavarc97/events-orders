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
CARRIER = "DHL"
DELIVERY_DAYS = 3


class FulfillmentService:
    def __init__(self):
        self.broker = Broker(service_name=SERVICE_NAME)

    def handler(self, event: Event) -> Event:
        """Called by Broker.consume() once per `payment.applied` message."""
        # 1. Simulate the shipping work: generate the shipment details.
        #    (The spec doesn't require real shipping, only the event.)
        tracking_number = f"TRK-{uuid.uuid4().hex[:8].upper()}"
        estimated_delivery = (datetime.now(timezone.utc) + timedelta(days=DELIVERY_DAYS)).date().isoformat()

        # 2. Build the NEXT event in the chain.
        #    derive() copies the whole previous payload (order_id, items, total, ...)
        #    and adds these extra fields on top.
        next_event = event.derive(
            EventType.ORDER_FULFILLED,       # what WE produce (not what we consume!)
            SERVICE_NAME,
            tracking_number=tracking_number,
            carrier=CARRIER,
            estimated_delivery=estimated_delivery,
        )

        # 3. Announce it. publish() logs "fulfillment-service - produced - order.fulfilled".
        return self.broker.publish(next_event)

    def listen_events(self) -> None:
        """Bind 'fulfillment-queue' to payment.applied and block forever."""
        self.broker.consume(
            queue_name="fulfillment-queue",
            routing_keys=[EventType.PAYMENT_APPLIED],   # what WE consume
            handler=self.handler,
        )


if __name__ == "__main__":
    FulfillmentService().listen_events()
