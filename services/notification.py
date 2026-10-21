from dataclasses import dataclass
from common.broker import Broker
from common.events import Event, EventType
import json


SERVICE_NAME = "notification-service"


@dataclass
class Email:
    to: str
    email_subject: str
    email_body: str


class Notification_service:
    def __init__(self):
        self.broker = Broker(service_name=SERVICE_NAME)

    def _build_email(self, event: Event) -> Email:
        """Builds email info from an event"""
        email_subject = None
        email_body = None

        if event.event_type == EventType.ORDER_PLACED:
            email_subject = "Order Placed"
            email_body = f"Order {event.order_id} has been placed."
        elif event.event_type == EventType.PAYMENT_DENIED:
            email_subject = "Payment Denied"
            email_body = f"Payment for order {event.order_id} has been denied."
        elif event.event_type == EventType.ORDER_FULFILLED:
            email_subject = "Order Fulfilled"
            email_body = f"Order {event.order_id} has been fulfilled."
        return Email(
            to=event.payload.get("customer_email"),
            email_subject=email_subject,
            email_body=email_body,
        )

    def handler(self, event: Event) -> Event:
        """handles the events for this service"""
        email = self._build_email(event)
        if email.to is not None:
            self._send_email(email)

        next_event = event.derive(EventType.EMAIL_SENT, SERVICE_NAME)
        self.broker.publish(next_event)

        return next_event

    def _send_email(self, email: Email) -> None:
        """Sends an email"""
        print(
            f"Sending email to {email.to}: {email.email_subject} - {email.email_body}"
        )

    def listen_events(self) -> None:
        """bind event types to the handler"""
        self.broker.consume(
            queue_name="notification-queue",
            routing_keys=[
                EventType.ORDER_PLACED,
                EventType.PAYMENT_DENIED,
                EventType.ORDER_FULFILLED,
            ],
            handler=self.handler,
        )


if __name__ == "__main__":
    service = Notification_service()
    service.listen_events()
