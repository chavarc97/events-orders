from dataclasses import dataclass
from common.broker import Broker
from common.events import Event, EventType

SERVICE_NAME = "notification-service"


@dataclass
class Email:
    to: str
    email_subject: str
    email_body: str


class NotificationService:
    def __init__(self):
        self.broker = Broker(service_name=SERVICE_NAME)

    def _build_email(self, event: Event) -> Email:
        """Builds email content dynamically based on the event received."""
        order_id = event.order_id
        items = event.payload.get("items", [])
        item_names = ", ".join([item.get("book_id", "") for item in items]) or "Selected books"
        email_subject = "Order Update"
        email_body = ""

        if event.event_type == EventType.ORDER_PLACED:
            email_subject = f"Order Received #{order_id[:8]}"
            total = event.payload.get("total", 0.0)
            email_body = (
                f"Hello! We received your order for [{item_names}]. "
                f"Total: ${total:.2f}. Status: Processing payment."
            )

        elif event.event_type == EventType.PAYMENT_DENIED:
            reason = event.payload.get("reason", "unknown error")
            email_subject = f"Payment Failed: Order #{order_id[:8]}"
            email_body = (
                f"We were unable to process your payment for order #{order_id[:8]}. "
                f"Reason: {reason}. Please update your payment method."
            )

        elif event.event_type == EventType.ORDER_FULFILLED:
            tracking = event.payload.get("tracking_number", "N/A")
            carrier = event.payload.get("carrier", "standard carrier")
            delivery = event.payload.get("estimated_delivery", "soon")
            email_subject = f"Your Order #{order_id[:8]} Has Shipped!"
            email_body = (
                f"Your items [{item_names}] are on their way via {carrier}!\n"
                f"Tracking code: {tracking}. Estimated arrival: {delivery}."
            )

        return Email(
            to=event.payload.get("customer_email"),
            email_subject=email_subject,
            email_body=email_body,
        )

    def handler(self, event: Event) -> Event:
        """Processes incoming events, simulates dispatching an email, and emits email.sent."""
        email = self._build_email(event)
        if email.to is not None:
            self._send_email(email)

        next_event = event.derive(
            EventType.EMAIL_SENT,
            SERVICE_NAME,
            email_to=email.to,
            email_subject=email.email_subject,
            email_body=email.email_body,
        )
        return self.broker.publish(next_event)

    def _send_email(self, email: Email) -> None:
        """Visual email dispatch representation in logs/console."""
        print("+" + "-" * 62 + "+")
        print(f"| 📬 [DISPATCHED EMAIL] To: {email.to:<34} |")
        print(f"|    Subject: {email.email_subject:<48} |")
        print(f"|    Message: {email.email_body:<48} |")
        print("+" + "-" * 62 + "+")

    def listen_events(self) -> None:
        """Listen to all order milestones that require customer alerts."""
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
    service = NotificationService()
    service.listen_events()
