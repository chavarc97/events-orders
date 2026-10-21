import uuid
from dataclasses import dataclass, asdict

from services.order import Order, OrderService


SCENARIOS = [
    ("cust-1", "ana@mail.com", ["b1"]),  # -> Payment Ok -> Fulfill
    ("cust-2", "bob@mail.com", ["b2", "b2", "b3"]),  # -> Payment Denied -> Canceled
    (
        "cust-3",
        "charlie@mail.com",
        ["b1", "b2", "b3", "b4", "b5"],
    ),  # -> Payment Ok -> Fulfill
    ("cust-4", "tom@mail.com", ["b1"]),  # stock problem -> Payment Denied
]


@dataclass
class Book:
    book_id: str
    price: float
    stock: int


@dataclass
class Customer:
    customer_id: str
    email: str
    books: list[Book]
    wallet: float


@dataclass
class Catalog:
    books: list[Book]


catalog = Catalog(
    books=[
        Book(book_id="b1", price=100.0, stock=1),
        Book(book_id="b2", price=200.0, stock=6),
        Book(book_id="b3", price=300.0, stock=3),
        Book(book_id="b4", price=400.0, stock=4),
        Book(book_id="b5", price=500.0, stock=5),
    ]
)


def build_order(customer: Customer, book_ids: list[str]) -> Order:
    total = 0
    items: list[Book] = []

    # look up each id in the Catalog
    for book_id in book_ids:
        for catalog_book in catalog.books:
            if catalog_book.book_id == book_id:
                items.append(asdict(catalog_book))
                total += catalog_book.price

    return Order(
        order_id=str(uuid.uuid4()),
        customer_id=customer.customer_id,
        customer_email=customer.email,
        items=items,
        total=total,
    )


def main():
    """
    Reqs:
        * Simulate the whole flow with multiple orders.
        * We can use enters to move into the next steps

        Steps:
            1. Order a Book
            2. Order Event is send
            3. Payment Event is processed
            4. Check if payment is Ok -> if not then
                * Send Payment Denied Event
                * Order is canceled

            5. if payment is Ok
                * Send Payment Applied Event
                * Send Order Fulfilled Event

    """

    service = OrderService()
    service.connect()

    try:
        for customer_id, customer_email, book_ids in SCENARIOS:
            customer = Customer(customer_id, customer_email, [], 0)
            order = build_order(customer, book_ids)
            service.place_order(order)
            input("Press Enter to continue...")

    finally:
        service.close()


if __name__ == "__main__":
    main()
