from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1] / "data"
EVENT_ROWS = 10_000_000
ORDER_ROWS = 2_500_000
CATALOG_ROWS = 250_000


def session_id_for(index: int) -> str:
    hex_id = f"{index:032x}"
    return f"{hex_id[:8]}-{hex_id[8:12]}-{hex_id[12:16]}-{hex_id[16:20]}-{hex_id[20:32]}"


def write_catalog(path: Path) -> None:
    categories = [
        "electronics",
        "fashion",
        "beauty",
        "home",
        "sports",
        "toys",
        "books",
        "grocery",
        "automotive",
        "garden",
        "health",
        "pets",
        "office",
        "gaming",
        "music",
        "travel",
        "baby",
        "luxury",
        "outdoor",
        "fitness",
    ]
    brands = [f"brand{chr(65 + i)}" for i in range(26)]

    with path.open("w", encoding="utf-8", newline="") as file_handle:
        file_handle.write("product_id\tcategory\tbrand\tprice\n")
        for product_id in range(1, CATALOG_ROWS + 1):
            category = categories[(product_id * 7) % len(categories)]
            brand = brands[(product_id * 11) % len(brands)]
            price = round(8 + ((product_id * 37) % 300_000) / 37.0, 2)
            file_handle.write(f"{product_id}\t{category}\t{brand}\t{price}\n")


def write_events(path: Path) -> None:
    devices = ["desktop", "mobile", "tablet"]
    referrers = [
        "direct",
        "google",
        "facebook",
        "instagram",
        "email",
        "affiliate",
        "newsletter",
        "tiktok",
        "youtube",
        "partner",
        "bing",
        "x",
        "reddit",
    ]
    event_types = ["view", "add_to_cart", "purchase", "search", "wishlist", "share", "direct"]
    user_pool = 1_500_000

    start_ts = datetime(2024, 1, 1, 0, 0, 0)
    timestamp_pool = [
        (start_ts + timedelta(minutes=minute)).strftime("%Y-%m-%dT%H:%M:%S")
        for minute in range(260_000)
    ]
    timestamp_pool_size = len(timestamp_pool)

    with path.open("w", encoding="utf-8", newline="") as file_handle:
        file_handle.write("user_id\ttimestamp\tevent_type\tproduct_id\tsession_id\tdevice\treferrer\n")
        for index in range(1, EVENT_ROWS + 1):
            user_id = 100_000 + (index * 17) % user_pool
            timestamp = timestamp_pool[(index * 13) % timestamp_pool_size]
            event_type = event_types[(index * 19) % len(event_types)]
            product_id = 1 + (index * 29) % CATALOG_ROWS
            session_id = session_id_for(index)
            device = devices[(index * 5) % len(devices)]
            referrer = referrers[(index * 23) % len(referrers)]
            file_handle.write(
                f"{user_id}\t{timestamp}\t{event_type}\t{product_id}\t{session_id}\t{device}\t{referrer}\n"
            )


def write_orders(path: Path) -> None:
    user_pool = 1_500_000
    start_ts = datetime(2024, 1, 1, 0, 0, 0)
    timestamp_pool = [
        (start_ts + timedelta(minutes=minute * 2)).strftime("%Y-%m-%dT%H:%M:%S")
        for minute in range(140_000)
    ]
    timestamp_pool_size = len(timestamp_pool)

    with path.open("w", encoding="utf-8", newline="") as file_handle:
        file_handle.write("order_id\tuser_id\ttimestamp\ttotal_amount\n")
        for order_id in range(1, ORDER_ROWS + 1):
            user_id = 100_000 + (order_id * 31) % user_pool
            timestamp = timestamp_pool[(order_id * 7) % timestamp_pool_size]
            base = ((order_id * 97) % 250_000) / 10.0
            total_amount = round(25.0 + base + ((order_id % 17) * 0.13), 2)
            file_handle.write(f"{order_id}\t{user_id}\t{timestamp}\t{total_amount}\n")


def main() -> None:
    BASE_DIR.mkdir(parents=True, exist_ok=True)
    write_catalog(BASE_DIR / "catalog.csv")
    write_events(BASE_DIR / "events.csv")
    write_orders(BASE_DIR / "orders.csv")


if __name__ == "__main__":
    main()