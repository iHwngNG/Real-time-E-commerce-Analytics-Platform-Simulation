import os
import uuid
import random
import time
import yaml
from datetime import datetime, timezone

# =============================================================================
# Event Generator Module — F1.3
# Purpose: Generate realistic behavioral events for streaming to Kafka.
# Output : JSON-serializable dicts matching the raw-events schema.
# =============================================================================

# Default config file path (relative to simulator/)
CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config.yaml")


def load_config(config_path: str = CONFIG_PATH) -> dict:
    """
    Load simulator configuration from YAML file.
    Environment variables override YAML values (F1.3.4).
    """
    config = {
        "num_active_users": 100,
        "target_events_per_sec": 500,
        "run_mode": "continuous",
        "peak_hour_enabled": True,
        "peak_hour_multiplier": 3,
        "peak_hours": [{"start": 12, "end": 13}, {"start": 20, "end": 22}],
    }

    # Load from YAML if exists
    if os.path.exists(config_path):
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)
            if yaml_data and "simulator" in yaml_data:
                config.update(yaml_data["simulator"])

    # Env vars override YAML (highest priority)
    config["num_active_users"] = int(
        os.environ.get("NUM_ACTIVE_USERS", config["num_active_users"])
    )
    config["target_events_per_sec"] = int(
        os.environ.get("TARGET_EVENTS_PER_SEC", config["target_events_per_sec"])
    )
    config["run_mode"] = os.environ.get("RUN_MODE", config["run_mode"])
    config["peak_hour_enabled"] = os.environ.get(
        "PEAK_HOUR_ENABLED", str(config["peak_hour_enabled"])
    ).lower() in ("true", "1", "yes")

    return config


class EventGenerator:
    """
    Simulates user behavioral events on an e-commerce platform.
    Events follow a weighted funnel: page_view -> product_view -> click ->
    add_to_cart -> purchase, with user segment influencing conversion rates.
    Supports configurable throughput and peak hour simulation.
    """

    # F1.3.1 — Event types and default weights
    EVENT_TYPES = [
        "page_view",
        "product_view",
        "click",
        "add_to_cart",
        "remove_from_cart",
        "wishlist_add",
        "purchase",
        "review_submit",
    ]
    EVENT_WEIGHTS = [0.30, 0.25, 0.20, 0.12, 0.03, 0.05, 0.04, 0.01]

    # F1.3.3 — Segment multipliers for purchase probability
    SEGMENT_MULTIPLIER = {
        "new_user": 1.0,
        "casual": 1.5,
        "regular": 2.0,
        "power_user": 3.0,
        "vip": 3.0,
    }

    REFERRER_CHOICES = [
        "homepage",
        "search",
        "social_media",
        "email_campaign",
        "direct",
        "ads",
        "referral",
    ]
    PAYMENT_METHODS = ["credit_card", "e-wallet", "bank_transfer", "cod"]
    APP_VERSIONS = ["3.0.0", "3.1.0", "3.2.1", "3.3.0"]

    def __init__(self, users: list, products: list, config: dict = None):
        """
        Initialize with pre-loaded user/product data and configuration.
        Args:
            users: List of user dicts (from PostgreSQL users table).
            products: List of product dicts (from PostgreSQL products table).
            config: Simulator config dict (from load_config()).
        """
        # Load config (F1.3.4)
        self.config = config or load_config()

        # Filter only active users for event generation (F1.1.4)
        self.active_users = [u for u in users if u.get("is_active", True)]
        if not self.active_users:
            raise ValueError("No active users available for simulation.")

        # Limit to NUM_ACTIVE_USERS (F1.3.4)
        num_active = self.config["num_active_users"]
        if len(self.active_users) > num_active:
            self.active_users = random.sample(self.active_users, num_active)

        self.products = products

        # Pre-compute product weights based on popularity_score (F1.3.3)
        self.product_weights = [
            max(p.get("popularity_score", 1), 1) for p in self.products
        ]

        # Session tracking: {user_id: {"session_id": ..., "last_active": ...}}
        self._sessions = {}
        self._session_timeout = 1800  # 30 minutes in seconds (F1.3.3)

    def is_peak_hour(self) -> bool:
        """
        Check if current hour falls within peak hour ranges (F1.3.3).
        Peak hours have event rate multiplied by peak_hour_multiplier.
        """
        if not self.config["peak_hour_enabled"]:
            return False

        current_hour = datetime.now(timezone.utc).hour
        for window in self.config.get("peak_hours", []):
            if window["start"] <= current_hour < window["end"]:
                return True
        return False

    def get_current_rate(self) -> float:
        """
        Return the target events/sec adjusted for peak hours.
        During peak hours, rate = base_rate * peak_hour_multiplier.
        """
        base_rate = self.config["target_events_per_sec"]
        if self.is_peak_hour():
            return base_rate * self.config.get("peak_hour_multiplier", 3)
        return base_rate

    def get_run_duration(self) -> int:
        """
        Parse run_mode to determine how long to run (in seconds).
        Returns:
            int: 0 for continuous, N for timed:N
        """
        mode = self.config["run_mode"]
        if mode == "continuous":
            return 0
        if mode.startswith("timed:"):
            return int(mode.split(":")[1])
        return 0

    def _get_or_create_session(self, user_id: str) -> str:
        """
        Return existing session_id or create a new one if idle > 30 min.
        Implements session lifecycle per F1.3.3.
        """
        now = time.time()

        if user_id in self._sessions:
            session = self._sessions[user_id]
            if now - session["last_active"] < self._session_timeout:
                session["last_active"] = now
                return session["session_id"]

        # Create new session
        new_session_id = str(uuid.uuid4())
        self._sessions[user_id] = {"session_id": new_session_id, "last_active": now}
        return new_session_id

    def _pick_event_type(self, user_segment: str) -> str:
        """
        Pick event type using weighted random, adjusted by user segment.
        VIP/power_user segments have higher purchase probability (F1.3.3).
        """
        multiplier = self.SEGMENT_MULTIPLIER.get(user_segment, 1.0)

        # Adjust weights: boost purchase and add_to_cart for high-value users
        adjusted_weights = list(self.EVENT_WEIGHTS)
        purchase_idx = self.EVENT_TYPES.index("purchase")
        cart_idx = self.EVENT_TYPES.index("add_to_cart")

        adjusted_weights[purchase_idx] *= multiplier
        adjusted_weights[cart_idx] *= multiplier * 0.8

        return random.choices(self.EVENT_TYPES, weights=adjusted_weights, k=1)[0]

    def _pick_product(self) -> dict:
        """Pick a product weighted by popularity_score (F1.3.3)."""
        return random.choices(self.products, weights=self.product_weights, k=1)[0]

    def _build_event_data(self, event_type: str, product: dict) -> dict:
        """
        Build context-specific event_data fields based on event type.
        Only relevant fields are populated per event type.
        """
        data = {
            "quantity": 1,
            "referrer": random.choice(self.REFERRER_CHOICES),
        }

        # Search keyword only for search-driven events
        if random.random() < 0.3:
            data["search_keyword"] = random.choice(
                [
                    product.get("brand", ""),
                    product.get("category_name", ""),
                    product.get("product_name", "").split()[0],
                ]
            ).lower()

        if event_type in ["product_view", "click"]:
            data["position_in_list"] = random.randint(1, 50)
            data["time_on_page_sec"] = random.randint(5, 300)

        if event_type == "purchase":
            data["quantity"] = random.randint(1, 5)
            data["payment_method"] = random.choice(self.PAYMENT_METHODS)
            data["order_id"] = (
                f"ORD-{datetime.now(timezone.utc).strftime('%Y%m%d')}"
                f"-{random.randint(10000, 99999)}"
            )

        if event_type == "add_to_cart":
            data["quantity"] = random.randint(1, 3)

        return data

    def _build_metadata(self, user: dict) -> dict:
        """Generate request metadata (IP, user agent, screen size)."""
        device = user.get("preferred_device", "mobile")

        if device == "mobile":
            ua = "Mozilla/5.0 (Linux; Android 14) Mobile Safari/537.36"
            screen = random.choice(["390x844", "412x915"])
        elif device == "tablet":
            ua = "Mozilla/5.0 (iPad; CPU OS 17_0) Safari/605.1.15"
            screen = "768x1024"
        else:
            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/122.0"
            screen = random.choice(["1920x1080", "1366x768", "2560x1440"])

        return {
            "ip_address": f"{random.randint(1, 255)}.{random.randint(0, 255)}"
            f".{random.randint(0, 255)}.{random.randint(1, 254)}",
            "user_agent": ua,
            "app_version": random.choice(self.APP_VERSIONS),
            "screen_size": screen,
        }

    def generate_event(self) -> dict:
        """
        Generate a single complete event following the F1.3.2 JSON schema.
        Returns:
            dict: A fully populated event ready for Kafka serialization.
        """
        user = random.choice(self.active_users)
        product = self._pick_product()
        event_type = self._pick_event_type(user.get("user_segment", "casual"))
        session_id = self._get_or_create_session(user["user_id"])

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_version": "1.0",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "session_id": session_id,
            "user": {
                "user_id": user["user_id"],
                "segment": user.get("user_segment", "casual"),
                "country": user.get("country", "VN"),
                "city": user.get("city", ""),
                "device": user.get("preferred_device", "mobile"),
                "os": user.get("os", "Android"),
                "language": user.get("preferred_language", "vi"),
            },
            "product": {
                "product_id": product["product_id"],
                "sku": product.get("sku", ""),
                "name": product.get("product_name", ""),
                "category": product.get("category_name", ""),
                "subcategory": product.get("subcategory", ""),
                "brand": product.get("brand", ""),
                "sale_price": product.get("sale_price", 0),
                "currency": product.get("currency", "VND"),
            },
            "event_data": self._build_event_data(event_type, product),
            "metadata": self._build_metadata(user),
        }

    def generate_batch(self, count: int = 100) -> list:
        """
        Generate a batch of events efficiently.
        Args:
            count: Number of events to generate.
        Returns:
            list: List of event dictionaries.
        """
        return [self.generate_event() for _ in range(count)]


# For local testing module directly
if __name__ == "__main__":
    import json
    from user_generator import UserGenerator
    from product_generator import ProductGenerator

    # Load config
    cfg = load_config()
    print("=== Loaded Config ===")
    print(json.dumps(cfg, indent=2, default=str))
    print()

    # Generate sample seed data
    user_gen = UserGenerator()
    prod_gen = ProductGenerator()
    sample_users = user_gen.generate_batch(10)
    sample_products = prod_gen.generate_batch(5)

    # Generate events using sample data + config
    event_gen = EventGenerator(users=sample_users, products=sample_products, config=cfg)
    print(f"=== Peak Hour: {event_gen.is_peak_hour()} ===")
    print(f"=== Current Rate: {event_gen.get_current_rate()} events/sec ===")
    print(f"=== Run Duration: {event_gen.get_run_duration()} (0=continuous) ===")
    print()

    sample_event = event_gen.generate_event()
    print("=== Sample Event ===")
    print(json.dumps(sample_event, indent=2))
    print()

    # --- SIMULATION LOOP DEMO ---
    run_duration = event_gen.get_run_duration()
    target_rate = event_gen.get_current_rate()
    sleep_time = 1.0 / target_rate if target_rate > 0 else 0.1

    print(
        f"🚀 Bắt đầu mô phỏng (Mode: {cfg['run_mode']}) ở tốc độ {target_rate} events/s..."
    )
    print("Bấm Ctrl+C để dừng lại!\n")

    start_time = time.time()
    events_generated = 0

    try:
        while True:
            # Kiểm tra thời gian nếu mode timed:N
            if run_duration > 0 and (time.time() - start_time) >= run_duration:
                print(f"⏱️ Đã hết thời gian mô phỏng ({run_duration}s). Tự động dừng.")
                break

            # Sinh event và in ra 1 dòng ngắn gọn để terminal không bị tràn
            event = event_gen.generate_event()
            print(
                f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] "
                f"User: {event['user']['user_id'][:8]}... | "
                f"Event: {event['event_type']:15} | "
                f"Product: {event['product']['sku']}"
            )

            events_generated += 1
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n🛑 Đã dừng thủ công bằng Ctrl+C.")

    finally:
        elapsed = time.time() - start_time
        print("\n📊 --- THỐNG KÊ ---")
        print(f"Tổng events sinh ra : {events_generated}")
        print(f"Thời gian chạy thực : {elapsed:.2f} giây")
        print(f"Tốc độ trung bình   : {events_generated / elapsed:.2f} events/s")
