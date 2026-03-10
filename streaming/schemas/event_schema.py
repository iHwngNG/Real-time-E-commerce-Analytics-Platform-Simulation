from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

# =============================================================================
# Event Schema Definition — F3.2.2
# Purpose: Define strictly typed StructType to parse incoming Kafka JSON bytes.
# =============================================================================

# Define the 'user' struct
user_schema = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("country", StringType(), True),
        StructField("device", StringType(), True),
    ]
)

# Define the 'product' struct
product_schema = StructType(
    [
        StructField("product_id", StringType(), True),
        StructField("sku", StringType(), True),
        StructField("category", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("sale_price", DoubleType(), True),
    ]
)

# Define the 'event_data' struct (varies per event, use superset of fields)
event_data_schema = StructType(
    [
        StructField("quantity", IntegerType(), True),
        StructField("position_in_list", IntegerType(), True),
        StructField("search_keyword", StringType(), True),
        StructField("time_on_page_sec", IntegerType(), True),
        StructField("payment_method", StringType(), True),
        StructField("order_id", StringType(), True),
    ]
)

# Define the 'metadata' struct
metadata_schema = StructType(
    [
        StructField("app_version", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("screen_size", StringType(), True),
    ]
)

# Complete nested schema representing the JSON payload
event_schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("event_type", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_version", StringType(), True),
        # Nested objects
        StructField("user", user_schema, True),
        StructField("product", product_schema, True),
        StructField("event_data", event_data_schema, True),
        StructField("metadata", metadata_schema, True),
    ]
)

# Valid event types for filtering/validation
VALID_EVENT_TYPES = [
    "page_view",
    "product_view",
    "search",
    "click",
    "add_to_cart",
    "remove_from_cart",
    "wishlist_add",
    "purchase",
    "review_submit",
]
