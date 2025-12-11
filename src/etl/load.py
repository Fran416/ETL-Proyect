"""
LOAD Module: Persistence of transformed data into databases.

This module implements the L (Load) phase of the ETL process, performing:
- Persistence of products in MongoDB (catalog)
- Persistence of cart events in Redis (real-time)
- Saving processed datasets to CSV (audit)
- Data validation before loading
- Connection management and error handling

Data destinations:
- MongoDB: Product catalog with inventory
- Redis: Cart events grouped by session
- CSV: Backup of processed data

Author: ETL Team
Date: 2025
"""

import json
import random
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.config import PROCESSED_CSV, get_mongo_connection, get_redis_connection
from src.etl.transform import transform_all
from src.utils import safe_float_conversion, safe_int_conversion, save_dataframe_to_csv

# ========================================================================
# BUSINESS CONSTANTS
# ========================================================================

# Initial stock for new products
DEFAULT_STOCK = 100

# Initial sales for new products
DEFAULT_SALES = 0


# ========================================================================
# MAIN LOAD FUNCTIONS
# ========================================================================


def load_products_to_mongodb(df: pd.DataFrame, recreate: bool = True) -> bool:
    """
    Loads transformed products to MongoDB.

    Args:
        df: DataFrame with transformed products
        recreate: If True, cleans collection before loading

    Returns:
        True if load was successful, False otherwise
    """
    if df is None or df.empty:
        print("[LOAD] Error: No data to load to MongoDB")
        return False

    client = None
    try:
        # Establish connection with MongoDB
        client, _, collection = get_mongo_connection()
        if collection is None:
            return False

        # Clean existing collection if requested
        if recreate:
            collection.delete_many({})
            # print("[LOAD] MongoDB collection cleaned")

        # Convert DataFrame to MongoDB documents using efficient iteration
        # Note: to_dict('records') is more efficient than iterrows()
        products = []
        for record in df.to_dict('records'):
            doc = {
                "product_id": record.get("product_id"),
                "product_name": record.get("product_name"),
                "category": record.get("category"),
                "actual_price": safe_float_conversion(record.get("actual_price")),
                "discounted_price": safe_float_conversion(record.get("discounted_price")),
                "discount_percentage": safe_float_conversion(record.get("discount_percentage")),
                "rating": safe_float_conversion(record.get("rating")),
                "rating_count": safe_int_conversion(record.get("rating_count")),
                "about_product": record.get("about_product", ""),
                # Business fields for inventory
                "stock": (
                            random.randint(150, 300) if safe_float_conversion(record.get("discounted_price")) < 500
                            else random.randint(80, 150) if safe_float_conversion(record.get("discounted_price")) < 2000
                            else random.randint(30, 100)
                        ),
                "total_sales": DEFAULT_SALES,
                "created_at": datetime.now(timezone.utc),
            }
            products.append(doc)

        # Bulk insert (more efficient than individual inserts)
        # ordered=False allows continuing if some document fails
        insert_result = collection.insert_many(products, ordered=False)
        # print(f"[LOAD] {len(insert_result.inserted_ids)} products loaded to MongoDB")

        # Save copy to CSV for audit
        save_dataframe_to_csv(df, Path(PROCESSED_CSV).parent, "amazon_processed.csv")

        return True

    except Exception as e:
        print(f"[LOAD] Error loading to MongoDB: {e}")
        return False

    finally:
        # Ensure connection close even if error
        if client is not None:
            client.close()


def load_carts_to_redis(df: pd.DataFrame) -> bool:
    """
    Loads cart events grouped by session to Redis.

    Args:
        df: DataFrame with transformed cart events

    Returns:
        True if load was successful, False otherwise
    """
    if df is None or df.empty:
        print("[LOAD] Error: No data to load to Redis")
        return False

    redis_client = None
    try:
        # Establish connection with Redis
        redis_client = get_redis_connection()
        if redis_client is None:
            return False

        # Clean existing Redis database
        redis_client.flushdb()
        # print("[LOAD] Redis cleaned")

        # Group events by cart using efficient iteration
        # Structure: {cart_id: {customer_id, events[], total_revenue, lost_revenue}}
        carts = {}
        for record in df.to_dict('records'):
            cart_id = record["cart_id"]

            # Initialize cart if first time
            if cart_id not in carts:
                carts[cart_id] = {
                    "customer_id": record["customer_id"],
                    "events": [],
                    "total_revenue": 0,
                    "lost_revenue": 0,
                }

            # Create event with record data
            event = {
                "event_time": str(record["event_time"]),
                "event_type": record["event_type"],
                "product_id": record["product_id"],
                "quantity": safe_int_conversion(record["quantity"]),
                "stock_before": safe_int_conversion(record["stock_before"]),
                "stock_after": safe_int_conversion(record["stock_after"]),
                "revenue": safe_float_conversion(record["revenue"]),
                "lost_revenue": safe_float_conversion(record["lost_revenue"]),
            }

            # Add event and accumulate metrics
            carts[cart_id]["events"].append(event)
            carts[cart_id]["total_revenue"] += safe_float_conversion(record["revenue"])
            carts[cart_id]["lost_revenue"] += safe_float_conversion(record["lost_revenue"])

        # Persist carts in Redis as hash keys
        # Format: cart:{cart_id} -> {customer_id, events, total_revenue, lost_revenue}
        for cart_id, cart_data in carts.items():
            redis_client.hset(
                f"cart:{cart_id}",
                mapping={
                    "customer_id": cart_data["customer_id"],
                    "events": json.dumps(cart_data["events"]),
                    "total_revenue": cart_data["total_revenue"],
                    "lost_revenue": cart_data["lost_revenue"],
                    "loaded_at": datetime.now(timezone.utc).isoformat(),
                },
            )

        # print(f"[LOAD] {len(carts)} carts loaded to Redis")

        # Save copy to CSV for audit
        save_dataframe_to_csv(df, Path(PROCESSED_CSV).parent, "carts_processed.csv")

        return True

    except Exception as e:
        print(f"[LOAD] Error loading to Redis: {e}")
        return False

    finally:
        # Ensure connection close even if error
        if redis_client is not None:
            redis_client.close()


# ========================================================================
# MAIN ORCHESTRATION FUNCTION
# ========================================================================


def load_all(amazon_df: pd.DataFrame, cart_df: pd.DataFrame) -> bool:
    """
    Executes the full LOAD stage of the ETL pipeline.

    Loads transformed data to:
    - MongoDB: Product catalog
    - Redis: Cart events
    - CSV: Audit copies

    Args:
        amazon_df: DataFrame with transformed products
        cart_df: DataFrame with transformed cart events

    Returns:
        True if both loads were successful, False otherwise
    """
    # print("\n[LOAD] Starting data load...\n")

    # Validate input data
    if amazon_df is None or amazon_df.empty:
        print("[LOAD] Error: Amazon products dataframe is required.")
        return False

    if cart_df is None or cart_df.empty:
        print("[LOAD] Error: Cart events dataframe is required.")
        return False

    # Execute loads logically parallel (independent of each other)
    mongo_ok = load_products_to_mongodb(amazon_df)
    redis_ok = load_carts_to_redis(cart_df)

    # Validate that both loads were successful
    if not (mongo_ok and redis_ok):
        print("\n[LOAD] Warning: Some loads failed")
        if not mongo_ok:
            print("  - MongoDB: FAILED")
        if not redis_ok:
            print("  - Redis: FAILED")
    
    return mongo_ok and redis_ok


def main():
    """Executes the LOAD module independently."""
    print("[LOAD] Getting transformed data...")
    transform_result = transform_all()
    if transform_result is None or transform_result[0] is None:
        print("[LOAD] Error: Could not get transformed data.")
        sys.exit(1)

    products_df, carts_df = transform_result
    if load_all(products_df, carts_df):
        print("[LOAD] Success")
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()