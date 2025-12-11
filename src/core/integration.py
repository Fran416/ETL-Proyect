"""
INTEGRATION: Data crossing between MongoDB (catalog) and Redis (real-time carts).
Analysis and metrics to simulate a Cyberday.
"""

import json
from datetime import datetime
import pandas as pd
from src.config import get_mongo_connection, get_redis_connection


def get_product_performance_mongodb() -> dict:
    """Gets product metrics from MongoDB."""
    try:
        _, _, collection = get_mongo_connection()
        if collection is None:
            return {}

        # Aggregation: products by brand and category
        pipeline = [
            {
                "$group": {
                    "_id": "$brand",
                    "count": {"$sum": 1},
                    "avg_price": {"$avg": "$discounted_price"},
                    "avg_rating": {"$avg": "$rating"},
                    "avg_discount": {"$avg": "$discount_percentage"},
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 10},
        ]

        results = list(collection.aggregate(pipeline))

        metrics = {
            "top_brands": results,
            "total_products": collection.count_documents({}),
            "timestamp": datetime.utcnow().isoformat(),
        }

        return metrics

    except Exception as e:
        print(f"[INTEGRATION] MongoDB Error: {e}")
        return {}


def get_cart_analytics_redis() -> dict:
    """Gets cart metrics from Redis."""
    try:
        redis_client = get_redis_connection()
        if redis_client is None:
            return {}

        # Get all cart keys
        cart_keys = redis_client.keys("cart:CART-*")
        
        metrics = {
            "total_carts": 0,
            "total_revenue": 0,
            "lost_revenue": 0,
            "checkout_events": 0,
            "abandoned_carts": 0,
            "carts": [],
        }

        for key in cart_keys:
            cart_data = redis_client.hgetall(key)
            if not cart_data:
                continue

            metrics["total_carts"] += 1
            metrics["total_revenue"] += float(cart_data.get("total_revenue", 0))
            metrics["lost_revenue"] += float(cart_data.get("lost_revenue", 0))

            # Count events
            try:
                events = json.loads(cart_data.get("events", "[]"))
                for event in events:
                    if event["event_type"] == "checkout":
                        metrics["checkout_events"] += 1
                    elif event["event_type"] == "abandon":
                        metrics["abandoned_carts"] += 1
            except:
                pass

            metrics["carts"].append({
                "cart_id": key.replace("cart:", ""),
                "customer_id": cart_data.get("customer_id", "Unknown"),
                "total_revenue": float(cart_data.get("total_revenue", 0)),
            })

        metrics["timestamp"] = datetime.utcnow().isoformat()
        return metrics

    except Exception as e:
        print(f"[INTEGRATION] Redis Error: {e}")
        return {}


def enrich_carts_with_product_info():
    """Enriches cart data with product information."""
    try:
        _, _, mongo_col = get_mongo_connection()
        redis_client = get_redis_connection()

        if mongo_col is None or redis_client is None:
            return False

        # Get all products as dictionary
        products = {}
        for doc in mongo_col.find({}, {"product_id": 1, "product_name": 1, "discounted_price": 1}):
            products[doc["product_id"]] = {
                "name": doc.get("product_name", "Unknown"),
                "price": doc.get("discounted_price", 0),
            }

        # Enrich carts
        cart_keys = redis_client.keys("cart:CART-*")
        
        for key in cart_keys:
            cart_data = redis_client.hgetall(key)
            try:
                events = json.loads(cart_data.get("events", "[]"))
                
                for event in events:
                    product_id = event.get("product_id")
                    if product_id in products:
                        event["product_name"] = products[product_id]["name"]
                        event["product_price"] = products[product_id]["price"]

                # Save enriched events
                redis_client.hset(key, "events_enriched", json.dumps(events))

            except Exception as e:
                # Silent error for individual carts
                continue

        redis_client.close()
        return True

    except Exception as e:
        print(f"[INTEGRATION] Enrichment Error: {e}")
        return False


def generate_cyberday_report() -> pd.DataFrame:
    """Generates complete Cyberday report."""
    
    # Get metrics
    product_metrics = get_product_performance_mongodb()
    cart_metrics = get_cart_analytics_redis()

    # Create report
    report = {
        "Metric": [],
        "Value": [],
    }

    # Products
    report["Metric"].append("Total Products")
    report["Value"].append(product_metrics.get("total_products", 0))

    # Carts
    report["Metric"].append("Total Carts")
    report["Value"].append(cart_metrics.get("total_carts", 0))

    report["Metric"].append("Completed Carts")
    report["Value"].append(cart_metrics.get("checkout_events", 0))

    report["Metric"].append("Abandoned Carts")
    report["Value"].append(cart_metrics.get("abandoned_carts", 0))

    # Revenue
    report["Metric"].append("Total Revenue")
    report["Value"].append(f"${cart_metrics.get('total_revenue', 0):.2f}")

    report["Metric"].append("Lost Revenue")
    report["Value"].append(f"${cart_metrics.get('lost_revenue', 0):.2f}")

    # Rates
    total_carts = cart_metrics.get("total_carts", 1)
    checkout_rate = (cart_metrics.get("checkout_events", 0) / total_carts * 100) if total_carts > 0 else 0
    abandon_rate = (cart_metrics.get("abandoned_carts", 0) / total_carts * 100) if total_carts > 0 else 0

    report["Metric"].append("Conversion Rate (%)")
    report["Value"].append(f"{checkout_rate:.2f}%")

    report["Metric"].append("Abandonment Rate (%)")
    report["Value"].append(f"{abandon_rate:.2f}%")

    df_report = pd.DataFrame(report)
    return df_report


def integration_all():
    """Executes the full INTEGRATION stage."""
    enrich_carts_with_product_info()
    report = generate_cyberday_report()
    return report


if __name__ == "__main__":
    integration_all()