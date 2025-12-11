"""
Visualization for Cyberday analysis: MongoDB + Redis.
"""

import json
from pathlib import Path

import matplotlib.pyplot as plt

from src.config import get_mongo_connection, get_redis_connection


# Output directory for images
OUTPUT_DIR = Path("data/processed")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def plot_product_categories_distribution():
    """Chart of product distribution by category."""
    try:
        mongo_client, _, collection = get_mongo_connection()
        if collection is None:
            return

        pipeline = [
            {"$group": {"_id": "$category", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 15},
        ]

        results = list(collection.aggregate(pipeline))
        if not results:
            print("[VIZ] No product data found")
            return

        categories = [r.get("_id", "No Category") for r in results]
        counts = [r["count"] for r in results]

        plt.figure(figsize=(12, 6))
        plt.barh(categories, counts, color="steelblue")
        plt.xlabel("Number of Products")
        plt.title("Top 15 Categories by Product Count - Amazon")
        plt.tight_layout()
        output_path = OUTPUT_DIR / "categories_distribution.png"
        plt.savefig(output_path, dpi=100, bbox_inches="tight")
        print(f"[VIZ] Chart saved: {output_path}")
        plt.close()

        mongo_client.close()

    except Exception as e:
        print(f"[VIZ] Error in category chart: {e}")


def plot_price_distribution():
    """Chart of price distribution."""
    try:
        mongo_client, _, collection = get_mongo_connection()
        if collection is None:
            return

        prices = [
            doc.get("discounted_price", 0)
            for doc in collection.find({}, {"discounted_price": 1})
            if doc.get("discounted_price", 0) > 0
        ]

        if not prices:
            print("[VIZ] No price data found")
            return

        plt.figure(figsize=(12, 6))
        plt.hist(prices, bins=50, color="coral", edgecolor="black", alpha=0.7)
        plt.xlabel("Discounted Price")
        plt.ylabel("Number of Products")
        plt.title("Price Distribution - Amazon")
        plt.tight_layout()
        output_path = OUTPUT_DIR / "price_distribution.png"
        plt.savefig(output_path, dpi=100, bbox_inches="tight")
        print(f"[VIZ] Chart saved: {output_path}")
        plt.close()

        mongo_client.close()

    except Exception as e:
        print(f"[VIZ] Error in price distribution: {e}")


def plot_cart_events_timeline():
    """Chart of cart events by type."""
    try:
        redis_client = get_redis_connection()
        if redis_client is None:
            return

        events_by_type = {
            "add": 0,
            "checkout": 0,
            "partial_checkout": 0,
            "abandon": 0,
            "stock_out": 0,
        }

        cart_keys = redis_client.keys("cart:CART-*")
        for key in cart_keys:
            cart_data = redis_client.hgetall(key)
            events = json.loads(cart_data.get("events", "[]"))
            for event in events:
                event_type = event.get("event_type", "unknown")
                events_by_type[event_type] = events_by_type.get(event_type, 0) + 1

        total_events = sum(events_by_type.values())
        if total_events == 0:
            print("[VIZ] No cart events found")
            return

        plt.figure(figsize=(10, 6))
        colors = ["#2ecc71", "#3498db", "#e74c3c", "#f39c12", "#9b59b6"]
        plt.bar(events_by_type.keys(), events_by_type.values(), color=colors[: len(events_by_type)])
        plt.xlabel("Event Type")
        plt.ylabel("Number of Events")
        plt.title("Cart Events - Amazon Cyberday")
        plt.tight_layout()
        output_path = OUTPUT_DIR / "cart_events.png"
        plt.savefig(output_path, dpi=100, bbox_inches="tight")
        print(f"[VIZ] Chart saved: {output_path}")
        plt.close()

        redis_client.close()

    except Exception as e:
        print(f"[VIZ] Error in cart events: {e}")


def plot_top_selling_products():
    """Chart of top selling products."""
    try:
        redis_client = get_redis_connection()
        mongo_client, _, collection = get_mongo_connection()

        if redis_client is None or collection is None:
            return

        sales = {}
        cart_keys = redis_client.keys("cart:CART-*")

        for key in cart_keys:
            cart_data = redis_client.hgetall(key)
            events = json.loads(cart_data.get("events", "[]"))

            for event in events:
                if event.get("event_type") in ["checkout", "partial_checkout"]:
                    product_id = event.get("product_id")
                    quantity = event.get("quantity", 0)
                    if product_id:
                        sales[product_id] = sales.get(product_id, 0) + quantity

        if not sales:
            print("[VIZ] No sales recorded")
            return

        top_products = sorted(sales.items(), key=lambda x: x[1], reverse=True)[:15]

        product_names = []
        quantities = []

        for product_id, qty in top_products:
            product = collection.find_one({"product_id": product_id})
            name = product.get("product_name", product_id) if product else product_id
            product_names.append(str(name)[:40])
            quantities.append(qty)

        plt.figure(figsize=(12, 8))
        plt.barh(product_names, quantities, color="steelblue")
        plt.xlabel("Units Sold")
        plt.title("Top 15 Best-Selling Products - Cyber Day")
        plt.tight_layout()
        output_path = OUTPUT_DIR / "top_selling_products.png"
        plt.savefig(output_path, dpi=100, bbox_inches="tight")
        print(f"[VIZ] Chart saved: {output_path}")
        plt.close()

        redis_client.close()
        mongo_client.close()

    except Exception as e:
        print(f"[VIZ] Error in top selling products: {e}")


def plot_top_categories():
    """Chart of top selling categories."""
    try:
        redis_client = get_redis_connection()
        if redis_client is None:
            return

        category_sales = {}
        cart_keys = redis_client.keys("cart:CART-*")

        for key in cart_keys:
            cart_data = redis_client.hgetall(key)
            events = json.loads(cart_data.get("events", "[]"))

            for event in events:
                if event.get("event_type") in ["checkout", "partial_checkout"]:
                    category = event.get("category", "Unknown")
                    main_category = category.split("|")[0] if "|" in category else category
                    revenue = event.get("revenue", 0)
                    category_sales[main_category] = category_sales.get(main_category, 0) + revenue

        if not category_sales:
            print("[VIZ] No category data found")
            return

        top_categories = sorted(category_sales.items(), key=lambda x: x[1], reverse=True)[:10]
        categories = [c[0][:30] for c in top_categories]
        revenues = [c[1] for c in top_categories]

        plt.figure(figsize=(12, 6))
        bars = plt.bar(range(len(categories)), revenues, color="coral")
        plt.xticks(range(len(categories)), categories, rotation=45, ha="right")
        plt.ylabel("Total Revenue (Rupees)")
        plt.title("Top 10 Best-Selling Categories - Cyber Day")

        for bar in bars:
            height = bar.get_height()
            plt.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"R{int(height):,}",
                ha="center",
                va="bottom",
                fontsize=8,
            )

        plt.tight_layout()
        output_path = OUTPUT_DIR / "top_categories.png"
        plt.savefig(output_path, dpi=100, bbox_inches="tight")
        print(f"[VIZ] Chart saved: {output_path}")
        plt.close()

        redis_client.close()

    except Exception as e:
        print(f"[VIZ] Error in top categories: {e}")


def plot_lost_revenue_breakdown():
    """Chart of lost revenue by category."""
    try:
        redis_client = get_redis_connection()
        if redis_client is None:
            return

        lost_by_category = {}
        cart_keys = redis_client.keys("cart:CART-*")

        for key in cart_keys:
            cart_data = redis_client.hgetall(key)
            events = json.loads(cart_data.get("events", "[]"))

            for event in events:
                lost = event.get("lost_revenue", 0)
                if lost > 0:
                    category = event.get("category", "Unknown")
                    main_category = category.split("|")[0] if "|" in category else category
                    lost_by_category[main_category] = lost_by_category.get(main_category, 0) + lost

        if not lost_by_category:
            print("[VIZ] No lost revenue recorded")
            return

        top_lost = sorted(lost_by_category.items(), key=lambda x: x[1], reverse=True)[:10]
        categories = [c[0][:25] for c in top_lost]
        losses = [c[1] for c in top_lost]

        plt.figure(figsize=(10, 6))
        plt.barh(categories, losses, color="#e74c3c")
        plt.xlabel("Lost Revenue (Rupees)")
        plt.title("Top 10 Categories with Highest Lost Revenue - Cyber Day")
        plt.tight_layout()
        output_path = OUTPUT_DIR / "lost_revenue_by_category.png"
        plt.savefig(output_path, dpi=100, bbox_inches="tight")
        print(f"[VIZ] Chart saved: {output_path}")
        plt.close()

        redis_client.close()

    except Exception as e:
        print(f"[VIZ] Error in lost revenue chart: {e}")


def plot_stock_out_times():
    """Chart of stock-out times."""
    try:
        redis_client = get_redis_connection()
        if redis_client is None:
            return

        stock_out_keys = redis_client.keys("stock_out:*")

        if not stock_out_keys:
            print("[VIZ] No products out of stock")
            return

        times = []
        names = []

        for key in list(stock_out_keys):
            data = redis_client.hgetall(key)
            duration_seconds = float(data.get("duration_seconds", 0))
            name = data.get("product_name", "Unknown")[:35]

            times.append(duration_seconds)
            names.append(name)

        # Build unique product map
        unique_products = {}
        for time_val, name in zip(times, names):
            if name not in unique_products or time_val < unique_products[name]:
                unique_products[name] = time_val
        
        # Sort and take top 15
        sorted_data = sorted(unique_products.items(), key=lambda x: x[1])[:15]
        times = [t for _, t in sorted_data]
        names = [n for n, _ in sorted_data]

        plt.figure(figsize=(12, 8))
        # Convert to hours for X axis
        times_hours = [t / 3600 for t in times]
        bars = plt.barh(names, times_hours, color="#f39c12")
        plt.xlabel("Time until Stock Out (hours)")
        plt.title("Most Sought-After Products (Sold Out Fastest)")

        for bar, seconds in zip(bars, times):
            width = bar.get_width()
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            plt.text(
                width,
                bar.get_y() + bar.get_height() / 2.0,
                f"{hours}h {minutes}min",
                ha="left",
                va="center",
                fontsize=8,
            )

        plt.tight_layout()
        output_path = OUTPUT_DIR / "stock_out_times.png"
        plt.savefig(output_path, dpi=100, bbox_inches="tight")
        print(f"[VIZ] Chart saved: {output_path}")
        plt.close()

        redis_client.close()

    except Exception as e:
        print(f"[VIZ] Error in stock out times: {e}")


def plot_revenue_comparison():
    """Comparative chart: Revenue vs Lost Revenue."""
    try:
        redis_client = get_redis_connection()

        if redis_client is None:
            return

        cart_keys = redis_client.keys("cart:CART-*")
        total_revenue = 0
        total_lost = 0

        for key in cart_keys:
            cart_data = redis_client.hgetall(key)
            total_revenue += float(cart_data.get("total_revenue", 0))
            total_lost += float(cart_data.get("lost_revenue", 0))

        if total_revenue == 0 and total_lost == 0:
            print("[VIZ] No revenue data found")
            return

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

        categories = ["Revenue\nEarned", "Revenue\nLost"]
        values = [total_revenue, total_lost]
        colors = ["#27ae60", "#e74c3c"]

        bars = ax1.bar(categories, values, color=colors, alpha=0.7)
        ax1.set_ylabel("Rupees (₹)")
        ax1.set_title("Total Revenue vs Lost Revenue")

        for bar in bars:
            height = bar.get_height()
            ax1.text(
                bar.get_x() + bar.get_width() / 2.0,
                height,
                f"R{int(height):,}",
                ha="center",
                va="bottom",
                fontweight="bold",
            )

        total = total_revenue + total_lost
        percentages = [total_revenue / total * 100, total_lost / total * 100]

        ax2.pie(percentages, labels=categories, colors=colors, autopct="%1.1f%%", startangle=90)
        ax2.set_title("Potential Revenue Distribution")

        plt.tight_layout()
        output_path = OUTPUT_DIR / "revenue_comparison.png"
        plt.savefig(output_path, dpi=100, bbox_inches="tight")
        print(f"[VIZ] Chart saved: {output_path}")
        plt.close()

        redis_client.close()

    except Exception as e:
        print(f"[VIZ] Error in revenue comparison: {e}")


def generate_all_visualizations():
    """Generates all visualizations."""
    print("\n[VIZ] Generating Cyberday visualizations...\n")

    plot_product_categories_distribution()
    plot_price_distribution()
    plot_cart_events_timeline()
    plot_top_selling_products()
    plot_top_categories()
    plot_lost_revenue_breakdown()
    plot_stock_out_times()
    plot_revenue_comparison()

    print("\n[VIZ] All visualizations completed\n")


if __name__ == "__main__":
    generate_all_visualizations()
