"""
TRANSFORM Module: Data cleaning and transformation of the ETL pipeline.

This module implements the T (Transform) phase of the ETL process, performing:
- Data integrity validation
- Format normalization (prices, percentages, dates)
- Missing value and outlier cleaning
- Business rule application
- Data quality statistics generation

Data sources:
- Amazon Products: Product catalog with prices and categories
- Redis Cart Events: Real-time shopping cart events
"""

from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import pandas as pd

from src.etl.extract import extract_all
from src.utils import (
    clean_percentage_column,
    clean_price_column,
    safe_numeric_conversion,
)

# ========================================================================
# BUSINESS VALIDATION CONSTANTS
# ========================================================================

# Quantities per transaction (business limits)
MIN_QUANTITY = 1
MAX_QUANTITY = 100


# ========================================================================
# MAIN TRANSFORMATION FUNCTIONS
# ========================================================================


def transform_amazon_products(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Transforms and cleans Amazon product data.

    Operations performed:
    - Removes unnecessary columns (reviews, ratings, images)
    - Filters products without name or ID
    - Normalizes prices and discount percentages

    Args:
        df: DataFrame with raw Amazon product data

    Returns:
        Transformed DataFrame or None if input is invalid
    """
    if df is None or df.empty:
        return None

    df = df.copy()
    
    # Integrity validation: remove records without critical identifiers
    # Step 1: Remove rows with NaN/None in mandatory fields
    df = df.dropna(subset=["product_name", "product_id"])

    # Step 2: Filter empty strings or whitespace only
    df = df[df["product_name"].astype(str).str.strip() != ""]
    df = df[df["product_id"].astype(str).str.strip() != ""]

    # Schema cleanup: remove columns not used in ETL pipeline
    # These columns add noise without value for sales/cart analysis
    unnecessary_fields = [
        'user_id', 'user_name', 'review_id', 'review_title',
        'review_content', 'img_link', 'product_link', 'rating', 'rating_count'
    ]
    df = df.drop(
        columns=[col for col in unnecessary_fields if col in df.columns],
        errors='ignore'
    )
    # print("[TRANSFORM] Metadata fields removed (reviews, ratings, links, users)")

    # Normalize missing values with semantically correct defaults
    df["about_product"] = df["about_product"].fillna("")

    # Clean and convert monetary columns
    # Removes currency symbols (₹), thousands separators (,) and converts to float
    df["actual_price"] = safe_numeric_conversion(
        clean_price_column(df["actual_price"]), default=0
    )
    df["discounted_price"] = safe_numeric_conversion(
        clean_price_column(df["discounted_price"]), default=0
    )
    df["discount_percentage"] = safe_numeric_conversion(
        clean_percentage_column(df["discount_percentage"]), default=0
    )

    # print(f"[TRANSFORM] {len(df)} Amazon products transformed")
    return df


def transform_redis_carts(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """
    Transforms and cleans cart event data.

    Operations performed:
    - Converts timestamps to datetime format
    - Validates quantities within permitted ranges
    - Normalizes stock and revenue values
    - Converts appropriate data types

    Args:
        df: DataFrame with raw cart events

    Returns:
        Transformed DataFrame or None if input is invalid
    """
    if df is None or df.empty:
        return None

    df = df.copy()

    # Temporal conversion: parse timestamps to pandas datetime objects
    # Allows temporal operations and time series analysis
    df["event_time"] = pd.to_datetime(df["event_time"], errors="coerce")

    # Quantity validation: ensure values within business limits
    # Permitted range: 1-100 units per transaction
    df["quantity"] = safe_numeric_conversion(df["quantity"], default=MIN_QUANTITY).astype(int)
    df["quantity"] = df["quantity"].clip(lower=MIN_QUANTITY, upper=MAX_QUANTITY)

    # Inventory and financial metrics conversion
    # Stock: non-negative integer values
    # Revenue: float values for monetary precision
    df["stock_before"] = safe_numeric_conversion(df["stock_before"], default=0).astype(int)
    df["stock_after"] = safe_numeric_conversion(df["stock_after"], default=0).astype(int)
    df["revenue"] = safe_numeric_conversion(df["revenue"], default=0).astype(float)
    df["lost_revenue"] = safe_numeric_conversion(df["lost_revenue"], default=0).astype(float)

    # print(f"[TRANSFORM] {len(df)} cart events transformed")
    return df


# ========================================================================
# METRICS AND STATISTICS FUNCTIONS
# ========================================================================


def get_transformation_stats(
    amazon_df: Optional[pd.DataFrame], cart_df: Optional[pd.DataFrame]
) -> Dict[str, dict]:
    """
    Gets transformation statistics.

    Args:
        amazon_df: Transformed Amazon product DataFrame
        cart_df: Transformed cart event DataFrame

    Returns:
        Dictionary with product and cart statistics
    """
    stats = {
        "products": {
            "total": len(amazon_df) if amazon_df is not None else 0,
            "categories": amazon_df["category"].nunique() if amazon_df is not None else 0,
            "avg_discount": amazon_df["discount_percentage"].mean() if amazon_df is not None else 0,
        },
        "carts": {
            "total_events": len(cart_df) if cart_df is not None else 0,
            "unique_carts": cart_df["cart_id"].nunique() if cart_df is not None else 0,
            "unique_customers": cart_df["customer_id"].nunique() if cart_df is not None else 0,
            "total_revenue": cart_df["revenue"].sum() if cart_df is not None else 0,
            "lost_revenue": cart_df["lost_revenue"].sum() if cart_df is not None else 0,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    return stats


# ========================================================================
# MAIN ORCHESTRATION FUNCTION
# ========================================================================


def transform_all() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Executes the complete TRANSFORM stage.

    Returns:
        Tuple with (transformed Amazon products, transformed cart events)
    """
    # print("\n[TRANSFORM] Starting transformation...\n")

    # Step 1: Extract raw data from CSV sources
    amazon_df, redis_cart_df = extract_all()

    # Step 2: Apply specific transformations by data type
    amazon_transformed = transform_amazon_products(amazon_df)
    cart_transformed = transform_redis_carts(redis_cart_df)

    # Step 3: Calculate post-transformation data quality metrics
    stats = get_transformation_stats(amazon_transformed, cart_transformed)

    # Step 4: Show executive summary of transformation (only if running main or verbose)
    # Keeping it minimal as per "silent by default", but useful for debugging
    # print(f"[TRANSFORM] Status: Products={stats['products']['total']}, Carts={stats['carts']['unique_carts']}")

    return amazon_transformed, cart_transformed


def main():
    """Executes the TRANSFORM module independently."""
    amazon, carts = transform_all()
    if amazon is not None and carts is not None:
         stats = get_transformation_stats(amazon, carts)
         print("\n[TRANSFORM] Statistics:")
         print(f"  Products: {stats['products']['total']}")
         print(f"  Categories: {stats['products']['categories']}")
         print(f"  Avg Discount: {stats['products']['avg_discount']:.2f}%")
         print(f"  Carts: {stats['carts']['unique_carts']}")
         print(f"  Revenue: ${stats['carts']['total_revenue']:.2f}")
         print(f"  Lost Revenue: ${stats['carts']['lost_revenue']:.2f}")


if __name__ == "__main__":
    main()
