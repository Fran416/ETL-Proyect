"""
EXTRACT Stage: Reads raw datasets from Amazon and Redis cart simulation.
For ETL with MongoDB (catalog) and Redis (real-time carts).
"""

from pathlib import Path
from typing import Optional, Tuple
import pandas as pd

from src.config import AMAZON_CSV, REDIS_CART_CSV


def _load_csv(path_str: str) -> Optional[pd.DataFrame]:
    """Reads a CSV and returns a DataFrame with basic validations."""
    path = Path(path_str)
    if not path.is_file():
        print(f"[EXTRACT] Error: File not found: {path}")
        return None

    df = pd.read_csv(path)

    # Validation 1: Empty file
    if len(df) == 0:
        print("[EXTRACT] Error: File is empty")
        return None

    # Validation 2: Null values detected
    null_count = df.isnull().sum().sum()
    if null_count > 0:
        print(f"[EXTRACT] Warning: {null_count} null values detected in {path.name}")

    # Validation 3: Duplicates detected
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        print(f"[EXTRACT] Warning: {duplicate_count} duplicate records in {path.name}")

    # Validation 4: Date/time formats
    date_cols = [col for col in df.columns if 'time' in col.lower() or 'date' in col.lower()]
    if date_cols:
        for col in date_cols:
            try:
                valid_dates = pd.to_datetime(df[col], errors='coerce')
                invalid_count = valid_dates.isnull().sum() - df[col].isnull().sum()
                if invalid_count > 0:
                    print(f"[EXTRACT] Warning: {col} has {invalid_count} invalid dates")
            except Exception as e:
                print(f"[EXTRACT] Error validating dates in {col}: {e}")

    return df


def load_amazon_data() -> Optional[pd.DataFrame]:
    """Loads Amazon product dataset for MongoDB."""
    return _load_csv(AMAZON_CSV)


def load_redis_cart_simulation() -> Optional[pd.DataFrame]:
    """Loads cart simulation for Redis."""
    return _load_csv(REDIS_CART_CSV)


def extract_all() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """Executes EXTRACT stage reading both datasets."""
    # print("\n[EXTRACT] Starting data extraction...\n")

    amazon_df = load_amazon_data()
    redis_cart_df = load_redis_cart_simulation()

    if amazon_df is None or redis_cart_df is None:
        print("[EXTRACT] Error: One or more datasets failed to load")
    
    return amazon_df, redis_cart_df


def main():
    """Executes EXTRACT module independently."""
    extract_all()


if __name__ == "__main__":
    main()
