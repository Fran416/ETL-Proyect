"""
UTILS Module: Shared helper functions for the ETL pipeline.

This module contains reusable helper functions for:
- Safe data type conversion
- Format cleaning (prices, percentages)
- File persistence
- Common validations

Author: ETL Team
Date: 2025
"""

from pathlib import Path
from typing import Union

import pandas as pd


# ========================================================================
# SAFE TYPE CONVERSION
# ========================================================================


def safe_float_conversion(value, default: float = 0.0) -> float:
    """
    Safely converts a value to float.

    Args:
        value: Value to convert
        default: Default value if conversion fails

    Returns:
        Converted float value or default
    """
    if pd.notna(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
    return default


def safe_int_conversion(value, default: int = 0) -> int:
    """
    Safely converts a value to int.

    Args:
        value: Value to convert
        default: Default value if conversion fails

    Returns:
        Converted int value or default
    """
    if pd.notna(value):
        try:
            return int(value)
        except (ValueError, TypeError):
            return default
    return default


def safe_numeric_conversion(series: pd.Series, default: float = 0) -> pd.Series:
    """
    Safely converts a series to numeric.

    Args:
        series: Pandas series to convert
        default: Default value for invalid values

    Returns:
        Series converted to numeric
    """
    return pd.to_numeric(series, errors="coerce").fillna(default)


# ========================================================================
# FORMAT CLEANING
# ========================================================================


def clean_price_column(series: pd.Series) -> pd.Series:
    """
    Cleans a price column by removing symbols and commas.

    Args:
        series: Pandas series with price values

    Returns:
        Series with cleaned prices
    """
    return (
        series.astype(str)
        .str.replace("₹", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )


def clean_percentage_column(series: pd.Series) -> pd.Series:
    """
    Cleans a percentage column by removing the % symbol.

    Args:
        series: Pandas series with percentage values

    Returns:
        Series with cleaned percentages
    """
    return series.astype(str).str.replace("%", "", regex=False).str.strip()


# ========================================================================
# DATA PERSISTENCE
# ========================================================================


def save_dataframe_to_csv(
    df: pd.DataFrame,
    output_dir: Union[str, Path],
    filename: str
) -> bool:
    """
    Saves a DataFrame to CSV.

    Args:
        df: DataFrame to save
        output_dir: Output directory
        filename: Filename (without path)

    Returns:
        True if saved successfully, False otherwise
    """
    try:
        out_path = Path(output_dir) / filename
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        # print(f"[UTILS] Dataset saved to {out_path}")
        return True
    except Exception as e:
        print(f"[UTILS] Error saving CSV: {e}")
        return False


# ========================================================================
# VALIDATIONS
# ========================================================================


def validate_dataframe(df: pd.DataFrame, name: str = "DataFrame") -> bool:
    """
    Validates that a DataFrame is not None or empty.

    Args:
        df: DataFrame to validate
        name: Descriptive name for messages

    Returns:
        True if DataFrame is valid, False otherwise
    """
    if df is None:
        print(f"[UTILS] Error: {name} is None")
        return False

    if df.empty:
        print(f"[UTILS] Error: {name} is empty")
        return False

    return True


def clip_to_range(
    series: pd.Series,
    min_value: float,
    max_value: float
) -> pd.Series:
    """
    Limits the values of a series to a specific range.

    Args:
        series: Pandas series
        min_value: Minimum allowed value
        max_value: Maximum allowed value

    Returns:
        Series with values within the range
    """
    return series.clip(lower=min_value, upper=max_value)
