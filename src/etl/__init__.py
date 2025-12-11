"""
ETL Package: Extract, Transform, Load.

This package contains the main modules of the ETL pipeline.
"""

from src.etl.extract import extract_all, load_amazon_data, load_redis_cart_simulation
from src.etl.load import load_all, load_carts_to_redis, load_products_to_mongodb
from src.etl.transform import (
    get_transformation_stats,
    transform_all,
    transform_amazon_products,
    transform_redis_carts,
)

__all__ = [
    # Extract
    "extract_all",
    "load_amazon_data",
    "load_redis_cart_simulation",
    # Transform
    "transform_all",
    "transform_amazon_products",
    "transform_redis_carts",
    "get_transformation_stats",
    # Load
    "load_all",
    "load_products_to_mongodb",
    "load_carts_to_redis",
]
