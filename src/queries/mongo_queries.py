"""
Independent MongoDB queries (raw product data).

This module provides functions to query the MongoDB product collection
directly without passing through the ETL pipeline.

All queries return data as stored, without additional transformations.
"""

from typing import List, Dict, Optional, Any
from src.config.database import get_mongo_connection


def get_all_products(limit: int = 0) -> List[Dict[str, Any]]:
    """
    Retrieves all products from the collection.
    
    Args:
        limit: Max number of products to return (0 for all).
        
    Returns:
        List of product documents.
    """
    _, _, collection = get_mongo_connection()
    if collection is None:
        return []

    cursor = collection.find({}, {'_id': 0})
    
    if limit > 0:
        cursor = cursor.limit(limit)
        
    return list(cursor)


def get_product_by_id(product_id: str) -> Optional[Dict[str, Any]]:
    """
    Retrieves a single product by its ID.
    
    Args:
        product_id: The unique product identifier.
        
    Returns:
        Product document or None if not found.
    """
    _, _, collection = get_mongo_connection()
    if collection is None:
        return None

    return collection.find_one({'product_id': product_id}, {'_id': 0})


def get_products_by_category(category: str) -> List[Dict[str, Any]]:
    """
    Retrieves products belonging to a specific category.
    
    Args:
        category: Category name (regex match supported).
        
    Returns:
        List of matching products.
    """
    _, _, collection = get_mongo_connection()
    if collection is None:
        return []

    query = {'category': {'$regex': category, '$options': 'i'}}
    return list(collection.find(query, {'_id': 0}))


def count_products_by_category() -> List[Dict[str, Any]]:
    """
    Counts products per category.
    
    Returns:
        List of dicts with 'category' and 'count'.
    """
    _, _, collection = get_mongo_connection()
    if collection is None:
        return []

    pipeline = [
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    results = list(collection.aggregate(pipeline))
    return [{"category": r["_id"], "count": r["count"]} for r in results]


def get_products_by_price_range(min_price: float, max_price: float) -> List[Dict[str, Any]]:
    """
    Retrieves products within a price range.
    
    Args:
        min_price: Minimum price.
        max_price: Maximum price.
        
    Returns:
        List of matching products.
    """
    _, _, collection = get_mongo_connection()
    if collection is None:
        return []

    query = {
        'discounted_price': {
            '$gte': min_price,
            '$lte': max_price
        }
    }
    return list(collection.find(query, {'_id': 0}))


def get_top_rated_products(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Retrieves top rated products.
    Assumes 'rating' and 'rating_count' fields exist.
    
    Args:
        limit: Max number of products.
        
    Returns:
        List of products sorted by rating.
    """
    _, _, collection = get_mongo_connection()
    if collection is None:
        return []

    # Sort by rating (desc) and then rating_count (desc)
    # Using 'rating' field if exists, otherwise this might return empty or unsorted depending on data
    cursor = collection.find(
        {'rating': {'$exists': True}}, 
        {'_id': 0}
    ).sort([
        ('rating', -1),
        ('rating_count', -1)
    ]).limit(limit)
    
    return list(cursor)