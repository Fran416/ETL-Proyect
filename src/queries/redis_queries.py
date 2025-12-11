"""
Independent Redis queries (raw cart data).

This module provides functions to query carts stored in Redis
directly without passing through the ETL pipeline.

All queries return data as stored, without additional transformations.
"""

import json
from typing import List, Dict, Optional, Any
from src.config.database import get_redis_connection


def get_all_cart_ids() -> List[str]:
    """
    Retrieves all active cart keys from Redis.
    
    Returns:
        List of cart keys (e.g., 'cart:CART-001').
    """
    r = get_redis_connection()
    if r is None:
        return []
    
    return r.keys("cart:CART-*")


def get_cart_by_id(cart_id: str) -> Dict[str, Any]:
    """
    Retrieves full details for a specific cart.
    
    Args:
        cart_id: Cart ID (e.g., 'CART-001' or 'cart:CART-001').
        
    Returns:
        Dictionary with cart data.
    """
    r = get_redis_connection()
    if r is None:
        return {}
    
    # Ensure key has prefix
    key = cart_id if cart_id.startswith("cart:") else f"cart:{cart_id}"
    
    data = r.hgetall(key)
    if not data:
        return {}
        
    # Parse events JSON string back to list
    if 'events' in data:
        try:
            data['events'] = json.loads(data['events'])
        except json.JSONDecodeError:
            data['events'] = []
            
    # Convert numeric strings
    for field in ['total_revenue', 'lost_revenue']:
        if field in data:
            try:
                data[field] = float(data[field])
            except ValueError:
                pass
                
    return data


def get_cart_events(cart_id: str) -> List[Dict[str, Any]]:
    """
    Retrieves only the list of events for a specific cart.
    
    Args:
        cart_id: Cart ID.
        
    Returns:
        List of event dictionaries.
    """
    details = get_cart_by_id(cart_id)
    return details.get('events', [])


def get_carts_by_customer(customer_id: str) -> List[Dict[str, Any]]:
    """
    Retrieves all carts belonging to a specific customer.
    Note: This requires scanning all carts, which can be expensive.
    
    Args:
        customer_id: Customer ID.
        
    Returns:
        List of cart objects.
    """
    r = get_redis_connection()
    if r is None:
        return []
    
    cart_keys = get_all_cart_ids()
    customer_carts = []
    
    for key in cart_keys:
        # Optimistic check: get only customer_id field first
        stored_cid = r.hget(key, 'customer_id')
        if stored_cid == customer_id:
            customer_carts.append(get_cart_by_id(key))
            
    return customer_carts


def count_carts_by_customer(customer_id: str) -> int:
    """
    Counts how many carts a specific customer has.
    
    Args:
        customer_id: Customer ID.
        
    Returns:
        Number of carts.
    """
    return len(get_carts_by_customer(customer_id))


def get_total_revenue() -> float:
    """
    Calculates total revenue across all carts.
    
    Returns:
        Total revenue as float.
    """
    r = get_redis_connection()
    if r is None:
        return 0.0
        
    cart_keys = get_all_cart_ids()
    total_revenue = 0.0
    
    for key in cart_keys:
        val = r.hget(key, 'total_revenue')
        if val:
            try:
                total_revenue += float(val)
            except ValueError:
                pass
            
    return total_revenue
