"""
CYBERDAY SIMULATOR
Simulates real-time shopping, calculates lost revenue,
best-selling products, categories, etc.
"""

import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import pandas as pd
from src.config import get_mongo_connection, get_redis_connection
import json
import time


class CyberdaySimulator:
    """Cyber Day Simulator with Amazon products."""
    
    def __init__(self, num_customers: int = 50, num_events: int = 200):
        """
        Initializes the simulator.
        
        Args:
            num_customers: Number of simulated customers
            num_events: Total events to generate
        """
        self.num_customers = num_customers
        self.num_events = num_events
        self.products = []
        self.simulation_start = datetime.now()
        
    def load_products_from_mongo(self) -> bool:
        """Loads products from MongoDB."""
        try:
            _, _, collection = get_mongo_connection()
            if collection is None:
                print("[SIMULATOR] Error: Could not connect to MongoDB")
                return False
            
            # Load all products
            self.products = list(collection.find({}))
            
            if not self.products:
                print("[SIMULATOR] Error: No products in MongoDB")
                return False
                
            return True
            
        except Exception as e:
            print(f"[SIMULATOR] Error loading products: {e}")
            return False
    
    def simulate_cyberday(self, save_to_redis: bool = True) -> pd.DataFrame:
        """
        Simulates a complete Cyber Day.
        
        Returns:
            DataFrame with all simulated events
        """
        if not self.load_products_from_mongo():
            return None
        
        events = []
        cart_id = 1
        
        # Dictionary for real-time stock tracking
        stock_tracker = {}
        stock_out_times = {}  # Tracks stock-out times
        
        for product in self.products:
            stock_tracker[product['product_id']] = product['stock']
        
        # Generate events
        current_time = self.simulation_start
        
        for i in range(self.num_events):
            # Select customer
            customer_id = f"CUST-{random.randint(1, self.num_customers):03d}"
            
            # Select random product
            product = random.choice(self.products)
            product_id = product['product_id']
            product_name = product['product_name']
            price = product['discounted_price']
            category = product['category']
            
            # Quantity (1-5 units)
            quantity = random.randint(1, 5)
            
            # Increment time
            current_time += timedelta(seconds=random.randint(1, 10))
            
            # Check available stock
            available_stock = stock_tracker.get(product_id, 0)
            
            # Decide event type
            if available_stock <= 0:
                # OUT OF STOCK - record lost sale
                event_type = "stock_out"
                stock_before = 0
                stock_after = 0
                revenue = 0
                lost_revenue = price * quantity
                
                # Record stock out time if first time
                if product_id not in stock_out_times:
                    stock_out_times[product_id] = {
                        'time': current_time,
                        'duration_seconds': (current_time - self.simulation_start).total_seconds(),
                        'product_name': product_name,
                        'category': category
                    }
                
            elif available_stock < quantity:
                # Insufficient stock - partial checkout
                quantity_sold = available_stock
                quantity_lost = quantity - available_stock
                
                event_type = "partial_checkout"
                stock_before = available_stock
                stock_after = 0
                revenue = price * quantity_sold
                lost_revenue = price * quantity_lost
                
                stock_tracker[product_id] = 0
                
                # Record stock out
                if product_id not in stock_out_times:
                    stock_out_times[product_id] = {
                        'time': current_time,
                        'duration_seconds': (current_time - self.simulation_start).total_seconds(),
                        'product_name': product_name,
                        'category': category
                    }
                
            else:
                # Stock available - random decision
                action = random.choices(
                    ['checkout', 'add', 'abandon'],
                    weights=[0.6, 0.3, 0.1],  # 60% checkout, 30% add, 10% abandon
                    k=1
                )[0]
                
                stock_before = available_stock
                
                if action == 'checkout':
                    event_type = "checkout"
                    stock_after = available_stock - quantity
                    revenue = price * quantity
                    lost_revenue = 0
                    stock_tracker[product_id] = stock_after
                    
                elif action == 'add':
                    event_type = "add"
                    stock_after = available_stock  # Not deducted yet
                    revenue = 0
                    lost_revenue = 0
                    
                else:  # abandon
                    event_type = "abandon"
                    stock_after = available_stock
                    revenue = 0
                    lost_revenue = 0
            
            # Create event
            event = {
                'cart_id': f"CART-{cart_id:03d}",
                'customer_id': customer_id,
                'event_time': current_time,
                'event_type': event_type,
                'product_id': product_id,
                'product_name': product_name,
                'category': category,
                'quantity': quantity,
                'price': price,
                'stock_before': stock_before,
                'stock_after': stock_after,
                'revenue': revenue,
                'lost_revenue': lost_revenue
            }
            
            events.append(event)
            
            # Increment cart_id randomly
            if random.random() > 0.7:  # 30% chance of new cart
                cart_id += 1
        
        # Create DataFrame
        df = pd.DataFrame(events)
        
        # Save to Redis if requested
        if save_to_redis:
            self._save_events_to_redis(df)
            self._save_stock_out_times_to_redis(stock_out_times)
        
        return df
    
    def _save_stock_out_times_to_redis(self, stock_out_times: Dict):
        """Saves stock-out times to Redis."""
        try:
            redis_client = get_redis_connection()
            if redis_client is None:
                return
            
            for product_id, data in stock_out_times.items():
                key = f"stock_out:{product_id}"
                redis_client.hset(key, mapping={
                    'product_name': data['product_name'],
                    'category': data['category'],
                    'time': data['time'].isoformat(),
                    'duration_seconds': data['duration_seconds']
                })
            
            redis_client.close()
            
        except Exception as e:
            print(f"[SIMULATOR] Error saving stock-out times: {e}")
    
    def _save_events_to_redis(self, df: pd.DataFrame):
        """Saves events grouped by cart to Redis."""
        try:
            redis_client = get_redis_connection()
            if redis_client is None:
                return
            
            # Clear Redis
            redis_client.flushdb()
            
            # Group by cart
            for cart_id in df['cart_id'].unique():
                cart_events = df[df['cart_id'] == cart_id]
                
                events_list = []
                total_revenue = 0
                total_lost = 0
                
                for _, row in cart_events.iterrows():
                    event = {
                        'event_time': row['event_time'].isoformat(),
                        'event_type': row['event_type'],
                        'product_id': row['product_id'],
                        'product_name': row['product_name'],
                        'category': row['category'],
                        'quantity': int(row['quantity']),
                        'price': float(row['price']),
                        'stock_before': int(row['stock_before']),
                        'stock_after': int(row['stock_after']),
                        'revenue': float(row['revenue']),
                        'lost_revenue': float(row['lost_revenue'])
                    }
                    events_list.append(event)
                    total_revenue += row['revenue']
                    total_lost += row['lost_revenue']
                
                # Save to Redis
                redis_client.hset(
                    f"cart:{cart_id}",
                    mapping={
                        'customer_id': cart_events.iloc[0]['customer_id'],
                        'events': json.dumps(events_list),
                        'total_revenue': total_revenue,
                        'lost_revenue': total_lost,
                        'created_at': datetime.now().isoformat()
                    }
                )
            
            redis_client.close()
            
        except Exception as e:
            print(f"[SIMULATOR] Error saving events: {e}")


def run_simulation(num_customers: int = 50, num_events: int = 200, save_csv: bool = True) -> pd.DataFrame:
    """
    Runs the full simulation.
    
    Args:
        num_customers: Number of customers
        num_events: Number of events
        save_csv: Whether to save result to CSV
        
    Returns:
        DataFrame with all events
    """
    simulator = CyberdaySimulator(num_customers, num_events)
    df = simulator.simulate_cyberday(save_to_redis=True)
    
    if df is not None and save_csv:
        output_path = "data/processed/cyberday_simulation.csv"
        df.to_csv(output_path, index=False)
    
    return df


if __name__ == "__main__":
    # Run simulation with default values
    df = run_simulation(num_customers=100, num_events=500)
    
    if df is not None:
        print("\n" + "="*70)
        print("SIMULATION SUMMARY")
        print("="*70)
        print(f"\nTotal events: {len(df)}")
        print(f"Total revenue: ${df['revenue'].sum():,.2f}")
        print(f"Lost revenue: ${df['lost_revenue'].sum():,.2f}")
        print(f"Success rate: {(df['revenue'].sum() / (df['revenue'].sum() + df['lost_revenue'].sum()) * 100):.1f}%")
