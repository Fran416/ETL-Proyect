"""
ETL Pipeline: Amazon → MongoDB | Redis Cart Simulation

Simulates a Cyberday event with real-time data flow.
Stages:
1. CHECK CONNECTIONS (MongoDB, Redis)
2. EXTRACT: Raw data ingestion
3. TRANSFORM: Data cleaning
4. LOAD: Database persistence
5. SIMULATOR: Traffic generation
6. INTEGRATION & VISUALIZATION: Analysis and plots
"""

import sys
from datetime import datetime

from src.config import get_mongo_connection, get_redis_connection
from src.core import integration_all, run_simulation
from src.etl import extract_all, get_transformation_stats, load_all, transform_all
from src.visualization import generate_all_visualizations


def print_header(title: str):
    """Prints a section header."""
    print(f"\n{'='*10} {title} {'='*10}")


def main():
    """Main execution entry point."""
    start_time = datetime.now()

    # 1. CHECK CONNECTIONS (Silent unless error)
    mongo_client, _, _ = get_mongo_connection()
    if not mongo_client:
        print("Error: MongoDB connection failed (is mongod running?)")
        sys.exit(1)
    mongo_client.close()

    redis_client = get_redis_connection()
    if not redis_client:
        print("Error: Redis connection failed (is redis-server running?)")
        sys.exit(1)

    # 2. EXTRACT
    amazon_df, redis_cart_df = extract_all()
    if amazon_df is None or redis_cart_df is None:
        print("Critical Error: Data load failed.")
        sys.exit(1)

    # 3. TRANSFORM
    amazon_transformed, cart_transformed = transform_all()
    stats = get_transformation_stats(amazon_transformed, cart_transformed)

    # 4. LOAD
    if not load_all(amazon_transformed, cart_transformed):
        print(" Warning: Partial data save issues.")

    # 5. SIMULATION
    try:
        simulation_df = run_simulation(num_customers=100, num_events=30000, save_csv=True)
    except Exception as e:
        print(f"Simulation error: {e}")
        simulation_df = None

    # 6. INTEGRATION & ANALYSIS
    integration_all()

    # 7. VISUALIZATIONS
    try:
        generate_all_visualizations()
    except Exception as e:
        print(f"Plot generation error: {e}")

    # ===== FINAL SUMMARY =====
    print_header("CYBERDAY ETL SUMMARY")
    
    # Simulation Stats
    if simulation_df is not None:
        rev = simulation_df['revenue'].sum()
        lost = simulation_df['lost_revenue'].sum()
        rate = (rev / (rev + lost) * 100) if (rev + lost) > 0 else 0
        print(f"Simulation:")
        print(f"   • Events: {len(simulation_df):,}")
        print(f"   • Revenue: ${rev:,.2f} | Lost: ${lost:,.2f}")
        print(f"   • Success Rate: {rate:.1f}%")

    # Data Stats
    p_stats = stats['products']
    c_stats = stats['carts']
    print(f"\nData Stats:")
    print(f"   • Products: {p_stats['total']} ({p_stats['categories']} cats)")
    print(f"   • Carts: {c_stats['unique_carts']} (from {c_stats['unique_customers']} customers)")
    
    duration = datetime.now() - start_time
    print(f"\nCompleted in {duration.total_seconds():.1f}s")


if __name__ == "__main__":
    main()