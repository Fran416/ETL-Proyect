"""
Centralized configuration for MongoDB, Redis, and dataset paths.
"""

from pymongo import MongoClient
import redis

# ===== MONGODB CONFIGURATION =====
# Cleaned Amazon product data is stored here
MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "amazon_db"
MONGO_COLLECTION = "amazon_products"


def get_mongo_connection(collection_name: str = MONGO_COLLECTION):
    """Gets MongoDB connection (db and collection)."""
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB]
        collection = db[collection_name]

        client.server_info()  # Test connection
        return client, db, collection
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None, None, None


# ===== REDIS CONFIGURATION =====
REDIS_HOST = "localhost"
REDIS_PORT = 6379
REDIS_DB = 0


def get_redis_connection():
    """Gets Redis connection."""
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            decode_responses=True,
        )

        r.ping()  # Test connection
        return r
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        return None


# ===== FILE PATHS =====
AMAZON_CSV = "data/raw/amazon.csv"
REDIS_CART_CSV = "data/raw/redis_cart_sim.csv"
PROCESSED_CSV = "data/processed/amazon_processed.csv"

if __name__ == "__main__":
    print("Testing configuration...")
    print(f"Amazon Dataset: {AMAZON_CSV}")

    print("\nTesting connections...")

    mongo_client, mongo_db, mongo_col = get_mongo_connection()
    redis_client = get_redis_connection()

    if mongo_client and redis_client:
        print("\n[CONFIG] All connections work!")
    else:
        print("\n[CONFIG] Check your configuration")
