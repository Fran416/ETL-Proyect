# Cyberday ETL Pipeline

A complete ETL pipeline simulating a Cyberday e-commerce event with real-time shopping cart analysis.

## Overview

This project processes e-commerce data using:
- **MongoDB** for the product catalog (Flipkart/Amazon dataset).
- **Redis** for real-time shopping carts and event tracking.

The pipeline performs Extract, Transform, Load (ETL) operations and integrates data to analyze conversion rates, revenue, and cart abandonment.

## Requirements

- Python 3.8+
- MongoDB (running locally or remote)
- Redis (running locally or remote)

## Project Structure

The project follows a modular structure separating concerns:
- `src/etl/`: Data extraction, transformation, and loading.
- `src/core/`: Business logic, simulation, and integration.
- `src/visualization/`: Chart generation.
- `src/config/`: Configuration settings.

## How to Run

### 1. Configure Operations Environment

It is recommended to use a virtual environment to manage dependencies.

```bash
# Create virtual environment
python -m venv venv

# Activate (Linux/Mac)
source venv/bin/activate

# Activate (Windows)
# venv\Scripts\activate
```

### 2. Install Dependencies

Install the required Python packages using pip:

```bash
pip install -r requirements.txt
```

### 3. Start Database Services

Ensure your database servers are up and running before executing the pipeline.

```bash
# Terminal 1: Start MongoDB
mongod

# Terminal 2: Start Redis
redis-server
```

### 4. Execute the Pipeline

Run the main script to trigger the full ETL process, simulation, and reporting.

```bash
python main.py
```

## Pipeline Execution Details

The `main.py` script executes the following stages sequentially:

1.  **Check Connections**: Verifies connectivity to MongoDB and Redis.
2.  **Extract**: Loads raw data from `data/raw/`.
3.  **Transform**: Cleanses data, normalizes prices, and validates schema.
4.  **Load**: Persists processed data into MongoDB (Products) and Redis (Carts).
5.  **Simulator**: Generates synthetic user traffic and shopping events.
6.  **Integration**: Cross-references MongoDB products with Redis cart events.
7.  **Visualization**: Generates performance charts in `data/processed/`.

## Output

After execution, you will find:
- **Console Summary**: Statistics on revenue, conversion rates, and data volume.
- **Visualizations**: Charts spanning various metrics in `data/processed/`:
    - `categories_distribution.png`
    - `price_distribution.png`
    - `top_selling_products.png`
    - `revenue_comparison.png`
    - And more.

## Troubleshooting

- **MongoDB connection failed**: Ensure `mongod` is running and port 27017 is open.
- **Redis connection failed**: Ensure `redis-server` is running on port 6379.
- **Module not found**: Check that you have activated your virtual environment and installed `requirements.txt`.
