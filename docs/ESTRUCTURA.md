# ETL Project Structure

## Folder Organization

```
Elt-base-de-datos/
│
├── src/                        # Main source code
│   ├── etl/                    # ETL Modules (Extract, Transform, Load)
│   │   ├── __init__.py
│   │   ├── extract.py          # Data extraction from CSVs
│   │   ├── transform.py        # Data cleaning and transformation
│   │   └── load.py             # Load to MongoDB and Redis
│   │
│   ├── core/                   # Main Business Logic
│   │   ├── __init__.py
│   │   ├── simulator.py        # Cyber Day Simulator
│   │   ├── integration.py      # Cross-analysis MongoDB + Redis
│   │   └── analytics.py        # Advanced metrics and analytics
│   │
│   ├── visualization/          # Charts and reports generation
│   │   ├── __init__.py
│   │   └── charts.py           # Visualizations with matplotlib
│   │
│   ├── utils/                  # Shared Utilities
│   │   ├── __init__.py
│   │   ├── helpers.py          # Helper functions (conversions, cleaning)
│   │   └── validators.py       # Data validators (future)
│   │
│   ├── config/                 # System Configurations
│   │   ├── __init__.py
│   │   ├── database.py         # MongoDB and Redis connections
│   │   └── constants.py        # Global constants (future)
│   │
│   └── __init__.py
│
├── data/                       # Project Data
│   ├── raw/                    # Raw unprocessed data
│   │   ├── amazon.csv
│   │   └── redis_cart_sim.csv
│   ├── processed/              # Cleaned and processed data
│   │   ├── amazon_processed.csv
│   │   └── carts_processed.csv
│   └── output/                 # Results and visualizations
│       └── *.png               # Generated charts
│
├── tests/                      # Unit and integration tests
│   ├── __init__.py
│   ├── test_etl/               # ETL Tests
│   ├── test_core/              # Business logic tests
│   └── test_utils/             # Utility tests
│
├── docs/                       # Project Documentation
│   ├── ESTRUCTURA.md           # This file
│   └── architecture.md         # System architecture
│
├── scripts/                    # Auxiliary Scripts
│   ├── setup_databases.py      # MongoDB and Redis setup
│   └── clean_data.py           # Data cleanup
│
├── main.py                     # Full pipeline entry point
├── requirements.txt            # Python dependencies
├── README.md                   # Main documentation
└── .gitignore                  # Git ignore rules
```

## Package Description

### src/etl/ - Main ETL Pipeline
**Responsibility**: Data extraction, transformation, and loading.

**Modules**:
- extract.py: Reads data from CSV files
- transform.py: Cleans and normalizes data (prices, dates, validations)
- load.py: Persists data in MongoDB and Redis + Audit CSVs

**Applied Principles**:
- Single Responsibility Principle (SRP)
- No business logic, only technical transformations

---

### src/core/ - Business Logic
**Responsibility**: Simulation, integration, and business analysis.

**Modules**:
- simulator.py: Simulates real-time Cyber Day events
- integration.py: Cross-analysis between MongoDB and Redis
- analytics.py: Business metrics and KPIs

**Applied Principles**:
- Separation of business logic from ETL
- Component reusability

---

### src/visualization/ - Reports and Charts
**Responsibility**: Visualization generation.

**Modules**:
- charts.py: Charts with matplotlib/seaborn

**Applied Principles**:
- Separation of data presentation
- Easy extension for new chart types

---

### src/utils/ - Shared Utilities
**Responsibility**: Reusable helper functions.

**Modules**:
- helpers.py: Conversions, format cleaning, persistence
- validators.py: Data validators (future)

**Applied Principles**:
- DRY (Don't Repeat Yourself)
- Pure and testable functions

---

### src/config/ - Configurations
**Responsibility**: Centralized system configuration.

**Modules**:
- database.py: MongoDB and Redis connections
- constants.py: Global constants (future)

**Applied Principles**:
- Centralized configuration
- Easy parameter modification

---

## Execution Flow

```
main.py
   ↓
   ├─→ src.config (get_mongo_connection, get_redis_connection)
   ├─→ src.etl.extract (extract_all)
   ├─→ src.etl.transform (transform_all)
   ├─→ src.etl.load (load_all)
   ├─→ src.core.simulator (run_simulation)
   ├─→ src.core.integration (integration_all)
   └─→ src.visualization (generate_all_visualizations)
```

## Advantages of this Structure

- **Modularity**: Each package has a clear responsibility
- **Scalability**: Easy to add new modules without affecting existing ones
- **Maintainability**: Organized code that is easy to find
- **Testability**: Structure facilitates unit testing
- **Professionalism**: Follows industry standards
- **Collaboration**: Multiple developers can work without conflicts

## Migration from Previous Structure

### Changes made:

| Previous File | New Location |
|-----------------|-----------------|
| src/extract.py | src/etl/extract.py |
| src/transform.py | src/etl/transform.py |
| src/load.py | src/etl/load.py |
| src/simulator.py | src/core/simulator.py |
| src/integration.py | src/core/integration.py |
| src/analytics.py | src/core/analytics.py |
| src/visualizations.py | src/visualization/charts.py |
| src/utils.py | src/utils/helpers.py |
| src/config.py | src/config/database.py |

### Updated Imports:

```python
# Before
from src.extract import extract_all
from src.transform import transform_all
from src.load import load_all

# Now
from src.etl import extract_all, transform_all, load_all
```

---

## Next Steps

1. Create unit tests in tests/
2. Add src/config/constants.py with business constants
3. Add src/utils/validators.py with validators
4. Document architecture in docs/architecture.md
5. Create setup scripts in scripts/

---

**Author**: ETL Team
**Date**: December 2025
**Version**: 2.0 (Refactored Structure)
