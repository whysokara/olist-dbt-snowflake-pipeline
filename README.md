# Olist dbt + Snowflake Pipeline

End-to-end ELT pipeline built on the Brazilian e-commerce Olist dataset using dbt Core and Snowflake.

## Architecture
```
Kaggle CSVs (local)
      ↓
Snowflake Internal Stage (PUT command)
      ↓
Raw Tables (COPY INTO)
      ↓
dbt Core
  ├── staging/        → clean, renamed, typed
  ├── intermediate/   → joins, business logic
  └── marts/          → final analytics tables
      ↓
Dagster (orchestration) [coming soon]
```

## Tech Stack

| Layer | Tool |
|---|---|
| Data Warehouse | Snowflake (AWS ap-southeast-1) |
| Transformation | dbt Core 1.11 |
| Orchestration | Dagster (coming soon) |
| Source Data | Olist E-Commerce (Kaggle) |
| Version Control | GitHub |

## Project Structure
```
olist_pipeline/
├── models/
│   ├── staging/          # 9 source-aligned views
│   ├── intermediate/     # 3 business logic models
│   └── marts/            # dimensional models (coming soon)
├── macros/
│   └── generate_schema_name.sql
├── tests/
└── snapshots/
```

## Data Model

### Raw Layer (OLIST_DB.RAW)
- `customers` — 99,441 rows
- `orders` — 99,441 rows
- `order_items` — 112,650 rows
- `order_payments` — 103,886 rows
- `order_reviews` — 99,224 rows
- `products` — 32,951 rows
- `sellers` — 3,095 rows
- `geolocation` — 1,000,163 rows
- `product_category_name_translation` — 71 rows

### Staging Layer (OLIST_DB.STAGING)
One view per source table with cleaned column names and types.

### Intermediate Layer (OLIST_DB.INTERMEDIATE)
- `int_orders_with_customers` — orders enriched with customer info + delivery metrics
- `int_order_items_with_products` — order items enriched with product, category, seller
- `int_order_payments_pivoted` — one row per order with payment breakdown by type

### Marts Layer (OLIST_DB.MARTS)
Coming soon:
- `fct_orders` — order-level facts
- `dim_customers` — customer dimension
- `dim_products` — product dimension
- `dim_sellers` — seller dimension

## Testing

58 tests passing across staging and intermediate layers covering:
- Primary key uniqueness
- Not null constraints
- Accepted values

## Setup

### Prerequisites
- Snowflake account
- dbt Core 1.11+
- Python 3.10+

### Quick Start
```bash
# Clone repo
git clone https://github.com/whysokara/olist-dbt-snowflake-pipeline.git
cd olist-dbt-snowflake-pipeline

# Configure Snowflake connection
vi ~/.dbt/profiles.yml

# Validate connection
dbt debug

# Run all models
dbt run

# Run tests
dbt test
```

## Author

Kara — [LinkedIn](https://linkedin.com/in/himanshukara) · [GitHub](https://github.com/whysokara)