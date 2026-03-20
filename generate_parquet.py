import pandas as pd
import json

# Sample data with a nested JSON-like structure
data = [
    {
        "order_id": 101,
        "customer_name": "Alice Smith",
        "user_metadata": {"age": 28, "city": "Mumbai", "segment": "Premium"},
        "items": ["Laptop", "Mouse"]
    },
    {
        "order_id": 102,
        "customer_name": "Bob Singh",
        "user_metadata": {"age": 35, "city": "Delhi", "segment": "Standard"},
        "items": ["Monitor"]
    }
]

# Create DataFrame
df = pd.DataFrame(data)

# IMPORTANT: In Parquet, nested objects are often stored as dictionaries/lists.
# To make it compatible with Postgres's JSONB, we can keep them as dicts.
df.to_parquet('parque001'
'.parquet', index=False)
print("Successfully created 'parque001.parquet' with nested structures.")