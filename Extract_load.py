import pandas as pd
import json
import numpy as np
from sqlalchemy import create_engine, types

# 1. THE FIX: Function to convert NumPy types to Python types
def serialize_complex_data(obj):
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, (np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.float64, np.float32)):
        return float(obj)
    if isinstance(obj, dict):
        return {k: serialize_complex_data(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [serialize_complex_data(i) for i in obj]
    return obj

# 2. Connection setup (using your WeatherDB)
user = 'user'
password = 'password'
local_host = 'localhost'
port = '5432'
db_name = 'DBNAME'

db_url = f'postgresql://{user}:{password}@{local_host}:{port}/{db_name}'
engine = create_engine(db_url)

# 3. Read and Transform
df = pd.read_parquet('parque001.parquet')

# Apply the sanitizer before converting to string
df['user_metadata'] = df['user_metadata'].apply(lambda x: json.dumps(serialize_complex_data(x)))
df['items'] = df['items'].apply(lambda x: json.dumps(serialize_complex_data(x)))

# 4. Load to Landing Schema
df.to_sql(
    'userdata', 
    engine, 
    schema='landing', 
    if_exists='append', 
    index=False,
    dtype={
        'user_metadata': types.JSON,
        'items': types.JSON
    }
)

print("Data successfully loaded into landing.userdata!")
