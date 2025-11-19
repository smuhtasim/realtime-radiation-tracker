import pandas as pd
import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

# Hardcoded parameters
input_file = 'D:/projects/measurements-out.csv'
output_file = './dataset/measurements_all.parquet'
required_columns = ["Device ID", "Captured Time", "Latitude", "Longitude", "Value", "Unit"]
chunksize = 100000

# Create output directory
os.makedirs(os.path.dirname(output_file), exist_ok=True)

# Define schema to ensure consistency
schema = pa.schema([
    ('Device ID', pa.float64()),
    ('Captured Time', pa.string()),
    ('Latitude', pa.float64()),
    ('Longitude', pa.float64()),
    ('Value', pa.float64()),
    ('Unit', pa.string())
])

# Initialize parquet writer
parquet_writer = pq.ParquetWriter(output_file, schema, compression='snappy')
total_rows_processed = 0

print(f"Starting to process {input_file} at {datetime.now()}...")


for chunk_num, chunk in enumerate(pd.read_csv(input_file, usecols=required_columns, chunksize=chunksize)):
    try:
        # Ensure consistent data types
        chunk = chunk.copy()
        chunk['Value'] = chunk['Value'].astype('float64', errors='ignore')
        chunk['Device ID'] = chunk['Device ID'].astype('float64', errors='ignore')
        chunk['Latitude'] = chunk['Latitude'].astype('float64', errors='ignore')
        chunk['Longitude'] = chunk['Longitude'].astype('float64', errors='ignore')
        chunk['Captured Time'] = chunk['Captured Time'].astype(str)
        chunk['Unit'] = chunk['Unit'].astype(str)
        chunk = chunk.reset_index(drop=True)
        
        # Convert to PyArrow table
        table = pa.Table.from_pandas(chunk, schema=schema)
        parquet_writer.write_table(table)
        total_rows_processed += len(chunk)
        
        # Progress update
        if chunk_num % 100 == 0:
            print(f"Processed {total_rows_processed:,} rows")
    except Exception as e:
        print(f"Error processing chunk {chunk_num}: {e}")

parquet_writer.close()
print(f"Processing complete at {datetime.now()}!")
print(f"Total rows processed: {total_rows_processed:,}")
print(f"Saved to '{output_file}'")