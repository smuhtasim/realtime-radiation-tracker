import pandas as pd
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import pyarrow.parquet as pq
import pyarrow as pa
from kafka.admin import KafkaAdminClient

def wait_for_kafka(kafka_server, timeout=60, interval=5):
    """Wait for Kafka broker to be available."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=kafka_server)
            admin_client.list_topics()
            admin_client.close()
            print(f"Kafka broker at {kafka_server} is ready")
            return True
        except Exception as e:
            print(f"Waiting for Kafka: {e}")
            time.sleep(interval)
    return False

def send_radiation_data_to_kafka(parquet_file, kafka_topic, kafka_server, delay=0.0001):
    print("Producer starting...")

    # Hardcoded parameters
    chunksize = 100000
    required_columns = ["Device ID", "Captured Time", "Latitude", "Longitude", "Value", "Unit"]
    unit_filter = "cpm"

    # Wait for Kafka to be ready
    if not wait_for_kafka(kafka_server):
        raise RuntimeError("Kafka broker not available after timeout")

    # Kafka connection retry logic
    for attempt in range(5):
        try:
            print(f"Attempt {attempt + 1} to connect to Kafka at {kafka_server}")
            producer = KafkaProducer(
                bootstrap_servers=kafka_server,
                value_serializer=lambda m: json.dumps(m).encode('utf-8')
            )
            print("Connected to Kafka")
            break
        except Exception as e:
            print(f"Kafka connection failed: {e}")
            time.sleep(3)
    else:
        raise RuntimeError("Failed to connect to Kafka after retries")

    print(f"Reading Parquet file: {parquet_file}")
    total_rows = 0
    produced_rows = 0

    # Read Parquet in chunks using pyarrow
    try:
        parquet_file_obj = pq.ParquetFile(parquet_file)
        for batch in parquet_file_obj.iter_batches(batch_size=chunksize, columns=required_columns):
            try:
                # Convert to pandas DataFrame
                chunk = batch.to_pandas()
                
                # Parse and clean timestamps
                chunk['Captured Time'] = pd.to_datetime(chunk['Captured Time'], errors='coerce')
                chunk = chunk.dropna(subset=['Captured Time'])
                chunk['Captured Time'] = chunk['Captured Time'].dt.round('1s')
                
                # Apply filtering
                chunk = chunk[chunk['Unit'].str.lower() == unit_filter]
                chunk = chunk[chunk['Value'].notna() & (chunk['Value'] != 0)]
                
                # Sort by Captured Time
                chunk.sort_values('Captured Time', inplace=True)
                
                grouped = chunk.groupby('Captured Time')
                print(f"Processing chunk with {len(chunk)} rows, {len(grouped)} timestamp groups")
                
                for timestamp, group in grouped:
                    print(f"Timestamp: {timestamp} → {len(group)} record(s)")
                    for _, row in group.iterrows():
                        try:
                            message = {
                                'device_id': str(row['Device ID']),
                                'latitude': float(row['Latitude']),
                                'longitude': float(row['Longitude']),
                                'captured_time': timestamp.isoformat(),
                                'cpm': float(row['Value'])
                            }
                            producer.send(kafka_topic, message)
                            print(f"Sent: {message}")
                            produced_rows += 1
                        except Exception as e:
                            print(f"Error sending row: {e}")
                    
                    producer.flush()
                    print(f"Finished timestamp group {timestamp}.")
                    time.sleep(delay)
                
                total_rows += len(chunk)
            except Exception as e:
                print(f"Error processing chunk: {e}")
    except Exception as e:
        print(f"Error reading/parsing Parquet: {e}")
        return

    producer.flush()
    producer.close()
    print(f"All messages sent. Producer done at {datetime.now()}")
    print(f"Total rows processed: {total_rows:,}, Produced: {produced_rows:,}")

if __name__ == "__main__":
    PARQUET_FILE = "/data/measurements_all.parquet"
    KAFKA_TOPIC = "radiation_data"
    KAFKA_SERVER = "kafka:9092"
    DELAY = 0.0001
    send_radiation_data_to_kafka(PARQUET_FILE, KAFKA_TOPIC, KAFKA_SERVER, DELAY)