from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common import Configuration, WatermarkStrategy, Duration
from pyflink.common.typeinfo import Types
from kafka.admin import KafkaAdminClient, NewTopic
import json
from datetime import datetime
import sys
import signal
import time
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def signal_handler(sig, frame):
    logger.info('Shutting down gracefully...')
    sys.exit(0)

def create_kafka_topics():
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers='kafka:9092',
            client_id='flink-topic-creator'
        )

        topic_list = [
            NewTopic(name="radiation_safe", num_partitions=1, replication_factor=1),
            NewTopic(name="radiation_elevated", num_partitions=1, replication_factor=1),
            NewTopic(name="radiation_dangerous", num_partitions=1, replication_factor=1),
        ]

        existing_topics = admin_client.list_topics()
        to_create = [t for t in topic_list if t.name not in existing_topics]

        if to_create:
            admin_client.create_topics(new_topics=to_create, validate_only=False)
            print("Created Kafka topics:", [t.name for t in to_create])
        else:
            print("Kafka topics already exist.")

        admin_client.close()

    except Exception as e:
        print(f"Error creating Kafka topics: {e}")

# --- Filtering / Classification logic as reusable functions ---

def classify_risk_level(data):
    """
    Accepts dict with radiation data, returns dict updated with 'risk_level' and 'processed_at'.
    """
    try:
        cpm = float(data.get("cpm", 0))
        if cpm <= 50:
            risk_level = "safe"
        elif cpm <= 100:
            risk_level = "elevated"
        else:
            risk_level = "dangerous"
        data["risk_level"] = risk_level
        data["processed_at"] = datetime.now().isoformat()
        return data
    except Exception as e:
        logger.error(f"Error in classify_risk_level: {e}")
        return None

def filter_safe(json_str):
    try:
        data = json.loads(json_str)
        classified = classify_risk_level(data)
        if classified and classified.get("risk_level") == "safe":
            return json.dumps(classified)
        else:
            return None
    except Exception as e:
        logger.error(f"Error in filter_safe: {e}")
        return None

def filter_elevated(json_str):
    try:
        data = json.loads(json_str)
        classified = classify_risk_level(data)
        if classified and classified.get("risk_level") == "elevated":
            return json.dumps(classified)
        else:
            return None
    except Exception as e:
        logger.error(f"Error in filter_elevated: {e}")
        return None

def filter_dangerous(json_str):
    try:
        data = json.loads(json_str)
        classified = classify_risk_level(data)
        if classified and classified.get("risk_level") == "dangerous":
            return json.dumps(classified)
        else:
            return None
    except Exception as e:
        logger.error(f"Error in filter_dangerous: {e}")
        return None

# --- Flink job ---

def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Create Kafka output topics
    create_kafka_topics()

    try:
        config = Configuration()
        config.set_string("pipeline.jars",
                          "file:///opt/flink/usrlib/flink-connector-kafka-1.17.0.jar;file:///opt/flink/usrlib/kafka-clients-3.4.0.jar")
        config.set_string("akka.ask.timeout", "60s")
        config.set_string("heartbeat.timeout", "50000")
        config.set_string("heartbeat.interval", "5000")
        config.set_string("restart-strategy", "fixed-delay")
        config.set_string("restart-strategy.fixed-delay.attempts", "3")
        config.set_string("restart-strategy.fixed-delay.delay", "5s")

        env = StreamExecutionEnvironment.get_execution_environment(configuration=config)
        env.set_parallelism(1)
        env.enable_checkpointing(10000)

        kafka_props = {
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'pyflink-consumer',
            'auto.offset.reset': 'latest',
            'session.timeout.ms': '30000',
            'heartbeat.interval.ms': '3000',
            'request.timeout.ms': '40000'
        }

        consumer = FlinkKafkaConsumer(
            topics='radiation_data',
            deserialization_schema=SimpleStringSchema(),
            properties=kafka_props
        )

        watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(10))
        stream = env.add_source(consumer).assign_timestamps_and_watermarks(watermark_strategy)

        producer_props = {
            'bootstrap.servers': 'kafka:9092',
            'acks': 'all',
            'retries': '3',
            'request.timeout.ms': '30000'
        }

        # Kafka producers for risk level outputs
        safe_producer = FlinkKafkaProducer(
            topic='radiation_safe',
            serialization_schema=SimpleStringSchema(),
            producer_config=producer_props
        )
        elevated_producer = FlinkKafkaProducer(
            topic='radiation_elevated',
            serialization_schema=SimpleStringSchema(),
            producer_config=producer_props
        )
        dangerous_producer = FlinkKafkaProducer(
            topic='radiation_dangerous',
            serialization_schema=SimpleStringSchema(),
            producer_config=producer_props
        )

        def label_and_log(msg):
            """
            Helper to classify, add risk level & processed timestamp, and log to console.
            Returns JSON string or None.
            """
            try:
                if not msg or (isinstance(msg, str) and msg.strip() == ''):
                    return None

                if isinstance(msg, bytes):
                    msg = msg.decode('utf-8')

                data = json.loads(msg)
                classified = classify_risk_level(data)
                if not classified:
                    return None

                risk_indicator = classified["risk_level"].upper()

                # Console log
                timestamp = datetime.now().strftime("%H:%M:%S")
                device_id = classified.get('device_id', 'N/A')
                latitude = classified.get('latitude', 'N/A')
                longitude = classified.get('longitude', 'N/A')
                captured_time = classified.get('captured_time', 'N/A')
                location_str = f"{latitude},{longitude}"
                cpm = classified.get('cpm', 'N/A')

                print(f"[{timestamp}] PROCESSED: Device {device_id} | CPM: {cpm} | Risk: {risk_indicator} | Location: {location_str} | Time: {captured_time}")

                return json.dumps(classified)

            except Exception as e:
                logger.error(f"Error in label_and_log: {e}")
                return None

        # Build processing pipeline
        labeled_stream = stream \
            .map(label_and_log, output_type=Types.STRING()) \
            .filter(lambda x: x is not None)

        # Split stream by risk level using our filtering functions:
        safe_stream = labeled_stream.filter(lambda x: filter_safe(x) is not None)
        elevated_stream = labeled_stream.filter(lambda x: filter_elevated(x) is not None)
        dangerous_stream = labeled_stream.filter(lambda x: filter_dangerous(x) is not None)

        # Send to separate topics
        safe_stream.add_sink(safe_producer)
        elevated_stream.add_sink(elevated_producer)
        dangerous_stream.add_sink(dangerous_producer)

        print("Starting Radiation Risk Tier Routing Job...")
        print("Listening for data from 'radiation_data'")
        print("Routing to: radiation_safe, radiation_elevated, radiation_dangerous")
        print("=" * 70)

        env.execute("Radiation Risk Tier Routing Job")

    except KeyboardInterrupt:
        logger.info("Job interrupted by user")
    except Exception as e:
        logger.error(f"Job failed: {e}")
        import traceback
        traceback.print_exc()
        time.sleep(2)
        sys.exit(1)

if __name__ == '__main__':
    main()