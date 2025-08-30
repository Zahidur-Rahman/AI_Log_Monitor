import os
import json
import logging
import asyncio
import time
from datetime import datetime, timezone
from typing import Dict, Any

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
from sqlalchemy import create_engine, text, Column, Integer, String, TIMESTAMP, JSON
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.exc import OperationalError

# Environment variables with proper defaults for container networking
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "loguser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "logpass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "logsdb")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# SQLAlchemy setup
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
Base = declarative_base()

class LogEntry(Base):
    __tablename__ = "logs"
    id = Column(Integer, primary_key=True)
    service = Column(String(50), nullable=False)
    level = Column(String(20), nullable=False)
    message = Column(String, nullable=False)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)
    raw_data = Column(JSON)

def wait_for_postgres(timeout=60, retry_interval=5):
    """Wait for PostgreSQL to be ready."""
    logger.info("Checking if PostgreSQL is ready...")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("PostgreSQL is ready!")
            return True
        except OperationalError as e:
            logger.warning(f"PostgreSQL is not ready yet: {e}. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
    
    logger.error(f"PostgreSQL was not ready after {timeout} seconds.")
    raise Exception("Failed to connect to PostgreSQL within timeout period.")

def wait_for_kafka(timeout=180, retry_interval=5):
    """Wait for Kafka to be ready by checking if we can list topics."""
    logger.info(f"Checking if Kafka is ready at {KAFKA_BOOTSTRAP_SERVERS}...")
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        try:
            admin_client.list_topics(timeout=5)
            logger.info("Kafka is ready!")
            return True
        except Exception as e:
            logger.warning(f"Kafka is not ready yet: {e}. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
    
    logger.error(f"Kafka was not ready after {timeout} seconds.")
    raise Exception("Failed to connect to Kafka within timeout period.")

def setup_database():
    """Create the database table if it doesn't exist."""
    try:
        wait_for_postgres(timeout=60, retry_interval=5)
        Base.metadata.create_all(engine)
        logger.info("Database table 'logs' created or already exists.")
    except Exception as e:
        logger.error(f"❌ Failed to connect to database or create table: {e}")
        raise

async def consume_logs():
    """Main function to consume messages from Kafka and save them to the database."""
    logger.info("Starting Kafka consumer...")
    
    wait_for_kafka(timeout=180, retry_interval=5)
    
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'log-processing-group',
        'auto.offset.reset': 'earliest',
    }

    try:
        consumer = Consumer(consumer_conf)
        consumer.subscribe([KAFKA_TOPIC])
        logger.info(f"Subscribed to topic '{KAFKA_TOPIC}' using bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    except KafkaException as e:
        logger.error(f"❌ Failed to create consumer: {e}")
        raise

    while True:
        try:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"❌ Consumer error: {msg.error()}")
                continue
            
            message_data = json.loads(msg.value().decode('utf-8'))
            logger.info(f"✅ Consumed message from Kafka: {message_data['message'][:50]}...")
            
            # Validate timestamp
            try:
                timestamp_str = message_data.get('timestamp')
                if timestamp_str:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                else:
                    timestamp = datetime.now(timezone.utc)
            except ValueError as e:
                logger.warning(f"Invalid timestamp format in message: {timestamp_str}. Using current time. Error: {e}")
                timestamp = datetime.now(timezone.utc)

            with Session(engine) as session:
                log_entry = LogEntry(
                    service=message_data.get('service', 'unknown'),
                    level=message_data.get('level', 'INFO'),
                    message=message_data.get('message', 'No message provided'),
                    timestamp=timestamp,
                    raw_data=message_data
                )
                session.add(log_entry)
                session.commit()
                logger.info(f"✅ Saved log to database: {message_data['message'][:50]}...")

        except Exception as e:
            logger.error(f"❌ An error occurred during processing: {e}")
            await asyncio.sleep(5)
            
    consumer.close()

if __name__ == "__main__":
    setup_database()
    asyncio.run(consume_logs())