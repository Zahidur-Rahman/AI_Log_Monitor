import os
import json
import logging
import asyncio
import time
import chromadb
from sentence_transformers import SentenceTransformer
from datetime import datetime, timezone

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient
from pgvector.sqlalchemy import Vector
from sqlalchemy import create_engine, text, Column, Integer, String, TIMESTAMP, JSON
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.exc import OperationalError

# --- Configuration ---
# Environment variables with proper defaults for container networking
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_USER = os.getenv("POSTGRES_USER", "loguser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "logpass")
POSTGRES_DB = os.getenv("POSTGRES_DB", "logsdb")
CHROMA_HOST = os.getenv("CHROMA_HOST", "chroma")

# Batching configuration
BATCH_SIZE = 100
BATCH_TIMEOUT = 1.0

# --- Standard Setup ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Database and Model Initialization ---
# SQLAlchemy
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
    embedding = Column(Vector(384))

# Embedding Model
logger.info("Loading sentence transformer model...")
embedding_model = SentenceTransformer('all-MiniLM-L6-v2', device='cpu')

# ChromaDB Client
logger.info(f"Connecting to ChromaDB at host: {CHROMA_HOST}")
chroma_client = chromadb.HttpClient(host=CHROMA_HOST, port=8000)
chroma_collection = chroma_client.get_or_create_collection("logs")
logger.info("✅ ChromaDB connection successful.")

# --- Waiter and Setup Functions ---
def wait_for_postgres(timeout=60, retry_interval=5):
    """Wait for PostgreSQL to be ready."""
    # (This function is correct, no changes needed)
    logger.info("Checking if PostgreSQL is ready...")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            logger.info("PostgreSQL is ready!")
            return True
        except OperationalError:
            logger.warning(f"PostgreSQL not ready, retrying in {retry_interval}s...")
            time.sleep(retry_interval)
    raise Exception("PostgreSQL did not become ready in time.")

def wait_for_kafka(timeout=180, retry_interval=5):
    """Wait for Kafka to be ready."""
    # (This function is correct, no changes needed)
    logger.info(f"Checking if Kafka is ready at {KAFKA_BOOTSTRAP_SERVERS}...")
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            admin_client.list_topics(timeout=5)
            logger.info("Kafka is ready!")
            return True
        except Exception:
            logger.warning(f"Kafka not ready, retrying in {retry_interval}s...")
            time.sleep(retry_interval)
    raise Exception("Kafka did not become ready in time.")

def setup_database():
    """Create the database table if it doesn't exist."""
    try:
        wait_for_postgres()
        with engine.connect() as connection:
            connection.execute(text('CREATE EXTENSION IF NOT EXISTS vector'))
            connection.commit()
        Base.metadata.create_all(engine)
        logger.info("Database setup complete. 'pgvector' enabled and 'logs' table ready.")
    except Exception as e:
        logger.error(f"❌ Failed to setup database: {e}")
        raise

# --- Core Batch Processing Logic ---
async def consume_logs_in_batches():
    """Consumes, embeds, and stores logs in batches."""
    wait_for_kafka()
    
    consumer_conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'log-processing-group-batch',
        'auto.offset.reset': 'earliest',
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])
    logger.info(f"Consumer started. Waiting for batches of up to {BATCH_SIZE} logs...")

    while True:
        try:
            # 1. Consume a BATCH of messages from Kafka.
            messages = consumer.consume(num_messages=BATCH_SIZE, timeout=BATCH_TIMEOUT)

            if not messages:
                await asyncio.sleep(1)
                continue

            logger.info(f"Processing a batch of {len(messages)} messages.")

            log_entries = []
            log_texts_for_embedding = []
            chroma_metadatas = []

            # 2. Prepare the data from the batch.
            for msg in messages:
                if msg.error():
                    logger.error(f"❌ Consumer error: {msg.error()}")
                    continue
                
                data = json.loads(msg.value().decode('utf-8'))
                
                # Prepare text for embedding model
                log_texts_for_embedding.append(data.get('message', ''))
                
                # Prepare metadata for ChromaDB
                chroma_metadatas.append({
                    "service": data.get('service', 'unknown'),
                    "level": data.get('level', 'INFO')
                })

                # Prepare LogEntry object for PostgreSQL
                try:
                    ts_str = data.get('timestamp')
                    if ts_str and isinstance(ts_str, str):
                        timestamp = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                    else:
                        timestamp = datetime.now(timezone.utc)
                except (ValueError, TypeError, AttributeError):
                    timestamp = datetime.now(timezone.utc)

                log_entries.append(LogEntry(
                    service=data.get('service', 'unknown'),
                    level=data.get('level', 'INFO'),
                    message=data.get('message', ''),
                    timestamp=timestamp,
                    raw_data=data
                ))

            # 3. Create embeddings for the entire batch at once.
            embeddings = embedding_model.encode(log_texts_for_embedding).tolist()

            # 4. Save the entire batch to the databases.
            with Session(engine) as session:
                # Assign the generated embeddings to their corresponding LogEntry objects
                for i, entry in enumerate(log_entries):
                    entry.embedding = embeddings[i]
                
                # Save all log entries to PostgreSQL in a single transaction
                session.add_all(log_entries)
                session.commit()
                logger.info(f"✅ Saved batch of {len(log_entries)} logs to PostgreSQL.")

                # Get the auto-generated IDs from PostgreSQL for ChromaDB
                postgres_ids = [str(entry.id) for entry in log_entries]
                
                # Add the entire batch to ChromaDB in a single operation
                if postgres_ids:
                    chroma_collection.add(
                        ids=postgres_ids,
                        embeddings=embeddings,
                        documents=log_texts_for_embedding,
                        metadatas=chroma_metadatas
                    )
                    logger.info(f"✅ Added batch of {len(postgres_ids)} embeddings to ChromaDB.")

        except Exception as e:
            logger.error(f"❌ An error occurred during batch processing: {e}")
            await asyncio.sleep(5)

    consumer.close()

if __name__ == "__main__":
    setup_database()
    asyncio.run(consume_logs_in_batches())