import os
import json
import socket
import random
import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from faker import Faker

# Read environment variables with proper defaults for container networking
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs")

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"KAFKA_BOOTSTRAP_SERVERS: {KAFKA_BOOTSTRAP_SERVERS}")

# Kafka wait mechanism
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

# Kafka producer
try:
    wait_for_kafka(timeout=180, retry_interval=5)
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": socket.gethostname(),
    })
    logger.info(f"Kafka producer initialized successfully with servers: {KAFKA_BOOTSTRAP_SERVERS}")
except Exception as e:
    logger.error(f"❌ Failed to initialize Kafka producer: {e}")
    raise

# FastAPI app and faker instance
app = FastAPI(title="Log Ingestion Service")
faker = Faker()

# Pydantic Model
class LogMessage(BaseModel):
    service: str = Field(..., min_length=1, max_length=50)
    level: str = Field(..., pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    message: str = Field(..., min_length=1)
    timestamp: Optional[datetime] = None

class FakeLogRequest(BaseModel):
    count: int = Field(..., ge=1, le=10000, description="Number of fake logs to generate")

# Delivery callback
def delivery_report(err, msg):
    if err:
        logger.error(f"❌ Failed: {err}")
    else:
        logger.info(f"✅ Sent to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")

@app.get("/")
def root():
    return {"status": "Log Service Running", "kafka_servers": KAFKA_BOOTSTRAP_SERVERS}

@app.post("/logs")
def send_log(log: LogMessage):
    """Send single log to Kafka"""
    try:
        if not log.timestamp:
            log.timestamp = datetime.now(timezone.utc)

        payload = log.model_dump()
        producer.produce(
            topic=KAFKA_TOPIC,
            value=json.dumps(payload, default=str).encode("utf-8"),
            key=log.service.encode("utf-8"),
            callback=delivery_report
        )
        producer.poll(0)
        return {"status": "sent", "data": payload}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/logs/batch")
def send_batch_logs(logs: List[LogMessage]):
    """Send multiple logs"""
    results = []
    for log in logs:
        if not log.timestamp:
            log.timestamp = datetime.now(timezone.utc)

        payload = log.model_dump()
        producer.produce(
            topic=KAFKA_TOPIC,
            value=json.dumps(payload, default=str).encode("utf-8"),
            key=log.service.encode("utf-8"),
            callback=delivery_report
        )
        results.append(payload)

    producer.flush(5)
    return {"status": f"sent {len(logs)} logs", "data": results}

@app.post("/fake-logs")
def generate_fake_logs(req: FakeLogRequest):
    """Auto-generate N fake logs and send to Kafka"""
    levels = ["INFO", "WARNING", "ERROR", "DEBUG", "CRITICAL"]
    services = ["auth", "payment", "orders", "inventory"]

    sent_logs = []
    for _ in range(req.count):
        log_data = {
            "service": random.choice(services),
            "level": random.choice(levels),
            "message": faker.sentence(),
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        producer.produce(
            topic=KAFKA_TOPIC,
            value=json.dumps(log_data).encode("utf-8"),
            key=log_data["service"].encode("utf-8"),
            callback=delivery_report
        )
        sent_logs.append(log_data)

    producer.flush(10)
    return {"status": f"sent {req.count} fake logs", "logs_preview": sent_logs[:5]}

@app.get("/health")
def health_check():
    """Simple health check"""
    try:
        admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        admin_client.list_topics(timeout=2)
        return {"status": "healthy", "kafka": "connected", "servers": KAFKA_BOOTSTRAP_SERVERS}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "kafka": "disconnected", "error": str(e)}