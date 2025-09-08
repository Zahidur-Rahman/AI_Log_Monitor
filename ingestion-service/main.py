import os
import json
import socket
import random
import logging
import time
import asyncio # <-- Import asyncio
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from faker import Faker

# --- Configuration and Setup (No changes here) ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "logs")
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
def wait_for_kafka(timeout=180, retry_interval=5):
    """Wait for Kafka to be ready."""
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


app = FastAPI(title="Log Ingestion Service")
faker = Faker()

# --- Pydantic Models and delivery_report (No changes here) ---
class LogMessage(BaseModel):
    service: str = Field(..., min_length=1, max_length=50)
    level: str = Field(..., pattern="^(DEBUG|INFO|WARNING|ERROR|CRITICAL)$")
    message: str = Field(..., min_length=1)
    timestamp: Optional[datetime] = None

class FakeLogRequest(BaseModel):
    count: int = Field(..., ge=1, le=10000, description="Number of fake logs to generate")

def delivery_report(err, msg):
    if err:
        logger.error(f"❌ Failed: {err}")
    # Hiding the success message for the scheduler to keep logs clean
    # else:
    #     logger.info(f"✅ Sent to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")


# ## NEW SECTION: Continuous Log Producer ##
# =================================================

async def continuous_log_producer():
    """A background task that produces a batch of logs every 5 minutes."""
    logger.info("🚀 Starting continuous log producer...")
    levels = ["INFO", "WARNING", "ERROR", "DEBUG", "CRITICAL"]
    services = ["auth", "payment", "orders", "inventory", "frontend"]
    
    while True:
        try:
            log_count = 1000
            logger.info(f"Scheduler: Generating {log_count} fake logs...")
            
            for _ in range(log_count):
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
            
            producer.flush()
            logger.info(f"Scheduler: ✅ Successfully produced {log_count} logs. Waiting for 5 minutes...")
            
            # Wait for 5 minutes (300 seconds) before the next run
            await asyncio.sleep(300)

        except Exception as e:
            logger.error(f"Scheduler Error: {e}. Retrying in 60 seconds...")
            await asyncio.sleep(60)

@app.on_event("startup")
async def startup_event():
    """On application startup, create the background task."""
    logger.info("Application startup: Initializing background tasks.")
    asyncio.create_task(continuous_log_producer())

# =================================================
# ## Existing API Endpoints (No changes here) ##

@app.get("/")
def root():
    return {"status": "Log Service Running", "kafka_servers": KAFKA_BOOTSTRAP_SERVERS}

@app.post("/logs")
def send_log(log: LogMessage):
    # ... (code is the same)
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
    # ... (code is the same)
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


@app.post("/fake-logs", summary="Generate a specific number of fake logs on demand")
def generate_fake_logs(req: FakeLogRequest):
    # ... (code is the same, this endpoint is still useful for manual testing)
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
    # ... (code is the same)
    try:
        admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
        admin_client.list_topics(timeout=2)
        return {"status": "healthy", "kafka": "connected", "servers": KAFKA_BOOTSTRAP_SERVERS}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "kafka": "disconnected", "error": str(e)}