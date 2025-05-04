# api_server/main.py

from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
import redis
import aio_pika
import uuid
import logging
from datetime import datetime

# ---------------------------
# Configuration
# ---------------------------

API_KEYS = {"traffic123"}  # Simple API key check
REDIS_HOST = "redis"       # Redis hostname inside Docker
REDIS_PORT = 6379
RABBITMQ_URL = "amqp://guest:guest@rabbitmq/"  # RabbitMQ hostname inside Docker
QUEUE_NAME = "main_queue"  # Master Worker listens to this
RATE_LIMIT = 10  # 10 uploads per minute per API key

# ---------------------------
# Initialization
# ---------------------------

app = FastAPI(title="🚦 Distributed Traffic Surveillance API")
api_key_header = APIKeyHeader(name="X-API-Key")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------
# Authentication
# ---------------------------

def authenticate(api_key: str = Depends(api_key_header)):
    if api_key not in API_KEYS:
        logger.warning(f"Invalid API key used: {api_key}")
        raise HTTPException(status_code=403, detail="Unauthorized")
    return api_key

# ---------------------------
# Rate Limiting
# ---------------------------

def rate_limit(api_key: str):
    key = f"ratelimit:{api_key}"
    current = r.incr(key)
    if current == 1:
        r.expire(key, 60)
    elif current > RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

# ---------------------------
# Upload Endpoint
# ---------------------------

@app.post("/upload/")
async def upload_video(file: UploadFile = File(...), api_key: str = Depends(authenticate)):
    rate_limit(api_key)

    # Validate file extension
    if not file.filename.lower().endswith((".mp4", ".avi", ".mov", ".mkv")):
        raise HTTPException(status_code=400, detail="Only video files are accepted.")

    contents = await file.read()
    task_id = str(uuid.uuid4())

    task = {
        "id": task_id,
        "filename": file.filename,
        "timestamp": datetime.utcnow().isoformat(),
        "source": api_key,
        "type": "video",
        "data": contents.hex()
    }

    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel = await connection.channel()
        await channel.declare_queue(QUEUE_NAME, durable=True)

        await channel.default_exchange.publish(
            aio_pika.Message(body=str(task).encode()),
            routing_key=QUEUE_NAME
        )
        await connection.close()

        r.set(f"task:{task_id}:status", "queued")
        logger.info(f"Task {task_id} queued successfully by {api_key}")

        return {"message": "Video uploaded successfully!", "task_id": task_id}

    except Exception as e:
        logger.error(f"Error while queuing task: {e}")
        raise HTTPException(status_code=500, detail="Internal server error while queuing")

# ---------------------------
# Result Fetch Endpoint
# ---------------------------

@app.get("/result/{task_id}")
def get_result(task_id: str):
    result = r.get(f"task:{task_id}:result")
    if result:
        return {
            "status": "done",
            "task_id": task_id,
            "result": result
        }

    status = r.get(f"task:{task_id}:status")
    if status:
        return {
            "status": status,
            "task_id": task_id
        }

    return {"status": "not_found", "task_id": task_id}

# ---------------------------
# Health Check Endpoint
# ---------------------------

@app.get("/")
def health_check():
    return {"message": "🚦 Traffic Surveillance API is running properly!"}
