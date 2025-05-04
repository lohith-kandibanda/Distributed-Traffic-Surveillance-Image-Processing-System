# helmet_worker/helmet_worker.py

import aio_pika
import redis
import ast
import socket
import logging
import asyncio
import json
import cv2
import numpy as np
from io import BytesIO
from ultralytics import YOLO
from PIL import Image

# ---------------------------
# Configuration
# ---------------------------

RABBITMQ_URL = "amqp://guest:guest@rabbitmq/"
HELMET_QUEUE = "helmet_queue"
REDIS_HOST = "redis"
REDIS_PORT = 6379

CONFIDENCE_THRESHOLD = 0.5   # Helmet detection confidence

# ---------------------------
# Initialization
# ---------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

WORKER_NAME = socket.gethostname()

helmet_model = YOLO("helmet_detection.pt")  # 📌 Your trained helmet detection model

# ---------------------------
# Task Handler
# ---------------------------

async def handle_task(message: aio_pika.IncomingMessage):
    async with message.process(requeue=True):
        task = None
        try:
            task = ast.literal_eval(message.body.decode())
            parent_id = task['parent_id']
            frame_no = task['frame_no']

            logger.info(f"🛡️ Helmet Worker [{WORKER_NAME}] processing Frame {frame_no} for Task {parent_id}")

            frame_data = bytes.fromhex(task['frame_data'])
            frame = np.array(Image.open(BytesIO(frame_data)).convert('RGB'))

            result = process_helmet_frame(frame)

            # Save result back to Redis
            key = f"{parent_id}:helmet:{frame_no}"
            r.set(key, json.dumps(result))

            logger.info(f"✅ Helmet Worker [{WORKER_NAME}] saved result for Frame {frame_no}")

        except Exception as e:
            logger.error(f"❌ Helmet Worker [{WORKER_NAME}] failed processing Frame {task['frame_no'] if task else 'UNKNOWN'}: {e}")
            raise e

# ---------------------------
# Helmet Detection Function
# ---------------------------

def process_helmet_frame(frame):
    helmet_violations = []

    try:
        detections = helmet_model.predict(source=frame, conf=CONFIDENCE_THRESHOLD)[0]

        for det in detections.boxes:
            cls = int(det.cls.item())
            name = helmet_model.model.names[cls]
            x1, y1, x2, y2 = map(int, det.xyxy[0].tolist())

            # 🚨 Adjust class names based on your trained dataset
            if name.lower() in ["no_helmet", "without_helmet"]:
                helmet_violations.append({
                    "violation": "No Helmet",
                    "bbox": [x1, y1, x2, y2],
                    "confidence": round(det.conf.item(), 3)
                })

    except Exception as e:
        logger.error(f"❌ Helmet detection failed: {e}")

    return {
        "helmet_violations": helmet_violations,
        "violation_count": len(helmet_violations)
    }

# ---------------------------
# Worker Entry
# ---------------------------

async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.declare_queue(HELMET_QUEUE, durable=True)
    await channel.consume(handle_task)
    logger.info(f"🚀 Helmet Worker [{WORKER_NAME}] is waiting for helmet tasks...")
    return connection

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    conn = loop.run_until_complete(main())
    loop.run_forever()
