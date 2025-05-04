# plate_worker/plate_worker.py

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
from paddleocr import PaddleOCR
from PIL import Image

# ---------------------------
# Configuration
# ---------------------------

RABBITMQ_URL = "amqp://guest:guest@rabbitmq/"
PLATE_QUEUE = "plate_queue"
REDIS_HOST = "redis"
REDIS_PORT = 6379
CONFIDENCE_THRESHOLD = 0.6  # Confidence filter for OCR

# ---------------------------
# Initialization
# ---------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

WORKER_NAME = socket.gethostname()

ocr_model = PaddleOCR(use_angle_cls=True, lang='en')

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

            logger.info(f"🔍 Plate Worker [{WORKER_NAME}] processing Frame {frame_no} for Task {parent_id}")

            frame_data = bytes.fromhex(task['frame_data'])
            frame = np.array(Image.open(BytesIO(frame_data)).convert('RGB'))

            result = process_plate_frame(frame)

            # Save result back to Redis
            key = f"{parent_id}:plate:{frame_no}"
            r.set(key, json.dumps(result))

            logger.info(f"✅ Plate Worker [{WORKER_NAME}] saved result for Frame {frame_no}")

        except Exception as e:
            logger.error(f"❌ Plate Worker [{WORKER_NAME}] failed processing Frame {task['frame_no'] if task else 'UNKNOWN'}: {e}")
            raise e

# ---------------------------
# Plate Detection Function
# ---------------------------

def process_plate_frame(frame):
    plates_detected = []

    # PaddleOCR expects BGR input
    frame_bgr = cv2.cvtColor(frame, cv2.COLOR_RGB2BGR)

    try:
        ocr_results = ocr_model.ocr(frame_bgr, cls=True)

        for line in ocr_results[0]:
            bbox, (text, confidence) = line
            if confidence >= CONFIDENCE_THRESHOLD:
                x1, y1 = map(int, bbox[0])
                x2, y2 = map(int, bbox[2])
                plates_detected.append({
                    "plate_text": text.strip(),
                    "confidence": round(confidence, 3),
                    "bbox": [x1, y1, x2, y2]
                })

    except Exception as e:
        logger.error(f"❌ OCR failed on frame: {e}")

    return {
        "plates": plates_detected,
        "plate_count": len(plates_detected)
    }

# ---------------------------
# Worker Entry
# ---------------------------

async def main():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    await channel.declare_queue(PLATE_QUEUE, durable=True)
    await channel.consume(handle_task)
    logger.info(f"🚀 Plate Worker [{WORKER_NAME}] is waiting for plate tasks...")
    return connection

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    conn = loop.run_until_complete(main())
    loop.run_forever()
