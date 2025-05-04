FROM python:3.10

WORKDIR /app

COPY . /app

RUN apt-get update && apt-get install -y libgl1-mesa-glx

RUN pip install --upgrade pip
RUN pip install --no-cache-dir torch torchvision opencv-python paddleocr aio_pika redis ultralytics

CMD ["python", "helmet_worker.py"]
