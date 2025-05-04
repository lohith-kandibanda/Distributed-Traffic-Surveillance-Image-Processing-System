FROM python:3.10

WORKDIR /app

COPY . /app

# Fix the OpenCV ImportError by installing libgl1
RUN apt-get update && apt-get install -y libgl1

RUN pip install --upgrade pip
RUN pip install --no-cache-dir aio_pika redis opencv-python

CMD ["python", "master_worker.py"]
