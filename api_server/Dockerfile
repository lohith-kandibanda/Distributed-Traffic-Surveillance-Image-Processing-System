FROM python:3.10

WORKDIR /app
COPY . /app

RUN pip install --upgrade pip
RUN pip install fastapi uvicorn redis aio_pika python-multipart pillow

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
