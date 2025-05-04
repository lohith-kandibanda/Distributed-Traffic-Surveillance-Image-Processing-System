Distributed Traffic Surveillance Image Processing System with Fault Tolerance and API Authentication is a scalable and secure platform designed to process real-time traffic images in a distributed environment. It uses a master-worker architecture to ensure fault tolerance and balanced task execution across multiple processing units.

The system incorporates:

YOLOv11m for object detection (vehicles and license plates).

YOLOv9n for helmet violation detection.

EasyOCR for license plate text recognition.

RabbitMQ for asynchronous and fault-tolerant task queueing.

Redis for result caching, rate limiting, and secure API key-based authentication.

Dockerized microservices for modular and scalable deployment.

Designed for use in smart city surveillance and law enforcement APIs, the system ensures high availability, real-time processing, and secure API access—even under load or partial service failures. It also supports a live dashboard for monitoring task status and traffic logs, with future plans for enhanced violation detection and analytics.

