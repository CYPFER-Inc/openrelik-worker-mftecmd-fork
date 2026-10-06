import os

import redis
from celery.app import Celery

from src.celery_guard import GuardedTask, configure_delivery

REDIS_URL = os.getenv("REDIS_URL") or "redis://localhost:6379/0"
celery = Celery(broker=REDIS_URL, backend=REDIS_URL, include=["src.mftecmd"], task_cls=GuardedTask)
# KAN-1216: ack after the task and requeue on worker loss, capped against
# poison tasks. See src/celery_guard.py.
configure_delivery(celery)
redis_client = redis.Redis.from_url(REDIS_URL)
