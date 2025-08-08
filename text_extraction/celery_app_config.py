# celery_app_config.py
from celery import Celery

import os
from dotenv import load_dotenv
load_dotenv()


BROKER_URL= os.getenv("BROKER_URL")

BACKEND_URL= os.getenv("BACKEND_URL")

app = Celery(
    'rag_ingestion_app',
    broker=BROKER_URL,
    backend=BACKEND_URL,
    # MODIFIED: Add the new module to the include list
    include=['text_extraction.tasks', 'text_extraction.docvlm_task', 'text_extraction.idp_app.tasks'],
    task_default_queue='text_extraction_queue',
    task_queues={
        'text_extraction_queue': {
            'exchange': 'text_extraction',
            'routing_key': 'text_extraction',
        },
    },
    task_routes={
        'tasks.*': {'queue': 'text_extraction_queue'},
        'text_extraction.*': {'queue': 'text_extraction_queue'},
    }
)

app.conf.beat_schedule = {
    'discover-users-and-dispatch-every-2-minutes': {
        # This task from your existing workflow remains unchanged
        'task': 'tasks.discover_users_and_dispatch_task',
        'schedule': 30.0, # Runs every 0.30 minutes
    },
}

app.conf.timezone = 'UTC'

app.autodiscover_tasks(['text_extraction.tasks', 'text_extraction.docvlm_task', 'text_extraction.idp_app.tasks'])
