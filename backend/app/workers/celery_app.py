from celery import Celery

# Create Celery app
celery_app = Celery(
    "gst_automation",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

# Configure Celery
celery_app.conf.update(
    task_track_started=True,
    task_time_limit=300,
    result_expires=3600,
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    broker_connection_retry_on_startup=True,
    imports=[
        'app.workers.tasks.process_file',
        'app.workers.tasks.validate_data',
        'app.workers.tasks.generate_template',
    ]
)

# Import tasks after app configuration
from app.workers.tasks import process_file, validate_data, generate_template
