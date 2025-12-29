from celery import Task
from sqlalchemy.orm import Session
import os

from app.workers.celery_app import celery_app
from app.database import SessionLocal
from app.models.upload import ProcessingStatus
from app.services.file_service import FileService
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.helpers import generate_unique_filename

logger = setup_logger(__name__)


class ProcessFileTask(Task):
    """Base task with database session"""
    _db = None
    
    @property
    def db(self) -> Session:
        if self._db is None:
            self._db = SessionLocal()
        return self._db
    
    def after_return(self, *args, **kwargs):
        if self._db is not None:
            self._db.close()

@celery_app.task(
    bind=True,
    base=ProcessFileTask,
    name="app.workers.tasks.process_file.process_uploaded_file"
)
def process_uploaded_file(self, upload_id: int):
    logger.info(f"Processing upload_id: {upload_id}")

    file_service = FileService(self.db)

    try:
        # Mark as processing
        file_service.update_status(upload_id, ProcessingStatus.PROCESSING)

        upload = file_service.get_upload_by_id(upload_id)
        if not upload:
            raise Exception(f"Upload {upload_id} not found")

        # Paths
        input_path = upload.file_path
        template_path = upload.user.custom_template_path or settings.DEFAULT_GSTR1_TEMPLATE

        base_name, _ = os.path.splitext(upload.original_filename)
        output_filename = generate_unique_filename(f"GSTR1_{base_name}.xlsx")
        output_path = os.path.join(settings.PROCESSED_DIR, output_filename)

        logger.info(f"Running new GSTR1 engine on: {input_path}")

        from app.gstr1.cli import run_gstr1

        # -------------------------------
        # PROGRESS CALLBACK
        # -------------------------------
        def update_progress(percent, message):
            self.update_state(
                state='PROGRESS',
                meta={'current': percent, 'status': message}
            )
            logger.info(f"[Progress] {percent}% - {message}")

        # -------------------------------
        # RUN THE NEW FAST GSTR1 ENGINE
        # -------------------------------

        run_gstr1(
            input_path=input_path,
            template_path=template_path,
            output_path=output_path,
            progress_callback=update_progress
        )

        logger.info(f"GSTR1 file created: {output_path}")

        # Save results
        file_service.update_processed_file_path(upload_id, output_path)
        file_service.update_status(upload_id, ProcessingStatus.COMPLETED)

        return {
            "upload_id": upload_id,
            "status": "completed",
            "processed_file": output_path
        }

    except Exception as e:
        logger.error(
            f"GSTR1 processing failed: {str(e)}",
            exc_info=True
        )
        file_service.update_status(
            upload_id,
            ProcessingStatus.FAILED,
            error_message=str(e)
        )
        raise
