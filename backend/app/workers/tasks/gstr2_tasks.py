from celery import shared_task
from sqlalchemy.orm import Session
from datetime import datetime

from app.database import SessionLocal
from app.models.gstr2b_upload import GSTR2BUpload
from app.gstr2.processor import process_gstr2b_files


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=30, retry_kwargs={"max_retries": 3})
def process_gstr2b_task(self, gstr2b_upload_id: int, purchase_upload_id: int):
    """
    Celery task to process GSTR-2B reconciliation using Excel files.
    """

    db: Session = SessionLocal()

    try:
        gstr2b_upload = db.get(GSTR2BUpload, gstr2b_upload_id)
        purchase_upload = db.get(GSTR2BUpload, purchase_upload_id)

        if not gstr2b_upload or not purchase_upload:
            raise ValueError("Invalid upload IDs")

        # Update status
        gstr2b_upload.status = "processing"
        db.commit()

        # Process Excel files
        result = process_gstr2b_files(
            gstr2b_path=gstr2b_upload.file_path,
            purchase_path=purchase_upload.file_path,
        )

        # Save metadata
        gstr2b_upload.status = "completed"
        gstr2b_upload.processed_at = datetime.utcnow()
        gstr2b_upload.processing_metadata = result

        db.commit()

    except Exception as exc:
        gstr2b_upload.status = "failed"
        gstr2b_upload.processing_metadata = {
            "error": str(exc)
        }
        db.commit()
        raise exc

    finally:
        db.close()
