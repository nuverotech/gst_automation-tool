from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime

from app.models.gstr2b_processing import GSTR2BProcessing, GSTR2BProcessingStatus
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


class GSTR2BService:
    def __init__(self, db: Session):
        self.db = db
    
    def create_job(
        self,
        user_id: int,
        purchase_file_path: str,
        purchase_filename: str,
        gstr2b_file_paths: List[str],
        gstr2b_filenames: List[str]
    ) -> GSTR2BProcessing:
        """Create a new GSTR2B processing job"""
        job = GSTR2BProcessing(
            user_id=user_id,
            purchase_file_path=purchase_file_path,
            purchase_filename=purchase_filename,
            gstr2b_file_paths=gstr2b_file_paths,
            gstr2b_filenames=gstr2b_filenames,
            status=GSTR2BProcessingStatus.PENDING
        )
        
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)
        
        logger.info(f"Created GSTR2B job {job.id} for user {user_id}")
        return job
    
    def get_job(self, job_id: int) -> Optional[GSTR2BProcessing]:
        """Get a GSTR2B job by ID"""
        return self.db.query(GSTR2BProcessing).filter(GSTR2BProcessing.id == job_id).first()
    
    def get_user_jobs(self, user_id: int, limit: int = 50) -> List[GSTR2BProcessing]:
        """Get all GSTR2B jobs for a user"""
        return (
            self.db.query(GSTR2BProcessing)
            .filter(GSTR2BProcessing.user_id == user_id)
            .order_by(GSTR2BProcessing.created_at.desc())
            .limit(limit)
            .all()
        )
    
    def update_task_id(self, job_id: int, task_id: str) -> None:
        """Update the Celery task ID for a job"""
        job = self.get_job(job_id)
        if job:
            job.task_id = task_id
            self.db.commit()
            logger.info(f"Updated job {job_id} with task_id {task_id}")
    
    def update_status(
        self,
        job_id: int,
        status: GSTR2BProcessingStatus,
        error_message: Optional[str] = None,
        processing_metadata: Optional[Dict[str, Any]] = None,
        result_file_path: Optional[str] = None
    ) -> None:
        """Update job status and related fields"""
        job = self.get_job(job_id)
        if job:
            job.status = status
            if error_message:
                job.error_message = error_message
            if processing_metadata:
                job.processing_metadata = processing_metadata
            if result_file_path:
                job.result_file_path = result_file_path
            if status == GSTR2BProcessingStatus.COMPLETED or status == GSTR2BProcessingStatus.FAILED:
                job.completed_at = datetime.utcnow()
            
            self.db.commit()
            logger.info(f"Updated job {job_id} status to {status}")
    
    def delete_job(self, job_id: int) -> bool:
        """Delete a GSTR2B job"""
        job = self.get_job(job_id)
        if job:
            self.db.delete(job)
            self.db.commit()
            logger.info(f"Deleted job {job_id}")
            return True
        return False
