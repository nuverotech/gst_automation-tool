from sqlalchemy.orm import Session
from datetime import datetime
from typing import Optional

from app.models.upload import Upload, ProcessingStatus
from app.schemas.upload import UploadCreate


class FileService:
    def __init__(self, db: Session):
        self.db = db
    
    def create_upload(
        self,
        filename: str,
        original_filename: str,
        file_path: str,
        file_size: int
    ) -> Upload:
        """
        Create new upload record
        """
        upload = Upload(
            filename=filename,
            original_filename=original_filename,
            file_path=file_path,
            file_size=file_size,
            status=ProcessingStatus.PENDING
        )
        self.db.add(upload)
        self.db.commit()
        self.db.refresh(upload)
        return upload
    
    def get_upload_by_id(self, upload_id: int) -> Optional[Upload]:
        """
        Get upload by ID
        """
        return self.db.query(Upload).filter(Upload.id == upload_id).first()
    
    def update_task_id(self, upload_id: int, task_id: str) -> Upload:
        """
        Update task ID for upload
        """
        upload = self.get_upload_by_id(upload_id)
        if upload:
            upload.task_id = task_id
            self.db.commit()
            self.db.refresh(upload)
        return upload
    
    def update_status(
        self,
        upload_id: int,
        status: ProcessingStatus,
        error_message: Optional[str] = None
    ) -> Upload:
        """
        Update processing status
        """
        upload = self.get_upload_by_id(upload_id)
        if upload:
            upload.status = status
            if error_message:
                upload.error_message = error_message
            if status == ProcessingStatus.COMPLETED:
                upload.completed_at = datetime.utcnow()
            self.db.commit()
            self.db.refresh(upload)
        return upload
    
    def update_processed_file_path(
        self,
        upload_id: int,
        processed_file_path: str
    ) -> Upload:
        """
        Update processed file path
        """
        upload = self.get_upload_by_id(upload_id)
        if upload:
            upload.processed_file_path = processed_file_path
            self.db.commit()
            self.db.refresh(upload)
        return upload
