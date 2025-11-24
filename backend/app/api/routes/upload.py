from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from sqlalchemy.orm import Session
import os

from app.database import get_db
from app.schemas.upload import UploadResponse
from app.schemas.response import ApiResponse
from app.services.file_service import FileService
from app.api.deps import get_current_active_user
from app.models.user import User
from app.utils.logger import setup_logger
from app.utils.helpers import (
    generate_unique_filename,
    is_allowed_file,
    validate_file_size
)
from app.config import settings

router = APIRouter()
logger = setup_logger(__name__)


@router.post("/", response_model=ApiResponse[UploadResponse])
async def upload_file(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Upload Excel file for GST processing
    """
    try:
        # Validate file extension
        if not is_allowed_file(file.filename):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File type not allowed. Allowed types: {settings.ALLOWED_EXTENSIONS}"
            )
        
        # Read file content to get size
        file_content = await file.read()
        file_size = len(file_content)
        
        # Validate file size
        if not validate_file_size(file_size):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File size exceeds maximum allowed size of {settings.MAX_UPLOAD_SIZE / (1024*1024)}MB"
            )
        
        # Generate unique filename
        unique_filename = generate_unique_filename(file.filename)
        file_path = os.path.join(settings.UPLOAD_DIR, unique_filename)
        
        # Save file
        with open(file_path, "wb") as buffer:
            buffer.write(file_content)
        
        logger.info(f"File saved: {file_path}")
        
        # Create upload record in database
        file_service = FileService(db)
        upload_record = file_service.create_upload(
            user_id=current_user.id,
            filename=unique_filename,
            original_filename=file.filename,
            file_path=file_path,
            file_size=file_size
        )
        
        logger.info(f"Upload record created with ID: {upload_record.id}")
        
        # Import task here to avoid circular imports
        from app.workers.tasks.process_file import process_uploaded_file
        
        # Trigger Celery task for processing
        logger.info(f"Triggering Celery task for upload_id: {upload_record.id}")
        task = process_uploaded_file.delay(upload_record.id)
        logger.info(f"Task triggered with ID: {task.id}")
        
        # Update task_id in database
        file_service.update_task_id(upload_record.id, task.id)
        
        logger.info(f"Processing task started: {task.id} for upload: {upload_record.id}")
        
        # Refresh to get updated data
        db.refresh(upload_record)
        
        return ApiResponse(
            success=True,
            message="File uploaded successfully and processing started",
            data=UploadResponse.model_validate(upload_record)
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
