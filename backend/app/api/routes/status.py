from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.upload import UploadStatusResponse
from app.schemas.response import ApiResponse
from app.services.file_service import FileService
from app.workers.celery_app import celery_app
from app.utils.logger import setup_logger
from app.api.deps import get_current_active_user
from app.models.user import User

router = APIRouter()
logger = setup_logger(__name__)


@router.get("/{upload_id}", response_model=ApiResponse[UploadStatusResponse])
async def get_upload_status(
    upload_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get processing status of uploaded file
    """
    try:
        file_service = FileService(db)
        upload = file_service.get_upload_by_id(upload_id)
        
        if not upload:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Upload with id {upload_id} not found"
            )
        
        # Verify user owns this upload
        if upload.user_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not authorized to access this upload"
            )
        
        # Get Celery task status if task_id exists
        progress = 0
        if upload.task_id:
            task = celery_app.AsyncResult(upload.task_id)
            if task.state == 'PROGRESS':
                progress = task.info.get('current', 0)
        
        response_data = UploadStatusResponse(
            id=upload.id,
            status=upload.status,
            task_id=upload.task_id,
            processed_file_path=upload.processed_file_path,
            error_message=upload.error_message,
            progress=progress
        )
        
        return ApiResponse(
            success=True,
            message="Status retrieved successfully",
            data=response_data
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Status check error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
