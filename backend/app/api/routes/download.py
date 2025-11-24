from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
import os

from app.api.deps import get_db_session
from app.services.file_service import FileService
from app.models.upload import ProcessingStatus
from app.utils.logger import setup_logger

router = APIRouter()
logger = setup_logger(__name__)


@router.get("/{upload_id}")
async def download_processed_file(
    upload_id: int,
    db: Session = Depends(get_db_session)
):
    """
    Download processed GST file
    """
    try:
        file_service = FileService(db)
        upload = file_service.get_upload_by_id(upload_id)
        
        if not upload:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Upload with id {upload_id} not found"
            )
        
        if upload.status != ProcessingStatus.COMPLETED:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File processing not completed. Current status: {upload.status}"
            )
        
        if not upload.processed_file_path or not os.path.exists(upload.processed_file_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Processed file not found"
            )
        
        return FileResponse(
            path=upload.processed_file_path,
            filename=f"GST_Processed_{upload.original_filename}",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
