from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
import os

from app.database import get_db
from app.api.deps import get_current_active_user
from app.models.user import User
from app.services.template_service import TemplateService
from app.services.user_service import UserService
from app.schemas.response import ApiResponse
from app.utils.logger import setup_logger
from app.config import settings

router = APIRouter()
logger = setup_logger(__name__)


@router.post("/upload")
async def upload_custom_template(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Upload custom GST template for user
    """
    try:
        # Validate file type
        if not file.filename.endswith(('.xlsx', '.xls')):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only Excel files (.xlsx, .xls) are allowed"
            )
        
        # Read file content
        file_content = await file.read()
        
        # Save template
        template_path = TemplateService.save_user_template(
            file_content,
            current_user.id,
            file.filename
        )
        
        # Update user's custom template path
        user_service = UserService(db)
        current_user.custom_template_path = template_path
        db.commit()
        
        logger.info(f"User {current_user.id} uploaded custom template: {template_path}")
        
        return ApiResponse(
            success=True,
            message="Custom template uploaded successfully",
            data={"template_path": os.path.basename(template_path)}
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Template upload error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.delete("/")
async def delete_custom_template(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Delete user's custom template and revert to default
    """
    try:
        if current_user.custom_template_path and os.path.exists(current_user.custom_template_path):
            os.remove(current_user.custom_template_path)
            logger.info(f"Deleted custom template for user {current_user.id}")
        
        current_user.custom_template_path = None
        db.commit()
        
        return ApiResponse(
            success=True,
            message="Custom template deleted. Using default template.",
            data=None
        )
    
    except Exception as e:
        logger.error(f"Template deletion error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/download-default")
async def download_default_template():
    """
    Download the default GST template
    """
    try:
        default_template = os.path.join(
            settings.TEMPLATES_DIR,
            settings.DEFAULT_TEMPLATE_NAME
        )
        
        if not os.path.exists(default_template):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Default template not found"
            )
        
        return FileResponse(
            path=default_template,
            filename="GST_Template_Default.xlsx",
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Template download error: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/current")
async def get_current_template_info(
    current_user: User = Depends(get_current_active_user)
):
    """
    Get information about user's current template
    """
    is_custom = bool(current_user.custom_template_path)
    template_name = "Custom Template" if is_custom else "Default Template"
    
    return ApiResponse(
        success=True,
        message="Template info retrieved",
        data={
            "is_custom": is_custom,
            "template_name": template_name,
            "can_delete": is_custom
        }
    )
