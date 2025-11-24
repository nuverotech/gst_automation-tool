from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List

from app.database import get_db
from app.schemas.user import UserResponse
from app.schemas.upload import UploadResponse
from app.api.deps import get_current_active_user
from app.models.user import User
from app.services.file_service import FileService
from app.utils.logger import setup_logger

router = APIRouter()
logger = setup_logger(__name__)


@router.get("/me", response_model=UserResponse)
def get_me(current_user: User = Depends(get_current_active_user)):
    """
    Get current user profile
    """
    return current_user


@router.get("/uploads", response_model=List[UploadResponse])
def get_my_uploads(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get all uploads for current user
    """
    file_service = FileService(db)
    uploads = file_service.get_uploads_by_user(current_user.id)
    return uploads
