from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional
from app.models.upload import ProcessingStatus


class UploadBase(BaseModel):
    filename: str
    original_filename: str


class UploadCreate(UploadBase):
    file_path: str
    file_size: int


class UploadResponse(UploadBase):
    id: int
    status: ProcessingStatus
    task_id: Optional[str] = None
    processed_file_path: Optional[str] = None
    error_message: Optional[str] = None
    created_at: datetime
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    class Config:
        from_attributes = True


class UploadStatusResponse(BaseModel):
    id: int
    status: ProcessingStatus
    task_id: Optional[str] = None
    processed_file_path: Optional[str] = None
    error_message: Optional[str] = None
    progress: Optional[int] = Field(default=0, ge=0, le=100)
    
    class Config:
        from_attributes = True
