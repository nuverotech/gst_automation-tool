from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class GSTR2BUploadPurchaseResponse(BaseModel):
    success: bool
    message: str
    file_path: str
    filename: str


class GSTR2BUpload2BFilesResponse(BaseModel):
    success: bool
    message: str
    file_paths: List[str]
    filenames: List[str]
    count: int


class GSTR2BProcessRequest(BaseModel):
    purchase_file_path: str
    purchase_filename: str
    gstr2b_file_paths: List[str]
    gstr2b_filenames: List[str]


class GSTR2BProcessResponse(BaseModel):
    success: bool
    message: str
    job_id: int
    task_id: Optional[str] = None


class GSTR2BStatusResponse(BaseModel):
    job_id: int = Field(alias="id")
    status: str
    created_at: datetime
    updated_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    processing_metadata: Optional[Dict[str, Any]] = None
    result_file_path: Optional[str] = None
    purchase_filename: str
    gstr2b_filenames: List[str]
    
    class Config:
        from_attributes = True
        populate_by_name = True


class GSTR2BJobResponse(BaseModel):
    id: int
    user_id: int
    purchase_filename: str
    gstr2b_filenames: List[str]
    status: str
    created_at: datetime
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    
    class Config:
        from_attributes = True
