from fastapi import APIRouter, UploadFile, File, Depends, HTTPException, status
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.orm import Session
from typing import List
import os
import shutil

from app.database import get_db
from app.schemas.gstr2b_schemas import (
    GSTR2BUploadPurchaseResponse,
    GSTR2BUpload2BFilesResponse,
    GSTR2BProcessRequest,
    GSTR2BProcessResponse,
    GSTR2BStatusResponse,
    GSTR2BJobResponse
)
from app.schemas.response import ApiResponse
from app.services.gstr2b_service import GSTR2BService
from app.api.deps import get_current_active_user
from app.models.user import User
from app.utils.logger import setup_logger
from app.utils.helpers import (
    generate_unique_filename,
    is_allowed_file,
    validate_file_size,
    get_max_upload_limit,
    get_file_size_mb
)
from app.config import settings

router = APIRouter()
logger = setup_logger(__name__)

# Temporary storage for uploaded files per session
# In production, consider using Redis or similar
upload_sessions = {}


@router.post("/upload-purchase", response_model=GSTR2BUploadPurchaseResponse)
async def upload_purchase_book(
    file: UploadFile = File(...),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Upload purchase book Excel file
    """
    try:
        # Validate file extension
        if not is_allowed_file(file.filename):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="File type not allowed. Only Excel files are supported."
            )
        
        # Read file content
        file_content = await file.read()
        file_size = len(file_content)
        
        # Validate file size
        if not validate_file_size(file_size):
            max_limit_mb = get_file_size_mb(get_max_upload_limit())
            actual_size_mb = get_file_size_mb(file_size)
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"File size {actual_size_mb}MB exceeds the {max_limit_mb}MB limit"
            )
        
        # Generate unique filename
        unique_filename = f"purchase_{generate_unique_filename(file.filename)}"
        file_path = os.path.join(settings.UPLOAD_DIR, unique_filename)
        
        # Save file
        with open(file_path, "wb") as buffer:
            buffer.write(file_content)
        
        logger.info(f"Purchase book uploaded: {file_path}")
        
        # Store in session (simple in-memory storage)
        session_key = f"user_{current_user.id}"
        if session_key not in upload_sessions:
            upload_sessions[session_key] = {}
        
        upload_sessions[session_key]["purchase_file"] = {
            "path": file_path,
            "filename": file.filename
        }
        
        return GSTR2BUploadPurchaseResponse(
            success=True,
            message="Purchase book uploaded successfully",
            file_path=file_path,
            filename=file.filename
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading purchase book: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/upload-2b-files", response_model=GSTR2BUpload2BFilesResponse)
async def upload_gstr2b_files(
    files: List[UploadFile] = File(...),
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Upload multiple GSTR2B Excel files (one per state)
    """
    try:
        if not files:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No files provided"
            )
        
        uploaded_files = []
        uploaded_filenames = []
        
        for file in files:
            # Validate file extension
            if not is_allowed_file(file.filename):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File type not allowed for {file.filename}. Only Excel files are supported."
                )
            
            # Read file content
            file_content = await file.read()
            file_size = len(file_content)
            
            # Validate file size
            if not validate_file_size(file_size):
                max_limit_mb = get_file_size_mb(get_max_upload_limit())
                actual_size_mb = get_file_size_mb(file_size)
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"File {file.filename} size {actual_size_mb}MB exceeds the {max_limit_mb}MB limit"
                )
            
            # Generate unique filename
            unique_filename = f"gstr2b_{generate_unique_filename(file.filename)}"
            file_path = os.path.join(settings.UPLOAD_DIR, unique_filename)
            
            # Save file
            with open(file_path, "wb") as buffer:
                buffer.write(file_content)
            
            uploaded_files.append(file_path)
            uploaded_filenames.append(file.filename)
            logger.info(f"GSTR2B file uploaded: {file_path}")
        
        # Store in session
        session_key = f"user_{current_user.id}"
        if session_key not in upload_sessions:
            upload_sessions[session_key] = {}
        
        upload_sessions[session_key]["gstr2b_files"] = {
            "paths": uploaded_files,
            "filenames": uploaded_filenames
        }
        
        return GSTR2BUpload2BFilesResponse(
            success=True,
            message=f"{len(uploaded_files)} GSTR2B files uploaded successfully",
            file_paths=uploaded_files,
            filenames=uploaded_filenames,
            count=len(uploaded_files)
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading GSTR2B files: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.post("/process", response_model=GSTR2BProcessResponse)
async def process_reconciliation(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Trigger GSTR2B reconciliation processing
    """
    try:
        # Get uploaded files from session
        session_key = f"user_{current_user.id}"
        if session_key not in upload_sessions:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No files found. Please upload files first."
            )
        
        session_data = upload_sessions[session_key]
        
        if "purchase_file" not in session_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Purchase book not uploaded"
            )
        
        if "gstr2b_files" not in session_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="GSTR2B files not uploaded"
            )
        
        purchase_data = session_data["purchase_file"]
        gstr2b_data = session_data["gstr2b_files"]
        
        # Create processing job
        gstr2b_service = GSTR2BService(db)
        job = gstr2b_service.create_job(
            user_id=current_user.id,
            purchase_file_path=purchase_data["path"],
            purchase_filename=purchase_data["filename"],
            gstr2b_file_paths=gstr2b_data["paths"],
            gstr2b_filenames=gstr2b_data["filenames"]
        )
        
        logger.info(f"Created GSTR2B job {job.id}")
        
        # Trigger Celery task
        from app.workers.tasks.process_gstr2b import process_gstr2b_reconciliation
        
        task = process_gstr2b_reconciliation.delay(job.id)
        logger.info(f"Triggered Celery task {task.id} for job {job.id}")
        
        # Update task ID
        gstr2b_service.update_task_id(job.id, task.id)
        
        # Clear session data
        upload_sessions.pop(session_key, None)
        
        return GSTR2BProcessResponse(
            success=True,
            message="Reconciliation processing started",
            job_id=job.id,
            task_id=task.id
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error starting reconciliation: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/status/{job_id}", response_model=GSTR2BStatusResponse)
async def get_job_status(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get GSTR2B job status
    """
    try:
        gstr2b_service = GSTR2BService(db)
        job = gstr2b_service.get_job(job_id)
        
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Job not found"
            )
        
        # Verify user owns this job
        if job.user_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        return GSTR2BStatusResponse.model_validate(job)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching job status: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/download/{job_id}")
async def download_report(
    job_id: int,
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Download reconciliation report
    """
    try:
        gstr2b_service = GSTR2BService(db)
        job = gstr2b_service.get_job(job_id)
        
        if not job:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Job not found"
            )
        
        # Verify user owns this job
        if job.user_id != current_user.id:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Access denied"
            )
        
        if not job.result_file_path or not os.path.exists(job.result_file_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Report file not found"
            )
        
        filename = os.path.basename(job.result_file_path)
        
        return FileResponse(
            path=job.result_file_path,
            filename=filename,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading report: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )


@router.get("/jobs", response_model=List[GSTR2BJobResponse])
async def get_user_jobs(
    current_user: User = Depends(get_current_active_user),
    db: Session = Depends(get_db)
):
    """
    Get all GSTR2B jobs for current user
    """
    try:
        gstr2b_service = GSTR2BService(db)
        jobs = gstr2b_service.get_user_jobs(current_user.id)
        
        return [GSTR2BJobResponse.model_validate(job) for job in jobs]
    
    except Exception as e:
        logger.error(f"Error fetching user jobs: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
