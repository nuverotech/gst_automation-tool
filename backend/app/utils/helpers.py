import os
import uuid
from datetime import datetime
from typing import Optional
from app.config import settings


def generate_unique_filename(original_filename: str) -> str:
    """
    Generate a unique filename with timestamp and UUID
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    name, ext = os.path.splitext(original_filename)
    return f"{name}_{timestamp}_{unique_id}{ext}"


def is_allowed_file(filename: str) -> bool:
    """
    Check if file extension is allowed
    """
    if '.' not in filename:
        return False
    ext = filename.rsplit('.', 1)[1].lower()
    return ext in settings.allowed_extensions_list


def get_file_size_mb(file_size: int) -> float:
    """
    Convert file size to MB
    """
    return round(file_size / (1024 * 1024), 2)


def validate_file_size(file_size: int) -> bool:
    """
    Check if file size is within allowed limit
    """
    return file_size <= settings.MAX_UPLOAD_SIZE
