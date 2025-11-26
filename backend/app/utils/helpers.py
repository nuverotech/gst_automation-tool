import os
import uuid
from datetime import datetime
from typing import Optional, List
from app.config import settings

_DEFAULT_ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'xlsb', 'csv'}
_MIN_UPLOAD_LIMIT_BYTES = 50 * 1024 * 1024  # 50MB safeguard


def generate_unique_filename(original_filename: str) -> str:
    """
    Generate a unique filename with timestamp and UUID
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    name, ext = os.path.splitext(original_filename)
    return f"{name}_{timestamp}_{unique_id}{ext}"


def _get_allowed_extensions() -> List[str]:
    allowed = set(ext.strip().lower() for ext in settings.allowed_extensions_list)
    allowed.update(_DEFAULT_ALLOWED_EXTENSIONS)
    return sorted(allowed)


def is_allowed_file(filename: str) -> bool:
    """
    Check if file extension is allowed
    """
    if '.' not in filename:
        return False
    ext = filename.rsplit('.', 1)[1].lower()
    return ext in _get_allowed_extensions()


def get_allowed_extensions_display() -> str:
    return ", ".join(ext.upper() for ext in _get_allowed_extensions())


def get_file_size_mb(file_size: int) -> float:
    """
    Convert file size to MB
    """
    return round(file_size / (1024 * 1024), 2)


def get_max_upload_limit() -> int:
    """
    Return the effective upload size limit in bytes
    (never lower than the default 50MB safeguard)
    """
    return max(settings.MAX_UPLOAD_SIZE, _MIN_UPLOAD_LIMIT_BYTES)


def validate_file_size(file_size: int) -> bool:
    """
    Check if file size is within allowed limit
    """
    return file_size <= get_max_upload_limit()
