from pydantic import BaseModel
from typing import Optional, Any, Generic, TypeVar

DataT = TypeVar('DataT')


class ApiResponse(BaseModel, Generic[DataT]):
    success: bool
    message: str
    data: Optional[DataT] = None
    error: Optional[str] = None


class ErrorResponse(BaseModel):
    success: bool = False
    message: str
    error: str
    detail: Optional[Any] = None
