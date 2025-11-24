from sqlalchemy import Column, Integer, String, DateTime, Text, Enum, ForeignKey
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base
import enum


class ProcessingStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class Upload(Base):
    __tablename__ = "uploads"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    filename = Column(String(255), nullable=False)
    original_filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_size = Column(Integer, nullable=False)
    
    status = Column(
        Enum(ProcessingStatus),
        default=ProcessingStatus.PENDING,
        nullable=False
    )
    
    task_id = Column(String(255), nullable=True, index=True)
    processed_file_path = Column(String(500), nullable=True)
    
    error_message = Column(Text, nullable=True)
    processing_metadata = Column(Text, nullable=True)  # JSON string
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)

    user = relationship("User", back_populates="uploads")
    
    def __repr__(self):
        return f"<Upload {self.id}: {self.original_filename} - {self.status}>"
