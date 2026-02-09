from sqlalchemy import Column, Integer, String, DateTime, Text, Enum, ForeignKey, JSON
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from app.database import Base
import enum


class GSTR2BProcessingStatus(str, enum.Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class GSTR2BProcessing(Base):
    __tablename__ = "gstr2b_processing"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    
    # File paths
    purchase_file_path = Column(String(500), nullable=False)
    purchase_filename = Column(String(255), nullable=False)
    gstr2b_file_paths = Column(JSON, nullable=False)  # Array of file paths
    gstr2b_filenames = Column(JSON, nullable=False)  # Array of original filenames
    
    # Processing
    status = Column(
        Enum(GSTR2BProcessingStatus),
        default=GSTR2BProcessingStatus.PENDING,
        nullable=False
    )
    task_id = Column(String(255), nullable=True, index=True)
    result_file_path = Column(String(500), nullable=True)
    
    # Metadata
    error_message = Column(Text, nullable=True)
    processing_metadata = Column(JSON, nullable=True)  # State-wise summary, etc.
    
    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    completed_at = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="gstr2b_jobs")
    
    def __repr__(self):
        return f"<GSTR2BProcessing {self.id}: {self.purchase_filename} - {self.status}>"
