from sqlalchemy import Column, Integer, String, Text, Boolean, DateTime
from sqlalchemy.sql import func
from app.database import Base


class ColumnMapping(Base):
    __tablename__ = "column_mappings"
    
    id = Column(Integer, primary_key=True, index=True)
    source_column = Column(String(255), nullable=False)
    target_column = Column(String(255), nullable=False)
    sheet_name = Column(String(100), nullable=False)
    
    validation_regex = Column(String(500), nullable=True)
    is_required = Column(Boolean, default=False)
    
    description = Column(Text, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    
    def __repr__(self):
        return f"<ColumnMapping {self.source_column} -> {self.target_column}>"
