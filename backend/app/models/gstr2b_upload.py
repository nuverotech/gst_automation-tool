"""
GSTR-2B Upload Model
===================

Stores GSTR-2B related Excel files and processing metadata.
No invoice-level data is persisted.
"""

from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    ForeignKey,
    Boolean,
    Index,
    func
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from app.database import Base


class GSTR2BUpload(Base):
    __tablename__ = "gstr2b_uploads"

    # ===== PRIMARY KEY =====
    id = Column(Integer, primary_key=True, index=True)

    # ===== FOREIGN KEYS =====
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    upload_id = Column(Integer, ForeignKey("uploads.id"), nullable=False, unique=True)

    # ===== FILE INFO =====
    file_type = Column(
        String(50),
        nullable=False,
        comment="GSTR2B | PURCHASE_REGISTER | RECONCILED_OUTPUT"
    )

    original_filename = Column(String(255), nullable=False)
    file_path = Column(String(500), nullable=False)
    file_hash = Column(String(64), nullable=False, index=True)

    # ===== PROCESSING STATUS =====
    status = Column(
        String(50),
        nullable=False,
        default="pending",
        comment="pending | processing | completed | failed"
    )

    # ===== METADATA =====
    file_metadata = Column(
        JSONB,
        nullable=True,
        comment="Sheet names, row counts, reconciliation summary, errors"
    )

    # ===== TIMESTAMPS =====
    uploaded_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    processed_at = Column(DateTime(timezone=True), nullable=True)

    # ===== RELATIONSHIPS =====
    user = relationship("User", backref="gstr2b_uploads")
    upload = relationship("Upload", backref="gstr2b_details")

    # ===== INDEXES =====
    __table_args__ = (
        Index("ix_gstr2b_upload_user", "user_id"),
        Index("ix_gstr2b_upload_status", "status"),
        Index("ix_gstr2b_upload_type", "file_type"),
    )

    def __repr__(self) -> str:
        return (
            f"<GSTR2BUpload("
            f"id={self.id}, "
            f"type={self.file_type}, "
            f"status={self.status}"
            f")>"
        )
