from celery import Task
from sqlalchemy.orm import Session
import os

from app.workers.celery_app import celery_app
from app.database import SessionLocal
from app.models.upload import ProcessingStatus
from app.services.file_service import FileService
from app.workers.utils.excel_parser import ExcelParser
from app.workers.utils.gst_validator import GSTValidator
from app.workers.utils.sheet_mapper import SheetMapper
from app.services.template_service import TemplateService
from app.config import settings
from app.utils.logger import setup_logger
from app.utils.helpers import generate_unique_filename

logger = setup_logger(__name__)


class ProcessFileTask(Task):
    """Base task with database session"""
    _db = None
    
    @property
    def db(self) -> Session:
        if self._db is None:
            self._db = SessionLocal()
        return self._db
    
    def after_return(self, *args, **kwargs):
        if self._db is not None:
            self._db.close()


@celery_app.task(bind=True, base=ProcessFileTask, name="app.workers.tasks.process_file.process_uploaded_file")
def process_uploaded_file(self, upload_id: int):
    """
    Main task to process uploaded file
    """
    logger.info(f"Starting processing for upload_id: {upload_id}")
    
    file_service = FileService(self.db)
    
    try:
        # Update status to processing
        file_service.update_status(upload_id, ProcessingStatus.PROCESSING)
        
        # Get upload record
        upload = file_service.get_upload_by_id(upload_id)
        if not upload:
            raise Exception(f"Upload with id {upload_id} not found")
        
        # Initialize parser and utilities
        parser = ExcelParser(upload.file_path)
        validator = GSTValidator()
        mapper = SheetMapper()
        template_service = TemplateService(upload.user.custom_template_path)
        
        logger.info(f"Processing file: {upload.file_path}")
        
        # Read Excel file
        df = parser.read_excel()
        logger.info(f"File read successfully. Shape: {df.shape}")
        
        # Update progress
        self.update_state(state='PROGRESS', meta={'current': 25, 'status': 'File read successfully'})
        
        # Prepare data for template (classify and split by sheet)
        populated_sheets = mapper.prepare_data_for_template(df)
        logger.info(f"Data prepared for {len(populated_sheets)} sheets")
        
        # Update progress
        self.update_state(state='PROGRESS', meta={'current': 50, 'status': 'Data classified'})
        
        # Validate each sheet
        validated_data = {}
        for sheet_type, sheet_df in populated_sheets.items():
            if not sheet_df.empty:
                logger.info(f"Validating sheet '{sheet_type}' with {len(sheet_df)} rows")
                
                # Get validation rules based on sheet type
                if sheet_type == 'b2b':
                    validation_rules = validator.get_b2b_validation_rules()
                elif sheet_type == 'b2cs':
                    validation_rules = validator.get_b2c_validation_rules()
                else:
                    validation_rules = {}
                
                # Validate
                valid_df, errors = validator.validate_dataframe(sheet_df, validation_rules)
                
                if len(errors) > 0:
                    logger.warning(f"Validation errors for '{sheet_type}': {len(errors)} errors")
                
                validated_data[sheet_type] = valid_df
                logger.info(f"Sheet '{sheet_type}' validated: {len(valid_df)} valid rows")
        
        logger.info(f"All sheets validated: {[(k, len(v)) for k, v in validated_data.items()]}")
        
        # Update progress
        self.update_state(state='PROGRESS', meta={'current': 75, 'status': 'Data validated'})
        
        # Generate output file from template
        output_filename = generate_unique_filename(f"GST_Processed_{upload.original_filename}")
        output_path = os.path.join(settings.PROCESSED_DIR, output_filename)
        
        logger.info(f"Creating output file from template: {output_path}")
        
        # Create GST file using template
        template_service.create_gst_file_from_template(output_path, validated_data)
        
        # Update database with processed file path
        file_service.update_processed_file_path(upload_id, output_path)
        file_service.update_status(upload_id, ProcessingStatus.COMPLETED)
        
        # Update progress
        self.update_state(state='PROGRESS', meta={'current': 100, 'status': 'Processing completed'})
        
        logger.info(f"Processing completed for upload_id: {upload_id}")
        
        return {
            'upload_id': upload_id,
            'status': 'completed',
            'processed_file': output_path
        }
    
    except Exception as e:
        logger.error(f"Processing failed for upload_id {upload_id}: {str(e)}", exc_info=True)
        file_service.update_status(
            upload_id,
            ProcessingStatus.FAILED,
            error_message=str(e)
        )
        raise


def process_file_sync(upload_id: int, db: Session):
    """
    Synchronous version of file processing (without Celery)
    For testing without Redis
    """
    logger.info(f"Starting synchronous processing for upload_id: {upload_id}")
    
    file_service = FileService(db)
    
    try:
        # Update status to processing
        file_service.update_status(upload_id, ProcessingStatus.PROCESSING)
        
        # Get upload record
        upload = file_service.get_upload_by_id(upload_id)
        if not upload:
            raise Exception(f"Upload with id {upload_id} not found")
        
        # Initialize parser and utilities
        parser = ExcelParser(upload.file_path)
        validator = GSTValidator()
        mapper = SheetMapper()
        template_service = TemplateService()
        
        logger.info(f"Processing file: {upload.file_path}")
        
        # Read Excel file
        df = parser.read_excel()
        logger.info(f"File read successfully. Shape: {df.shape}")
        
        # Prepare data for template (classify and split by sheet)
        populated_sheets = mapper.prepare_data_for_template(df)
        logger.info(f"Data prepared for {len(populated_sheets)} sheets")
        
        # Validate each sheet
        validated_data = {}
        for sheet_type, sheet_df in populated_sheets.items():
            if not sheet_df.empty:
                logger.info(f"Validating sheet '{sheet_type}' with {len(sheet_df)} rows")
                
                # Get validation rules based on sheet type
                if sheet_type == 'b2b':
                    validation_rules = validator.get_b2b_validation_rules()
                elif sheet_type == 'b2cs':
                    validation_rules = validator.get_b2c_validation_rules()
                else:
                    validation_rules = {}
                
                # Validate
                valid_df, errors = validator.validate_dataframe(sheet_df, validation_rules)
                
                if len(errors) > 0:
                    logger.warning(f"Validation errors for '{sheet_type}': {len(errors)} errors")
                
                validated_data[sheet_type] = valid_df
                logger.info(f"Sheet '{sheet_type}' validated: {len(valid_df)} valid rows")
        
        logger.info(f"All sheets validated: {[(k, len(v)) for k, v in validated_data.items()]}")
        
        # Generate output file from template
        output_filename = generate_unique_filename(f"GST_Processed_{upload.original_filename}")
        output_path = os.path.join(settings.PROCESSED_DIR, output_filename)
        
        logger.info(f"Creating output file from template: {output_path}")
        
        # Create GST file using template
        template_service.create_gst_file_from_template(output_path, validated_data)
        
        # Update database with processed file path
        file_service.update_processed_file_path(upload_id, output_path)
        file_service.update_status(upload_id, ProcessingStatus.COMPLETED)
        
        logger.info(f"Synchronous processing completed for upload_id: {upload_id}")
        
        return {
            'upload_id': upload_id,
            'status': 'completed',
            'processed_file': output_path
        }
    
    except Exception as e:
        logger.error(f"Synchronous processing failed for upload_id {upload_id}: {str(e)}", exc_info=True)
        file_service.update_status(
            upload_id,
            ProcessingStatus.FAILED,
            error_message=str(e)
        )
        raise
