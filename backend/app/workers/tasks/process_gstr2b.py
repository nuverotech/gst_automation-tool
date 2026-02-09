from celery import shared_task
from sqlalchemy.orm import Session
from datetime import datetime
from pathlib import Path
import os

from app.database import SessionLocal
from app.models.gstr2b_processing import GSTR2BProcessing, GSTR2BProcessingStatus
from app.gstr2.processor import process_b2b_multi_state
from app.gstr2.excel_generator import generate_reconciliation_report
from app.services.gstr2b_service import GSTR2BService
from app.utils.logger import setup_logger
from app.utils.helpers import generate_unique_filename
from app.config import settings

logger = setup_logger(__name__)


@shared_task(bind=True, autoretry_for=(Exception,), retry_backoff=30, retry_kwargs={"max_retries": 3})
def process_gstr2b_reconciliation(self, job_id: int):
    """
    Celery task to process GSTR2B reconciliation.
    
    Args:
        job_id: ID of the GSTR2BProcessing record
    """
    db: Session = SessionLocal()
    
    try:
        logger.info(f"Starting GSTR2B reconciliation task for job {job_id}")
        
        # Get job from database
        gstr2b_service = GSTR2BService(db)
        job = gstr2b_service.get_job(job_id)
        
        if not job:
            raise ValueError(f"GSTR2B job {job_id} not found")
        
        # Update status to processing
        gstr2b_service.update_status(job_id, GSTR2BProcessingStatus.PROCESSING)
        logger.info(f"Job {job_id} status updated to PROCESSING")
        
        # Process reconciliation using existing logic
        logger.info(f"Processing purchase book: {job.purchase_file_path}")
        logger.info(f"Processing {len(job.gstr2b_file_paths)} GSTR2B files")
        
        # Call the existing reconciliation function
        result = process_b2b_multi_state(
            purchase_path=job.purchase_file_path,
            gstr2b_paths=job.gstr2b_file_paths
        )
        
        logger.info(f"Reconciliation completed. Overall stats: {result.get('overall', {})}")
        
        # Now we need to get the actual reconciled data rows
        # Re-run the reconciliation to get detailed rows (not just summary)
        from app.gstr2.sheet_reader.b2b import read_purchase_register, read_gstr2b_b2b
        from app.gstr2.reconciler import reconcile_b2b
        
        all_purchase_rows = read_purchase_register(job.purchase_file_path)
        all_reconciled_rows = []
        
        for gstr2b_path in job.gstr2b_file_paths:
            gstr2b_rows = read_gstr2b_b2b(gstr2b_path)
            
            # Get state from first row
            states = {r["pos_state"] for r in gstr2b_rows}
            if len(states) != 1:
                raise ValueError(f"Multiple POS states in {gstr2b_path}")
            
            state = states.pop()
            
            # Filter purchase rows for this state
            purchase_rows = [
                p for p in all_purchase_rows
                if p["pos_state"] == state
            ]
            
            # Reconcile
            reconciled = reconcile_b2b(
                gstr2b_rows=gstr2b_rows,
                purchase_rows=purchase_rows
            )
            
            # Add POS state to each row
            for row in reconciled:
                row["pos_state"] = state
            
            all_reconciled_rows.extend(reconciled)
        
        logger.info(f"Total reconciled rows: {len(all_reconciled_rows)}")
        
        # Generate Excel report
        output_filename = f"GSTR2B_Reconciliation_{job_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        output_path = os.path.join(settings.PROCESSED_DIR, output_filename)
        
        generate_reconciliation_report(
            reconciled_data=all_reconciled_rows,
            output_path=output_path,
            state_wise_summary=result
        )
        
        logger.info(f"Excel report generated: {output_path}")
        
        # Update job with results
        gstr2b_service.update_status(
            job_id=job_id,
            status=GSTR2BProcessingStatus.COMPLETED,
            processing_metadata=result,
            result_file_path=output_path
        )
        
        logger.info(f"Job {job_id} completed successfully")
        return {
            "job_id": job_id,
            "status": "completed",
            "output_file": output_path,
            "summary": result
        }
    
    except Exception as exc:
        logger.error(f"Error processing GSTR2B job {job_id}: {str(exc)}", exc_info=True)
        
        # Update job status to failed
        gstr2b_service = GSTR2BService(db)
        gstr2b_service.update_status(
            job_id=job_id,
            status=GSTR2BProcessingStatus.FAILED,
            error_message=str(exc)
        )
        
        raise exc
    
    finally:
        db.close()
