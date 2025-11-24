from celery import Task
from typing import Dict
import pandas as pd

from app.workers.celery_app import celery_app
from app.services.template_service import TemplateService
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


@celery_app.task(name="app.workers.tasks.generate_template.create_gst_file")
def create_gst_file(output_path: str, data: Dict[str, Dict]) -> str:
    """
    Create GST template file
    
    Args:
        output_path: Path where file should be saved
        data: Dictionary with sheet names as keys and data dicts as values
    
    Returns:
        Path to created file
    """
    try:
        logger.info(f"Creating GST template at: {output_path}")
        
        # Convert data dicts to DataFrames
        df_data = {}
        for sheet_name, sheet_data in data.items():
            if sheet_data:
                df_data[sheet_name] = pd.DataFrame(sheet_data)
        
        # Create template
        template_service = TemplateService()
        result_path = template_service.create_gst_template(output_path, df_data)
        
        logger.info(f"GST template created successfully")
        return result_path
    
    except Exception as e:
        logger.error(f"Template generation error: {str(e)}", exc_info=True)
        raise
