from celery import Task
import pandas as pd
from typing import Dict, List

from app.workers.celery_app import celery_app
from app.workers.utils.gst_validator import GSTValidator
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


@celery_app.task(name="app.workers.tasks.validate_data.validate_gst_data")
def validate_gst_data(data_dict: Dict, sheet_type: str) -> Dict:
    """
    Validate GST data
    
    Args:
        data_dict: Dictionary representation of DataFrame
        sheet_type: Type of sheet (B2B, B2C, etc.)
    
    Returns:
        Dict with validation results
    """
    try:
        logger.info(f"Validating {sheet_type} data")
        
        # Convert dict to DataFrame
        df = pd.DataFrame(data_dict)
        
        # Initialize validator
        validator = GSTValidator()
        
        # Get validation rules
        if sheet_type == 'B2B':
            rules = validator.get_b2b_validation_rules()
        elif sheet_type == 'B2C':
            rules = validator.get_b2c_validation_rules()
        else:
            rules = {}
        
        # Validate
        valid_df, errors = validator.validate_dataframe(df, rules)
        
        return {
            'valid_data': valid_df.to_dict('records'),
            'errors': errors,
            'valid_count': len(valid_df),
            'total_count': len(df)
        }
    
    except Exception as e:
        logger.error(f"Validation error: {str(e)}", exc_info=True)
        raise
