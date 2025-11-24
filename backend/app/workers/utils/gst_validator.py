import re
from typing import Dict, List, Tuple
import pandas as pd

from app.services.validation_service import ValidationService
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


class GSTValidator:
    
    def __init__(self):
        self.validation_service = ValidationService()
        self.errors: List[Dict] = []
    
    def validate_row(self, row: pd.Series, row_index: int, validations: Dict[str, callable]) -> bool:
        """
        Validate a single row based on provided validation rules
        
        Args:
            row: Pandas Series representing a row
            row_index: Index of the row
            validations: Dict with column names as keys and validation functions as values
        
        Returns:
            True if all validations pass, False otherwise
        """
        is_valid = True
        
        for column, validator in validations.items():
            if column in row.index:
                value = row[column]
                
                # Skip validation for NaN values if not required
                if pd.isna(value):
                    continue
                
                valid, error_msg = validator(value)
                
                if not valid:
                    is_valid = False
                    self.errors.append({
                        'row': row_index,
                        'column': column,
                        'value': value,
                        'error': error_msg
                    })
                    logger.warning(f"Validation error at row {row_index}, column {column}: {error_msg}")
        
        return is_valid
    
    def validate_dataframe(self, df: pd.DataFrame, validation_rules: Dict[str, callable]) -> Tuple[pd.DataFrame, List[Dict]]:
        """
        Validate entire DataFrame
        
        Args:
            df: DataFrame to validate
            validation_rules: Dict with column names as keys and validation functions as values
        
        Returns:
            Tuple of (valid_df, errors)
        """
        self.errors = []
        valid_rows = []
        
        for idx, row in df.iterrows():
            if self.validate_row(row, idx, validation_rules):
                valid_rows.append(idx)
        
        valid_df = df.loc[valid_rows]
        
        logger.info(f"Validation complete. Valid rows: {len(valid_rows)}/{len(df)}, Errors: {len(self.errors)}")
        
        return valid_df, self.errors
    
    def get_b2b_validation_rules(self) -> Dict[str, callable]:
        """
        Get validation rules for B2B transactions
        """
        return {
            'GSTIN of Recipient': lambda x: self.validation_service.validate_gstin(str(x)),
            'Invoice Number': lambda x: self.validation_service.validate_invoice_number(str(x)),
            'Invoice Value': lambda x: self.validation_service.validate_amount(x),
            'Taxable Value': lambda x: self.validation_service.validate_amount(x),
        }
    
    def get_b2c_validation_rules(self) -> Dict[str, callable]:
        """
        Get validation rules for B2C transactions
        """
        return {
            'Invoice Number': lambda x: self.validation_service.validate_invoice_number(str(x)),
            'Invoice Value': lambda x: self.validation_service.validate_amount(x),
            'Taxable Value': lambda x: self.validation_service.validate_amount(x),
        }
