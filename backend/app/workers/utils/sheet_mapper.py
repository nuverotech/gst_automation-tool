import pandas as pd
from typing import Dict, List
from app.utils.logger import setup_logger
from app.services.template_service import TemplateService

logger = setup_logger(__name__)


class SheetMapper:
    
    def __init__(self):
        self.template_service = TemplateService()
        self.template_sheets = self.template_service.get_template_sheets()
        
        logger.info(f"Template sheets available: {self.template_sheets}")
    
    def classify_transaction_type(self, row: pd.Series) -> str:
        """
        Classify transaction as b2b, b2cl, b2cs, export, etc.
        based on the data characteristics
        """
        # Check if GSTIN exists and is valid (B2B indicator)
        gstin_columns = [col for col in row.index if 'gstin' in str(col).lower() and 'recipient' in str(col).lower()]
        
        if gstin_columns:
            gstin_value = row[gstin_columns[0]]
            if pd.notna(gstin_value) and str(gstin_value).strip():
                return "b2b"
        
        # Check for export indicators
        export_columns = [col for col in row.index if any(kw in str(col).lower() for kw in ['export', 'shipping', 'overseas'])]
        if export_columns:
            export_value = row[export_columns[0]]
            if pd.notna(export_value) and str(export_value).strip():
                return "export"
        
        # Check for debit/credit note
        if any('note' in str(col).lower() or 'credit' in str(col).lower() or 'debit' in str(col).lower() 
               for col in row.index):
            return "cdnr"
        
        # Check for B2C (no GSTIN, but has invoice and amount)
        invoice_columns = [col for col in row.index if 'invoice' in str(col).lower()]
        if invoice_columns:
            return "b2cs"
        
        # Default to B2C small
        return "b2cs"
    
    def split_dataframe_by_sheet_type(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Split DataFrame into sheets matching template sheet names
        
        Returns:
            Dict with sheet names as keys and DataFrames as values
            (only populated sheets are included; empty sheets handled by template)
        """
        # Initialize dict with all template sheets
        result = {sheet: pd.DataFrame() for sheet in self.template_sheets}
        
        # Collect rows by type
        rows_by_type = {}
        
        for idx, row in df.iterrows():
            transaction_type = self.classify_transaction_type(row)
            
            if transaction_type not in rows_by_type:
                rows_by_type[transaction_type] = []
            
            rows_by_type[transaction_type].append(row)
        
        # Convert to DataFrames
        for sheet_type, rows in rows_by_type.items():
            if rows and sheet_type in result:
                result[sheet_type] = pd.DataFrame(rows)
                logger.info(f"Sheet '{sheet_type}': {len(rows)} rows")
            elif sheet_type not in result:
                logger.warning(f"Transaction type '{sheet_type}' not in template sheets")
        
        logger.info(f"Data split complete: {[(k, len(v)) for k, v in result.items() if not v.empty]}")
        
        return result
    
    def prepare_data_for_template(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Prepare data for template - classify, split, and return
        Only includes sheets with data
        """
        # Split data by sheet type
        split_data = self.split_dataframe_by_sheet_type(df)
        
        # Filter out empty sheets for processing
        populated_sheets = {k: v for k, v in split_data.items() if not v.empty}
        
        return populated_sheets
