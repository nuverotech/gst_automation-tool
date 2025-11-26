import os
import pandas as pd
from typing import Dict, List, Optional, Tuple
import re

from app.utils.logger import setup_logger

logger = setup_logger(__name__)


class ExcelParser:
    
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df: Optional[pd.DataFrame] = None
    
    def read_excel(self, sheet_name: Optional[str] = None) -> pd.DataFrame:
        """
        Read Excel file into DataFrame
        """
        try:
            engines = self._get_engine_priority()
            last_error: Optional[Exception] = None
            for engine in engines:
                try:
                    self.df = pd.read_excel(
                        self.file_path,
                        sheet_name=sheet_name or 0,
                        engine=engine
                    )
                    last_error = None
                    break
                except Exception as engine_error:
                    last_error = engine_error
                    logger.warning(
                        f"{engine} engine failed to read file '{self.file_path}': {engine_error}"
                    )
            if self.df is None:
                raise last_error or Exception("Unable to read Excel file")
            logger.info(f"Excel file read successfully. Shape: {self.df.shape}")
            return self.df
        
        except Exception as e:
            logger.error(f"Error reading Excel file: {str(e)}", exc_info=True)
            raise
    
    def detect_column_by_content(self, column_name: str) -> Optional[str]:
        """
        Detect column by analyzing content, not just headers
        Returns the actual column name if found
        """
        if self.df is None:
            return None
        
        # Mapping of target columns to detection patterns
        detection_patterns = {
            'gstin': self._is_gstin_column,
            'pan': self._is_pan_column,
            'invoice_number': self._is_invoice_column,
            'invoice_date': self._is_date_column,
            'amount': self._is_amount_column,
        }
        
        target_column = column_name.lower()
        
        if target_column in detection_patterns:
            detector = detection_patterns[target_column]
            
            for col in self.df.columns:
                # Sample first 10 non-null values
                sample = self.df[col].dropna().head(10)
                if len(sample) > 0 and detector(sample):
                    logger.info(f"Detected {column_name} in column: {col}")
                    return col
        
        return None
    
    def _is_gstin_column(self, series: pd.Series) -> bool:
        """
        Check if series contains GSTIN numbers
        """
        gstin_pattern = r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$'
        matches = series.astype(str).str.match(gstin_pattern)
        return matches.sum() / len(series) > 0.7  # 70% match threshold
    
    def _is_pan_column(self, series: pd.Series) -> bool:
        """
        Check if series contains PAN numbers
        """
        pan_pattern = r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$'
        matches = series.astype(str).str.match(pan_pattern)
        return matches.sum() / len(series) > 0.7
    
    def _is_invoice_column(self, series: pd.Series) -> bool:
        """
        Check if series contains invoice numbers
        """
        # Invoice numbers typically contain alphanumeric characters
        invoice_pattern = r'^[A-Z0-9\-/]+$'
        matches = series.astype(str).str.match(invoice_pattern)
        return matches.sum() / len(series) > 0.6
    
    def _is_date_column(self, series: pd.Series) -> bool:
        """
        Check if series contains dates
        """
        try:
            pd.to_datetime(series, errors='coerce')
            non_null = series.notna().sum()
            return non_null / len(series) > 0.7
        except Exception:
            return False
    
    def _is_amount_column(self, series: pd.Series) -> bool:
        """
        Check if series contains numeric amounts
        """
        try:
            numeric = pd.to_numeric(series, errors='coerce')
            non_null = numeric.notna().sum()
            return non_null / len(series) > 0.7
        except Exception:
            return False
    
    def map_columns(self, target_columns: Dict[str, str]) -> Dict[str, str]:
        """
        Map source columns to target columns
        
        Args:
            target_columns: Dict with target column names as keys
        
        Returns:
            Dict mapping source columns to target columns
        """
        mapping = {}
        
        for target_col, search_terms in target_columns.items():
            # First try header matching
            for col in self.df.columns:
                col_lower = str(col).lower()
                if any(term.lower() in col_lower for term in search_terms):
                    mapping[col] = target_col
                    break
            
            # If not found, try content detection
            if target_col not in mapping.values():
                detected_col = self.detect_column_by_content(target_col)
                if detected_col:
                    mapping[detected_col] = target_col
        
        logger.info(f"Column mapping: {mapping}")
        return mapping
    
    def get_sheet_names(self) -> List[str]:
        """
        Get all sheet names from Excel file
        """
        try:
            engines = self._get_engine_priority()
            last_error: Optional[Exception] = None
            for engine in engines:
                try:
                    xl_file = pd.ExcelFile(self.file_path, engine=engine)
                    break
                except Exception as engine_error:
                    last_error = engine_error
                    logger.warning(
                        f"{engine} engine failed to inspect sheets for '{self.file_path}': {engine_error}"
                    )
                    xl_file = None
            if xl_file is None:
                raise last_error or Exception("Unable to inspect Excel file")
            return xl_file.sheet_names
        except Exception as e:
            logger.error(f"Error getting sheet names: {str(e)}")
            return []

    def _get_engine_priority(self) -> List[str]:
        extension = os.path.splitext(self.file_path)[1].lower()
        if extension == '.xlsb':
            return ['pyxlsb']
        if extension == '.xls':
            return ['xlrd', 'openpyxl']
        # Default to modern Excel formats
        return ['openpyxl', 'xlrd']
