import re
from typing import Optional, Tuple


class ValidationService:
    
    @staticmethod
    def validate_gstin(gstin: str) -> Tuple[bool, Optional[str]]:
        """
        Validate GSTIN format
        Format: 2 digits (state code) + 10 characters (PAN) + 1 digit (entity number) + Z + 1 checksum
        Example: 27AAPFU0939F1Z5
        """
        if not gstin or not isinstance(gstin, str):
            return False, "GSTIN is required"
        
        gstin = gstin.strip().upper()
        
        # Check length
        if len(gstin) != 15:
            return False, "GSTIN must be 15 characters"
        
        # Validate format using regex
        pattern = r'^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[1-9A-Z]{1}Z[0-9A-Z]{1}$'
        if not re.match(pattern, gstin):
            return False, "Invalid GSTIN format"
        
        return True, None
    
    @staticmethod
    def validate_pan(pan: str) -> Tuple[bool, Optional[str]]:
        """
        Validate PAN format
        Format: 5 letters + 4 digits + 1 letter
        Example: AAPFU0939F
        """
        if not pan or not isinstance(pan, str):
            return False, "PAN is required"
        
        pan = pan.strip().upper()
        
        # Check length
        if len(pan) != 10:
            return False, "PAN must be 10 characters"
        
        # Validate format
        pattern = r'^[A-Z]{5}[0-9]{4}[A-Z]{1}$'
        if not re.match(pattern, pan):
            return False, "Invalid PAN format"
        
        return True, None
    
    @staticmethod
    def validate_invoice_number(invoice_no: str) -> Tuple[bool, Optional[str]]:
        """
        Validate invoice number (basic validation)
        """
        if not invoice_no or not isinstance(invoice_no, str):
            return False, "Invoice number is required"
        
        invoice_no = str(invoice_no).strip()
        
        if len(invoice_no) < 1 or len(invoice_no) > 50:
            return False, "Invoice number must be between 1 and 50 characters"
        
        return True, None
    
    @staticmethod
    def validate_amount(amount) -> Tuple[bool, Optional[str]]:
        """
        Validate amount (must be positive number)
        """
        try:
            amount_float = float(amount)
            if amount_float < 0:
                return False, "Amount must be positive"
            return True, None
        except (ValueError, TypeError):
            return False, "Invalid amount format"
    
    @staticmethod
    def validate_date(date_str: str) -> Tuple[bool, Optional[str]]:
        """
        Validate date format
        """
        if not date_str:
            return False, "Date is required"
        
        # Will be enhanced based on actual date formats in data
        return True, None
