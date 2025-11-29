import re
from typing import Optional, Tuple


class ValidationService:
    VALID_STATE_CODES = {f"{code:02d}" for code in range(1, 39)} | {'97', '98', '99'}
    
    @classmethod
    def validate_gstin(cls, gstin: str) -> Tuple[bool, Optional[str]]:
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
        
        state_code = gstin[:2]
        if state_code not in cls.VALID_STATE_CODES:
            return False, "Invalid GST state code"
        
        if not cls._validate_gstin_checksum(gstin):
            return False, "GSTIN checksum failed"
        
        return True, None
    
    @staticmethod
    def _gstin_char_value(char: str) -> int:
        if char.isdigit():
            return int(char)
        return ord(char) - ord('A') + 10
    
    @classmethod
    def _validate_gstin_checksum(cls, gstin: str) -> bool:
        if len(gstin) != 15:
            return False
        data = gstin[:14]
        check_digit = gstin[-1]
        product = 0
        multiplier = 2
        for char in reversed(data):
            try:
                value = cls._gstin_char_value(char)
            except ValueError:
                return False
            product = (product + (value * multiplier)) % 11
            multiplier = (multiplier % 9) + 2
        calculated = (11 - product) % 11
        expected = str(calculated) if calculated < 10 else chr(ord('A') + (calculated - 10))
        return expected == check_digit
    
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
