import re
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field

import pandas as pd


from app.services.template_service import TemplateService
from app.services.validation_service import ValidationService
from app.utils.logger import setup_logger

logger = setup_logger(__name__)


@dataclass
class ValidationError:
    """Represents a validation error for a specific row and field"""
    row_number: int
    field_name: str
    error_type: str
    value_provided: Any
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        return {
            'row_number': self.row_number,
            'field_name': self.field_name,
            'error_type': self.error_type,
            'value_provided': str(self.value_provided),
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class ValidationWarning:
    """Represents a validation warning for data quality issues"""
    row_number: int
    field_name: str
    warning_type: str
    action_taken: str
    original_value: Any
    corrected_value: Any
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict:
        return {
            'row_number': self.row_number,
            'field_name': self.field_name,
            'warning_type': self.warning_type,
            'action_taken': self.action_taken,
            'original_value': str(self.original_value),
            'corrected_value': str(self.corrected_value),
            'timestamp': self.timestamp.isoformat()
        }


class ValidationTracker:
    """Tracks validation errors and warnings during processing"""
    
    def __init__(self):
        self.errors: List[ValidationError] = []
        self.warnings: List[ValidationWarning] = []
        self.processed_count = 0
        self.valid_count = 0
        self.skipped_count = 0
    
    def add_error(self, row_number: int, field_name: str, error_type: str, value: Any):
        """Add a validation error"""
        self.errors.append(ValidationError(
            row_number=row_number,
            field_name=field_name,
            error_type=error_type,
            value_provided=value
        ))
        logger.warning(f"Row {row_number}: {field_name} - {error_type}")
    
    def add_warning(self, row_number: int, field_name: str, warning_type: str, 
                    action: str, original: Any, corrected: Any):
        """Add a validation warning"""
        self.warnings.append(ValidationWarning(
            row_number=row_number,
            field_name=field_name,
            warning_type=warning_type,
            action_taken=action,
            original_value=original,
            corrected_value=corrected
        ))
        logger.info(f"Row {row_number}: {field_name} - {warning_type} (corrected)")
    
    def get_summary(self) -> Dict:
        """Get validation summary"""
        return {
            'processed_count': self.processed_count,
            'valid_count': self.valid_count,
            'skipped_count': self.skipped_count,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'errors': [e.to_dict() for e in self.errors],
            'warnings': [w.to_dict() for w in self.warnings]
        }



def normalize_label(value: str) -> str:
    return re.sub(r'[^a-z0-9]+', '', str(value).lower())


STATE_DATA = [
    ('JK', '01', 'Jammu & Kashmir', ['jammu and kashmir', 'jammu & kashmir', 'jk']),
    ('HP', '02', 'Himachal Pradesh', ['himachal pradesh', 'hp']),
    ('PB', '03', 'Punjab', ['pb']),
    ('CH', '04', 'Chandigarh', ['ch']),
    ('UT', '05', 'Uttarakhand', ['uttaranchal', 'uk', 'uttarakhand']),
    ('HR', '06', 'Haryana', ['hr']),
    ('DL', '07', 'Delhi', ['new delhi', 'dl']),
    ('RJ', '08', 'Rajasthan', ['rajasthan', 'rj']),
    ('UP', '09', 'Uttar Pradesh', ['uttar pradesh', 'up']),
    ('BR', '10', 'Bihar', ['bihar', 'br']),
    ('SK', '11', 'Sikkim', ['sikkim', 'sk']),
    ('AR', '12', 'Arunachal Pradesh', ['arunachal pradesh', 'ar']),
    ('NL', '13', 'Nagaland', ['nagaland', 'nl']),
    ('MN', '14', 'Manipur', ['manipur', 'mn']),
    ('MZ', '15', 'Mizoram', ['mizoram', 'mz']),
    ('TR', '16', 'Tripura', ['tripura', 'tr']),
    ('ML', '17', 'Meghalaya', ['meghalaya', 'ml']),
    ('AS', '18', 'Assam', ['assam', 'as']),
    ('WB', '19', 'West Bengal', ['west bengal', 'wb']),
    ('JH', '20', 'Jharkhand', ['jharkhand', 'jh']),
    ('OD', '21', 'Odisha', ['odisha', 'orissa', 'od']),
    ('CG', '22', 'Chhattisgarh', ['chhattisgarh', 'chattisgarh', 'cg']),
    ('MP', '23', 'Madhya Pradesh', ['madhya pradesh', 'mp']),
    ('GJ', '24', 'Gujarat', ['gujarat', 'gj']),
    ('DD', '25', 'Daman & Diu', ['daman and diu', 'dd']),
    ('DN', '26', 'Dadra & Nagar Haveli and Daman & Diu', ['dadra and nagar haveli', 'dnhdd', 'dn']),
    ('MH', '27', 'Maharashtra', ['maharashtra', 'mh']),
    ('AP', '37', 'Andhra Pradesh', ['andhra pradesh', 'ap']),
    ('KA', '29', 'Karnataka', ['karnataka', 'ka']),
    ('GA', '30', 'Goa', ['goa', 'ga']),
    ('LD', '31', 'Lakshadweep', ['lakshadweep', 'ld']),
    ('KL', '32', 'Kerala', ['kerala', 'kl']),
    ('TN', '33', 'Tamil Nadu', ['tamil nadu', 'tn']),
    ('PY', '34', 'Puducherry', ['puducherry', 'pondicherry', 'py']),
    ('AN', '35', 'Andaman & Nicobar Islands', ['andaman', 'andaman and nicobar', 'andaman & nicobar islands', 'an']),
    ('TS', '36', 'Telangana', ['telangana', 'ts']),
    ('LA', '38', 'Ladakh', ['ladakh', 'la']),
    ('OT', '97', 'Other Territory', ['other territory', 'ot']),
]

STATE_DETAILS = {code: {'code': numeric, 'name': name} for code, numeric, name, _ in STATE_DATA}
STATE_NAME_TO_CODE: Dict[str, str] = {}
STATE_NUMERIC_TO_CODE: Dict[str, str] = {}
for code, numeric, name, aliases in STATE_DATA:
    STATE_NAME_TO_CODE[normalize_label(name)] = code
    STATE_NAME_TO_CODE[normalize_label(code)] = code
    STATE_NUMERIC_TO_CODE[numeric] = code
    for alias in aliases:
        STATE_NAME_TO_CODE[normalize_label(alias)] = code
# Handle legacy Andhra Pradesh numeric code
STATE_NUMERIC_TO_CODE.setdefault('28', 'AP')


FIELD_KEYWORDS: Dict[str, List[str]] = {
    'gstin': ['customer gstin', 'customer gstn', 'gstin/uin', 'gstin', 'gstn'],
    'receiver_name': ['receiver name', 'trade name', 'customer name', 'name of recipient'],
    'customer_name': ['customer name', 'receiver name', 'trade name'],
    'invoice_number': ['invoice number', 'invoice no', 'invoice #', 'invoice id', 'document number'],
    'invoice_date': ['invoice date', 'date of invoice', 'invoice dt', 'document date'],
    'invoice_value': ['invoice value', 'value of invoice', 'invoice amount'],
    'place_of_supply': ['place of supply', 'pos'],
    'applicable_tax_rate': ['applicable % of tax rate', 'applicable tax rate', 'reduced rate'],
    'reverse_charge': ['reverse charge', 'rcm'],
    'invoice_type': ['invoice type'],
    'rate': ['gst%', 'tax rate', 'rate'],
    'taxable_value': ['taxable value', 'taxable amount'],
    'cess_amount': ['cess amount', 'cess'],
    'igst_amount': ['igst amount', 'igst', 'integrated tax'],
    'cgst_amount': ['cgst amount', 'cgst', 'central tax'],
    'sgst_amount': ['sgst amount', 'sgst', 'state tax'],
    'original_invoice_number': ['original invoice number', 'original invoice no', 'original inv no'],
    'original_invoice_date': ['original invoice date', 'original date'],
    'revised_invoice_number': ['revised invoice number', 'revised invoice no', 'new invoice number'],
    'revised_invoice_date': ['revised invoice date', 'revised date', 'new invoice date'],
    'amendment_reason': ['amendment reason', 'reason for amendment', 'revision reason'],
    'type': ['type (e/oe)', 'type e/oe', 'type'],
    'note_number': ['note number', 'note no', 'dr./ cr. note no', 'dr./ cr. no.'],
    'note_date': ['note date', 'dr./ cr. note date', 'dr./cr. date'],
    'note_type': ['note type', 'type of note', 'dr./ cr.'],
    'note_value': ['note value', 'dr./ cr. value'],
    'ur_type': ['ur type', 'supply type'],
    'export_type': ['type of export', 'export type', 'wpay/wopay'],
    'port_code': ['port code', 'port'],
    'shipping_bill_number': ['shipping bill number', 'shipping bill no', 'sb number', 'sb no'],
    'shipping_bill_date': ['shipping bill date', 'sb date', 'date of shipping bill'],
    'financial_year': ['financial year', 'fy', 'fiscal year'],
    'original_month': ['original month', 'month', 'tax month'],
    'gross_advance_received': ['gross advance received', 'gross advance', 'advance amount'],
    'gross_advance_adjusted': ['gross advance adjusted', 'adjusted advance', 'advance adjustment'],
    'taxable_advance': ['taxable advance', 'advance taxable'],
    'description': ['description', 'supply description', 'category'],
    'nil_rated_supplies': ['nil rated supplies', 'nil rated value', 'nil rated amount'],
    'exempted_supplies': ['exempted supplies', 'exempted value', 'exempted amount'],
    'non_gst_supplies': ['non gst supplies', 'non-gst supplies', 'non gst value'],
    'hsn_code': ['hsn code', 'hsn', 'hsn/sac'],
    'uqc': ['uqc', 'unit quantity code', 'unit'],
    'quantity': ['quantity', 'total quantity', 'qty'],
    'total_value': ['total value', 'total invoice value'],
    'nature_of_document': ['nature of document', 'document type', 'doc type'],
    'sr_no_from': ['sr. no from', 'sr no from', 'series from', 'from'],
    'sr_no_to': ['sr. no to', 'sr no to', 'series to', 'to'],
    'total_number': ['total number', 'total', 'total documents'],
    'cancelled': ['cancelled', 'cancelled documents', 'canceled'],
    'nature_of_supply': ['nature of supply', 'supply nature', 'eco supply type'],
    'eco_gstin': ['e-commerce operator gstin', 'eco gstin', 'ecommerce operator gstin'],
    'eco_name': ['e-commerce operator name', 'eco name', 'ecommerce operator name'],
    'net_value_of_supplies': ['net value of supplies', 'net value', 'supply value'],
    'supplier_gstin': ['gstin/uin of supplier', 'supplier gstin', 'supplier gstn'],
    'supplier_name': ['supplier name', 'name of supplier'],
    'recipient_gstin': ['gstin/uin of recipient', 'recipient gstin', 'buyer gstin'],
    'recipient_name': ['recipient name', 'name of recipient', 'buyer name'],
    'document_number': ['document number', 'doc number', 'doc no'],
    'document_date': ['document date', 'doc date'],
    'document_type': ['document type', 'doc type', 'supply type'],
    'value_of_supplies': ['value of supplies made', 'value of supplies', 'supply value'],
    'original_document_number': ['original document number', 'original doc number', 'original invoice number'],
    'original_document_date': ['original document date', 'original doc date', 'original invoice date'],
    'revised_document_number': ['revised document number', 'revised doc number', 'new document number'],
    'revised_document_date': ['revised document date', 'revised doc date', 'new document date'],
    'ecommerce_gstin': ['gstin of e-commerce', 'e-commerce gstin'],
}


DATA_COLUMN_KEYWORDS: Dict[str, List[str]] = {
    'gstin': ['customer gstin', 'customer gstn', 'recipient gstin', 'gstin'],
    'customer_name': ['customer name', 'receiver name', 'trade name', 'buyer name'],
    'invoice_number': ['invoice number', 'invoice no', 'invoice id', 'order id', 'document number'],
    'invoice_date': ['invoice date', 'date', 'document date'],
    'invoice_value': ['invoice value', 'gross sales after discount', 'invoice total', 'invoice amount'],
    'tax_total': ['tax total', 'total tax', 'tax amount', 'tax total amount'],
    'gross_amount': ['gross sales', 'gross value'],
    'mrp_value': ['mrp total', 'mrp value'],
    'taxable_value': ['taxable value', 'net sales', 'net sales amount', 'taxable amount'],
    'place_of_supply': ['place of supply', 'pos', 'customer state', 'shipping state', 'billing state'],
    'source_of_supply': ['source of supply', 'source state', 'state of supply'],
    'sales_channel': ['sales channel', 'channel'],
    'doc_type': ['doc type', 'document type'],
    'supply_type': ['supply type', 'transaction type', 'unique', 'unique type'],
    'reverse_charge': ['reverse charge', 'rcm', 'reverse charge applicable'],
    'applicable_tax_rate': ['applicable % of tax rate', 'applicable tax rate', 'reduced rate', 'special rate'],
    'amendment_flag': ['amendment', 'is amendment', 'amended'],
    'original_invoice_number': ['original invoice number', 'original invoice no', 'original inv no'],
    'original_invoice_date': ['original invoice date', 'original date'],
    'revised_invoice_number': ['revised invoice number', 'revised invoice no', 'new invoice number'],
    'revised_invoice_date': ['revised invoice date', 'revised date', 'new invoice date'],
    'amendment_reason': ['amendment reason', 'reason for amendment', 'revision reason'],
    'note_number': ['cn number', 'dn number', 'note number', 'credit note number', 'debit note number', 'dr./ cr. no.'],
    'note_date': ['note date', 'cn date', 'dn date', 'credit note date', 'debit note date', 'dr./ cr. date'],
    'note_value': ['note value', 'credit amount', 'debit amount', 'dr./ cr. note value', 'dr./ cr. value', 'gross sales after discount'],
    'igst_rate': ['igst tax%', 'igst%', 'igst rate'],
    'cgst_rate': ['cgst tax%', 'cgst%', 'cgst rate'],
    'sgst_rate': ['sgst tax%', 'sgst%', 'sgst rate'],
    'rate': ['gst%', 'tax rate', 'rate'],
    'igst_amount': ['igst amount'],
    'cgst_amount': ['cgst amount'],
    'sgst_amount': ['sgst amount'],
    'cess_amount': ['cess amount', 'cess'],
    'ecommerce_gstin': ['e-commerce gstin', 'ecommerce gstin', 'eco gstin'],
    'unique_type': ['unique', 'transaction type'],
    'export_flag': ['export'],
    'export_type': ['export type', 'type of export', 'wpay/wopay'],
    'port_code': ['port code', 'port', 'export port'],
    'shipping_bill_number': ['shipping bill number', 'shipping bill no', 'sb number', 'sb no'],
    'shipping_bill_date': ['shipping bill date', 'sb date', 'shipping date'],
    'financial_year': ['financial year', 'fy', 'fiscal year'],
    'original_month': ['original month', 'month', 'tax month'],
    'supply_category': ['supply category', 'category', 'exemption type'],
    'nil_rated_supplies': ['nil rated supplies', 'nil rated value'],
    'exempted_supplies': ['exempted supplies', 'exempted value'],
    'non_gst_supplies': ['non gst supplies', 'non-gst supplies', 'non gst value'],
}


class SheetMapper:
    SUPPORTED_SHEETS = ('b2b', 'b2ba', 'b2cl', 'b2cla', 'b2cs', 'b2csa', 'cdnr', 'cdnra', 'cdnur', 'cdnura', 'export', 'expa', 'at', 'ata', 'atadj', 'atadja', 'exemp', 'hsn', 'hsnb2c', 'docs', 'eco', 'ecoa', 'ecob2b', 'ecourp2b', 'ecob2c', 'ecourp2c', 'ecoab2b', 'ecoaurp2b', 'ecoab2c', 'ecoaurp2c')
    VALID_GST_RATES = [0, 0.25, 0.1, 3, 5, 12, 18, 28]  # Valid GST rates in India
    VALID_CESS_RATES = [0, 1, 3, 5, 12, 13, 15, 20, 22, 25, 28, 50, 204, 290]
    
    def __init__(self, template_service: Optional[TemplateService] = None):
        self.template_service = template_service or TemplateService()
        self.validation_service = ValidationService()
        self.template_structure = self.template_service.load_template_structure()
        self.column_map: Dict[str, Optional[str]] = {}
        self.validation_tracker = ValidationTracker()
        self.seen_invoice_keys: set = set()  # For duplicate detection
        
        self.sheet_mapping = self._build_sheet_mapping()
        self.template_field_headers = self._build_template_field_headers()
        
        logger.info("Template sheet mapping: %s", self.sheet_mapping)
    
    def prepare_data_for_template(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        if df.empty:
            return {}
        
        # Reset validation tracker for new processing
        self.validation_tracker = ValidationTracker()
        self.seen_invoice_keys = set()
        self.b2cl_invoice_keys = set()  # Track B2CL invoices to prevent double aggregation
        self.b2cs_invoice_keys = set()  # Track B2CS invoices
        self.cdnr_note_keys = set()  # Track CDNR notes for CDNRA cross-reference
        self.cdnur_note_keys = set()  # Track CDNUR notes for CDNURA cross-reference
        self.export_invoice_keys = set()  # Track EXP invoices for EXPA cross-reference
        
        working_df = self._augment_dataframe(df)
        
        # Log classification counts after augmentation
        logger.info("=" * 80)
        logger.info("DATA CLASSIFICATION AFTER AUGMENTATION:")
        logger.info(f"Total rows: {len(working_df)}")
        logger.info(f"Has valid GSTIN (_has_valid_gstin): {working_df['_has_valid_gstin'].sum()}")
        logger.info(f"Is large B2CL (_is_large_b2cl): {working_df['_is_large_b2cl'].sum()}")
        logger.info(f"Is interstate (_is_interstate): {working_df['_is_interstate'].sum()}")
        logger.info(f"Is credit/debit note (_is_credit_or_debit): {working_df['_is_credit_or_debit'].sum()}")
        logger.info(f"Is export (_is_export): {working_df['_is_export'].sum()}")
        logger.info(f"Is amendment (_is_amendment): {working_df['_is_amendment'].sum()}")
        
        # Log first 5 rows for debugging
        logger.info("SAMPLE ROWS (First 5):")
        for i in range(min(5, len(working_df))):
            row = working_df.iloc[i]
            logger.info(f"  Row {i+2}: GSTIN={row.get('_has_valid_gstin')}, "
                       f"B2CL={row.get('_is_large_b2cl')}, "
                       f"Interstate={row.get('_is_interstate')}, "
                       f"CN/DN={row.get('_is_credit_or_debit')}, "
                       f"Export={row.get('_is_export')}, "
                       f"Amend={row.get('_is_amendment')}, "
                       f"Invoice#={row.get('_invoice_number', 'N/A')}")
        logger.info("=" * 80)
        
        populated: Dict[str, pd.DataFrame] = {}
        
        for builder in (
            self._build_b2b,
            self._build_b2ba,
            self._build_b2cl,
            self._build_b2cla,
            self._build_b2cs,
            self._build_b2csa,
            self._build_cdnr,
            self._build_cdnra,
            self._build_cdnur,
            self._build_cdnura,
            self._build_export,
            self._build_expa,
            self._build_at,
            self._build_ata,
            self._build_atadj,
            self._build_atadja,
            self._build_exemp,
            self._build_hsn,
            self._build_hsnb2c,
            self._build_docs,
            self._build_eco,
            self._build_ecoa,
            self._build_ecob2b,
            self._build_ecourp2b,
            self._build_ecob2c,
            self._build_ecourp2c,
            self._build_ecoab2b,
            self._build_ecoaurp2b,
            self._build_ecoab2c,
            self._build_ecoaurp2c,
        ):
            sheet_name, sheet_df = builder(working_df)
            if sheet_name and not sheet_df.empty:
                populated[sheet_name] = sheet_df
        
        # Log validation summary
        summary = self.validation_tracker.get_summary()
        logger.info(
            "Prepared sheets: %s | Validation: %d processed, %d valid, %d skipped, %d errors, %d warnings",
            {sheet: len(df_sheet) for sheet, df_sheet in populated.items()},
            summary['processed_count'],
            summary['valid_count'],
            summary['skipped_count'],
            summary['error_count'],
            summary['warning_count']
        )
        
        return populated
    
    def get_validation_summary(self) -> Dict:
        """Get the validation summary with errors and warnings"""
        return self.validation_tracker.get_summary()
    
    # ------------------------------------------------------------------
    # Data preparation helpers
    # ------------------------------------------------------------------
    def _augment_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        enriched = df.copy()
        self.column_map = self._resolve_source_columns(df)
        
        enriched['_gstin'] = enriched.apply(
            lambda row: self._clean_gstin_value(self._get_value(row, 'gstin')), axis=1
        )
        enriched['_has_valid_gstin'] = enriched['_gstin'].apply(self._is_valid_gstin)
        
        enriched['_invoice_number'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'invoice_number')), axis=1
        )
        enriched['_invoice_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'invoice_date')), axis=1
        )
        
        enriched['_tax_total'] = enriched.apply(self._extract_tax_total, axis=1)
        enriched['_invoice_value'] = enriched.apply(self._resolve_invoice_value, axis=1)
        enriched['_taxable_value'] = enriched.apply(
            lambda row: self._resolve_taxable_value(row, row['_invoice_value']), axis=1
        )
        enriched['_rate'] = enriched.apply(self._resolve_rate, axis=1)
        enriched['_cess_amount'] = enriched.apply(
            lambda row: self._resolve_cess_amount(row), axis=1
        )
        
        enriched['_receiver_name'] = enriched.apply(
            lambda row: self._truncate(self._safe_string(self._get_value(row, 'customer_name')), 255),
            axis=1
        )
        enriched['_ecommerce_gstin'] = enriched.apply(
            lambda row: self._clean_gstin_value(self._get_value(row, 'ecommerce_gstin')), axis=1
        )
        enriched['_type_flag'] = enriched['_ecommerce_gstin'].apply(lambda val: 'E' if val else 'OE')
        enriched['_supply_text'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'supply_type') or self._get_value(row, 'unique_type')),
            axis=1
        )
        enriched['_is_sez'] = enriched['_supply_text'].apply(self._detect_sez)
        enriched['_invoice_type'] = enriched.apply(
            lambda row: self._determine_invoice_type(row['_is_sez'], row['_supply_text']),
            axis=1
        )
        
        enriched['_pos_code'] = enriched.apply(
            lambda row: self._state_code_from_value(self._get_value(row, 'place_of_supply')), axis=1
        )
        enriched['_source_state_code'] = enriched.apply(self._resolve_source_state_code, axis=1)
        enriched['_is_interstate'] = enriched.apply(
            lambda row: bool(row['_pos_code'] and row['_source_state_code'] and row['_pos_code'] != row['_source_state_code']),
            axis=1
        )
        enriched['_is_large_b2cl'] = enriched.apply(
            lambda row: self._is_large_b2cl(row['_invoice_value'], row['_is_interstate'], row['_invoice_date']),
            axis=1
        )
        # UR Type is ONLY for CDNUR (credit/debit notes for unregistered)
        # Valid values: B2CL, EXPWP, EXPWOP (NOT B2CS)
        enriched['_ur_type'] = None  # Will be set later for CDNUR entries only
        
        enriched['_doc_type'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'doc_type') or self._get_value(row, 'unique_type')),
            axis=1
        )
        enriched['_note_number'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'note_number')) or row['_invoice_number'],
            axis=1
        )
        enriched['_note_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'note_date')) or row['_invoice_date'],
            axis=1
        )
        enriched['_note_value'] = enriched.apply(self._resolve_note_value, axis=1)
        enriched['_note_type'] = enriched.apply(
            lambda row: self._determine_note_type(row['_doc_type'], row['_supply_text'], row['_note_value']),
            axis=1
        )
        enriched['_is_credit_or_debit'] = enriched.apply(
            lambda row: self._is_credit_or_debit(row['_doc_type'], row['_supply_text']) or bool(row['_note_type']),
            axis=1
        )
        
        enriched['_is_export'] = enriched.apply(self._detect_export, axis=1)
        enriched['_export_type'] = enriched.apply(self._resolve_export_type, axis=1)
        enriched['_port_code'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'port_code')), axis=1
        )
        enriched['_shipping_bill_number'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'shipping_bill_number')), axis=1
        )
        enriched['_shipping_bill_date'] = enriched.apply(
            lambda row: self._get_value(row, 'shipping_bill_date'), axis=1
        )
        
        # Add reverse charge handling
        enriched['_reverse_charge'] = enriched.apply(
            lambda row: self._normalize_reverse_charge(self._get_value(row, 'reverse_charge')), axis=1
        )
        
        # Add 65% applicable tax rate support
        enriched['_applicable_tax_rate'] = enriched.apply(
            lambda row: self._parse_applicable_tax_rate(self._get_value(row, 'applicable_tax_rate')), axis=1
        )
        
        # Calculate tax amounts (IGST/CGST/SGST) based on interstate flag and reverse charge
        enriched['_igst_amount'] = enriched.apply(lambda row: self._calculate_igst(row), axis=1)
        enriched['_cgst_amount'] = enriched.apply(lambda row: self._calculate_cgst(row), axis=1)
        enriched['_sgst_amount'] = enriched.apply(lambda row: self._calculate_sgst(row), axis=1)
        
        # Add amendment detection and fields
        enriched['_is_amendment'] = enriched.apply(self._detect_amendment, axis=1)
        enriched['_original_invoice_number'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'original_invoice_number')), axis=1
        )
        enriched['_original_invoice_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'original_invoice_date')), axis=1
        )
        enriched['_revised_invoice_number'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'revised_invoice_number')), axis=1
        )
        enriched['_revised_invoice_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'revised_invoice_date')), axis=1
        )
        enriched['_amendment_reason'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'amendment_reason')), axis=1
        )
        
        # Add row number for error tracking
        enriched['_row_number'] = range(1, len(enriched) + 1)
        
        return enriched
    
    def _resolve_source_columns(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        column_map: Dict[str, Optional[str]] = {}
        for field, keywords in DATA_COLUMN_KEYWORDS.items():
            column_map[field] = self._match_column(df.columns, keywords)
        logger.info("Source column mapping: %s", column_map)
        return column_map
    
    def _build_sheet_mapping(self) -> Dict[str, str]:
        base: Dict[str, str] = {}
        fallback: Dict[str, str] = {}
        for sheet_name in self.template_structure.keys():
            canonical = self._canonical_sheet_key(sheet_name)
            if not canonical:
                continue
            if self._is_amendment_sheet(sheet_name):
                fallback.setdefault(canonical, sheet_name)
                continue
            base.setdefault(canonical, sheet_name)
        for canonical, sheet_name in fallback.items():
            base.setdefault(canonical, sheet_name)
        return base
    
    def _build_template_field_headers(self) -> Dict[str, Dict[str, str]]:
        mapping: Dict[str, Dict[str, str]] = {}
        for canonical, sheet_name in self.sheet_mapping.items():
            headers = self.template_structure.get(sheet_name, {}).get('headers', [])
            header_map: Dict[str, str] = {}
            for header in headers:
                normalized_header = normalize_label(header)
                for field_key, keywords in FIELD_KEYWORDS.items():
                    if field_key in header_map:
                        continue
                    if self._header_matches(normalized_header, field_key, keywords):
                        header_map[field_key] = header
            mapping[canonical] = header_map
        return mapping
    
    # ------------------------------------------------------------------
    # Sheet builders
    # ------------------------------------------------------------------
    def _build_b2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('b2b')
        if not sheet_name:
            return None, pd.DataFrame()
        
        mask = (
            df['_has_valid_gstin']
            & (~df['_is_credit_or_debit'])
            & (~df['_is_export'])
            & (~df['_is_amendment'])
        )
        if not mask.any():
            logger.info("B2B: No rows match criteria (has_valid_gstin AND not CN/DN AND not export AND not amendment)")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"B2B: Processing {mask.sum()} rows with valid GSTIN (non-CN/DN, non-export, non-amendment)")
        
        rows: List[Dict[str, object]] = []
        row_count = 0
        for _, row in df[mask].iterrows():
            row_count += 1
            row_number = row.get('_row_number', 0)
            
            # Perform all validations
            is_valid = True
            
            # Log first 5 rows for debugging
            if row_count <= 5:
                logger.info(f"B2B Row {row_count}: GSTIN={row.get('_gstin', 'N/A')[:10]}..., "
                           f"Invoice={row.get('_invoice_number', 'N/A')}, "
                           f"Value={row.get('_invoice_value', 'N/A')}, "
                           f"Rate={row.get('_rate', 'N/A')}")
            
            # Validate invoice number
            if not self._validate_invoice_number(row['_invoice_number'], row_number):
                is_valid = False
            
            # Validate invoice value
            if not self._validate_amount_not_zero_negative(row['_invoice_value'], 'Invoice Value', row_number):
                is_valid = False
            
            # Validate taxable value
            if not self._validate_amount_not_zero_negative(row['_taxable_value'], 'Taxable Value', row_number):
                is_valid = False
            
            # Validate tax rate
            if not self._validate_tax_rate(row['_rate'], row['_invoice_type'], row_number):
                is_valid = False
            
            # Validate date
            if not self._validate_date_range(row['_invoice_date'], row_number):
                is_valid = False
            
            # Check for duplicates
            if not self._check_duplicate_invoice(
                row['_gstin'], 
                row['_invoice_number'], 
                row['_invoice_date'], 
                row_number
            ):
                is_valid = False
            
            # Validate cess if present
            cess_amount = row['_cess_amount'] if row['_cess_amount'] is not None else 0
            if cess_amount != 0 and row['_taxable_value']:
                self._validate_cess_rate(cess_amount, row['_taxable_value'], row_number)
            
            # Validate receiver name
            validated_name = self._validate_receiver_name(row['_receiver_name'], row_number)
            
            # Skip row if critical validations failed
            if not is_valid:
                self.validation_tracker.skipped_count += 1
                continue
            
            self.validation_tracker.processed_count += 1
            self.validation_tracker.valid_count += 1
            
            # Build payload with all fields including calculated tax amounts
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2b', 'gstin', row['_gstin'])
            self._set_field(payload, 'b2b', 'receiver_name', validated_name)
            self._set_field(payload, 'b2b', 'invoice_number', row['_invoice_number'].upper())
            self._set_field(payload, 'b2b', 'invoice_date', row['_invoice_date'])
            self._set_field(payload, 'b2b', 'invoice_value', self._round_money(row['_invoice_value']))
            self._set_field(payload, 'b2b', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'b2b', 'applicable_tax_rate', row['_applicable_tax_rate'])
            self._set_field(payload, 'b2b', 'reverse_charge', row['_reverse_charge'])
            self._set_field(payload, 'b2b', 'invoice_type', row['_invoice_type'])
            self._set_field(payload, 'b2b', 'ecommerce_gstin', row['_ecommerce_gstin'])
            self._set_field(payload, 'b2b', 'rate', row['_rate'])
            self._set_field(payload, 'b2b', 'taxable_value', self._round_money(row['_taxable_value']))
            self._set_field(payload, 'b2b', 'cess_amount', self._round_money(cess_amount))
            
            # Add calculated tax amounts
            self._set_field(payload, 'b2b', 'igst_amount', self._round_money(row['_igst_amount']))
            self._set_field(payload, 'b2b', 'cgst_amount', self._round_money(row['_cgst_amount']))
            self._set_field(payload, 'b2b', 'sgst_amount', self._round_money(row['_sgst_amount']))
            
            if payload:
                rows.append(payload)
        
        logger.info(f"B2B: Processed {self.validation_tracker.processed_count}, " 
                   f"Valid {self.validation_tracker.valid_count}, "
                   f"Skipped {self.validation_tracker.skipped_count}")
        
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_b2ba(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build B2BA (B2B Amendments) sheet with complete validation per spec:
        - Original invoice must exist in B2B
        - Revised date >= Original date
        - POS and Invoice Type cannot change
        - Cross-period amendments allowed with specific conditions
        """
        sheet_name = self.sheet_mapping.get('b2ba')
        if not sheet_name:
            # If no B2BA sheet in template, skip
            return None, pd.DataFrame()
        
        mask = (
            df['_has_valid_gstin']
            & df['_is_amendment']
            & (~df['_is_export'])
        )
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        
        for _, row in df[mask].iterrows():
            row_number = row.get('_row_number', 0)
            is_valid = True
            
            # Amendment-specific validations
            
            # 1. Validate original invoice number exists
            if not row['_original_invoice_number']:
                self.validation_tracker.add_error(
                    row_number, 'Original Invoice Number',
                    'Original invoice number required for amendment',
                    row['_original_invoice_number']
                )
                is_valid = False
            
            # 2. Validate original invoice date exists
            if not row['_original_invoice_date']:
                self.validation_tracker.add_error(
                    row_number, 'Original Invoice Date',
                    'Original invoice date required for amendment',
                    row['_original_invoice_date']
                )
                is_valid = False
            
            # 3. Validate revised invoice number exists
            revised_inv_no = row['_revised_invoice_number'] if row['_revised_invoice_number'] else row['_invoice_number']
            if not revised_inv_no:
                self.validation_tracker.add_error(
                    row_number, 'Revised Invoice Number',
                    'Revised invoice number required for amendment',
                    revised_inv_no
                )
                is_valid = False
            
            # 4. Validate revised invoice date exists
            revised_inv_date = row['_revised_invoice_date'] if row['_revised_invoice_date'] else row['_invoice_date']
            if not revised_inv_date:
                self.validation_tracker.add_error(
                    row_number, 'Revised Invoice Date',
                    'Revised invoice date required for amendment',
                    revised_inv_date
                )
                is_valid = False
            
            # 5. Validate date order (revised >= original)
            if row['_original_invoice_date'] and revised_inv_date:
                if revised_inv_date < row['_original_invoice_date']:
                    self.validation_tracker.add_error(
                        row_number, 'Revised Invoice Date',
                        'Revised date cannot be before original date',
                        f"Original: {row['_original_invoice_date']}, Revised: {revised_inv_date}"
                    )
                    is_valid = False
            
            # Standard validations (same as B2B)
            if revised_inv_no:
                if not self._validate_invoice_number(revised_inv_no, row_number):
                    is_valid = False
            
            if row['_original_invoice_number']:
                if not self._validate_invoice_number(row['_original_invoice_number'], row_number):
                    is_valid = False
            
            if not self._validate_amount_not_zero_negative(row['_invoice_value'], 'Invoice Value', row_number):
                is_valid = False
            
            if not self._validate_amount_not_zero_negative(row['_taxable_value'], 'Taxable Value', row_number):
                is_valid = False
            
            if not self._validate_tax_rate(row['_rate'], row['_invoice_type'], row_number):
                is_valid = False
            
            if row['_original_invoice_date']:
                if not self._validate_date_range(row['_original_invoice_date'], row_number):
                    is_valid = False
            
            if revised_inv_date:
                if not self._validate_date_range(revised_inv_date, row_number):
                    is_valid = False
            
            # Validate cess if present
            cess_amount = row['_cess_amount'] if row['_cess_amount'] is not None else 0
            if cess_amount != 0 and row['_taxable_value']:
                self._validate_cess_rate(cess_amount, row['_taxable_value'], row_number)
            
            # Validate receiver name
            validated_name = self._validate_receiver_name(row['_receiver_name'], row_number)
            
            # Skip if validations failed
            if not is_valid:
                self.validation_tracker.skipped_count += 1
                continue
            
            self.validation_tracker.processed_count += 1
            self.validation_tracker.valid_count += 1
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2ba', 'gstin', row['_gstin'])
            self._set_field(payload, 'b2ba', 'receiver_name', validated_name)
            self._set_field(payload, 'b2ba', 'original_invoice_number', row['_original_invoice_number'].upper() if row['_original_invoice_number'] else '')
            self._set_field(payload, 'b2ba', 'original_invoice_date', row['_original_invoice_date'])
            self._set_field(payload, 'b2ba', 'revised_invoice_number', revised_inv_no.upper() if revised_inv_no else '')
            self._set_field(payload, 'b2ba', 'revised_invoice_date', revised_inv_date)
            self._set_field(payload, 'b2ba', 'invoice_value', self._round_money(row['_invoice_value']))
            self._set_field(payload, 'b2ba', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'b2ba', 'reverse_charge', row['_reverse_charge'])
            self._set_field(payload, 'b2ba', 'invoice_type', row['_invoice_type'])
            self._set_field(payload, 'b2ba', 'ecommerce_gstin', row['_ecommerce_gstin'])
            self._set_field(payload, 'b2ba', 'rate', row['_rate'])
            self._set_field(payload, 'b2ba', 'taxable_value', self._round_money(row['_taxable_value']))
            self._set_field(payload, 'b2ba', 'cess_amount', self._round_money(cess_amount))
            
            # Add calculated tax amounts
            self._set_field(payload, 'b2ba', 'igst_amount', self._round_money(row['_igst_amount']))
            self._set_field(payload, 'b2ba', 'cgst_amount', self._round_money(row['_cgst_amount']))
            self._set_field(payload, 'b2ba', 'sgst_amount', self._round_money(row['_sgst_amount']))
            
            # Add amendment reason if available
            if row['_amendment_reason']:
                self._set_field(payload, 'b2ba', 'amendment_reason', row['_amendment_reason'])
            
            if payload:
                rows.append(payload)
        
        logger.info(f"B2BA: Processed {len(rows)} amendments")
        
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_b2cl(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build B2CL sheet with complete validation per spec:
        - Unregistered recipients only
        - Interstate transactions only
        - Value >= threshold (₹100,000 from Aug 2024, ₹250,000 before)
        - IGST only (no CGST/SGST)
        - No reverse charge
        - No aggregation (separate rows)
        """
        sheet_name = self.sheet_mapping.get('b2cl')
        if not sheet_name:
            return None, pd.DataFrame()
        
        mask = (
            (~df['_has_valid_gstin'])
            & df['_is_large_b2cl']
            & (~df['_is_credit_or_debit'])
            & (~df['_is_export'])
            & (~df['_is_amendment'])
        )
        if not mask.any():
            logger.info("B2CL: No rows match criteria (no GSTIN AND large B2CL AND not CN/DN AND not export AND not amendment)")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"B2CL: Processing {mask.sum()} large B2C invoices (>=₹2.5L, interstate)")
        
        rows: List[Dict[str, object]] = []
        subset = df[mask]
        
        # Log first 5 rows for debugging
        for i, (idx, row) in enumerate(subset.head(5).iterrows()):
            logger.info(f"B2CL Row {i+1}: Invoice={row.get('_invoice_number', 'N/A')}, "
                       f"Value={row.get('_invoice_value', 'N/A')}, "
                       f"POS={row.get('_pos_code', 'N/A')}, "
                       f"Rate={row.get('_rate', 'N/A')}")
        
        for _, row in subset.iterrows():
            row_number = row.get('_row_number', 0)
            is_valid = True
            
            # B2CL-specific validations
            
            # 1. Validate invoice value meets threshold
            threshold = 250000
            if row['_invoice_date'] and row['_invoice_date'] >= date(2024, 8, 1):
                threshold = 100000
            
            if row['_invoice_value'] is None or abs(row['_invoice_value']) < threshold:
                self.validation_tracker.add_error(
                    row_number, 'Invoice Value',
                    f'Invoice value below B2CL threshold (₹{threshold})',
                    row['_invoice_value']
                )
                is_valid = False
            
            # 2. Validate interstate requirement (critical for B2CL)
            if not row['_is_interstate']:
                self.validation_tracker.add_error(
                    row_number, 'Place of Supply',
                    'B2CL requires interstate transaction (POS different from source state)',
                    row['_pos_code']
                )
                is_valid = False
            
            # 3. Validate no reverse charge for B2C
            if row['_reverse_charge'] == 'Y':
                self.validation_tracker.add_error(
                    row_number, 'Reverse Charge',
                    'Reverse charge not applicable for B2C supplies',
                    'Y'
                )
                is_valid = False
            
            # 4. Validate GSTIN is NOT valid (unregistered recipient)
            if row['_has_valid_gstin']:
                self.validation_tracker.add_warning(
                    row_number, 'GSTIN',
                    'Valid GSTIN found for B2CL (should be B2B)',
                    'Reclassify to B2B', row['_gstin'], 'B2B'
                )
                is_valid = False
            
            # 5. Validate POS is present
            if not row['_pos_code']:
                self.validation_tracker.add_error(
                    row_number, 'Place of Supply',
                    'POS mandatory for B2CL',
                    row['_pos_code']
                )
                is_valid = False
            
            # Standard validations (same as B2B)
            if not self._validate_invoice_number(row['_invoice_number'], row_number):
                is_valid = False
            
            if not self._validate_amount_not_zero_negative(row['_invoice_value'], 'Invoice Value', row_number):
                is_valid = False
            
            if not self._validate_amount_not_zero_negative(row['_taxable_value'], 'Taxable Value', row_number):
                is_valid = False
            
            if not self._validate_tax_rate(row['_rate'], 'Regular', row_number):
                is_valid = False
            
            if not self._validate_date_range(row['_invoice_date'], row_number):
                is_valid = False
            
            # Validate cess if present
            cess_amount = row['_cess_amount'] if row['_cess_amount'] is not None else 0
            if cess_amount != 0 and row['_taxable_value']:
                self._validate_cess_rate(cess_amount, row['_taxable_value'], row_number)
            
            # Validate customer name
            validated_name = self._validate_receiver_name(
                row['_receiver_name'] if row['_receiver_name'] else 'Unregistered Recipient',
                row_number
            )
            
            # Skip if validations failed
            if not is_valid:
                self.validation_tracker.skipped_count += 1
                continue
            
            self.validation_tracker.processed_count += 1
            self.validation_tracker.valid_count += 1
            
            # Track this invoice in B2CL to prevent double aggregation in B2CS
            invoice_key = f"{row['_invoice_number']}_{row['_invoice_date']}"
            self.b2cl_invoice_keys.add(invoice_key)
            
            # Build payload - B2CL uses IGST only (interstate)
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2cl', 'customer_name', validated_name)
            self._set_field(payload, 'b2cl', 'invoice_number', row['_invoice_number'].upper())
            self._set_field(payload, 'b2cl', 'invoice_date', row['_invoice_date'])
            self._set_field(payload, 'b2cl', 'invoice_value', self._round_money(row['_invoice_value']))
            self._set_field(payload, 'b2cl', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'b2cl', 'rate', row['_rate'])
            self._set_field(payload, 'b2cl', 'taxable_value', self._round_money(row['_taxable_value']))
            self._set_field(payload, 'b2cl', 'ecommerce_gstin', row['_ecommerce_gstin'])
            self._set_field(payload, 'b2cl', 'cess_amount', self._round_money(cess_amount))
            
            # B2CL is always interstate, so only IGST (CGST/SGST = 0)
            self._set_field(payload, 'b2cl', 'igst_amount', self._round_money(row['_igst_amount']))
            
            if payload:
                rows.append(payload)
        
        logger.info(f"B2CL: Processed {self.validation_tracker.processed_count}, "
                   f"Valid {self.validation_tracker.valid_count}, "
                   f"Skipped {self.validation_tracker.skipped_count}")
        
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_b2cs(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build B2CS sheet with comprehensive aggregation logic.
        B2CS = B2C Small transactions that need to be aggregated
        """
        sheet_name = self.sheet_mapping.get('b2cs')
        if not sheet_name:
            return None, pd.DataFrame()
        
        # Classification: B2C without valid GSTIN, small invoices, not credit/debit/export
        # AND not already in B2CL
        mask = (
            (~df['_has_valid_gstin'])
            & (~df['_is_large_b2cl'])
            & (~df['_is_credit_or_debit'])
            & (~df['_is_export'])
            & (~df['_is_amendment'])  # Regular invoices only, not amendments
        )
        subset = df[mask].copy()
        if subset.empty:
            logger.info("B2CS: No rows match criteria (no GSTIN AND small B2C AND not CN/DN AND not export AND not amendment)")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"B2CS: Processing {len(subset)} rows for aggregation (small B2C, aggregated by Type+POS+Rate)")
        
        # Log first 5 rows for debugging
        for i, (idx, row) in enumerate(subset.head(5).iterrows()):
            logger.info(f"B2CS Row {i+1}: Invoice={row.get('_invoice_number', 'N/A')}, "
                       f"Value={row.get('_invoice_value', 'N/A')}, "
                       f"POS={row.get('_pos_code', 'N/A')}, "
                       f"Rate={row.get('_rate', 'N/A')}")
        
        # Prepare aggregation data structures
        aggregation_table = {}
        
        for idx, row in subset.iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2  # Excel row (header + 0-index)
            
            # Step 1: Validate Place of Supply
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(row_num, 'Place of Supply', 'Missing or invalid', pos_code)
                self.validation_tracker.skipped_count += 1
                continue
            
            pos_display = self._format_place_of_supply(pos_code)
            
            # Step 2: Validate Rate
            rate = row.get('_rate')
            if rate is None or pd.isna(rate):
                self.validation_tracker.add_error(row_num, 'Rate', 'Missing', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
                if rate not in self.VALID_GST_RATES:
                    self.validation_tracker.add_warning(
                        row_num, 'Rate', f'Unusual GST rate: {rate}%',
                        'Accepted', rate, rate
                    )
            except (ValueError, TypeError):
                self.validation_tracker.add_error(row_num, 'Rate', 'Invalid format', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            # Step 3: Determine Type (E or OE) and validate E-Commerce GSTIN
            ecommerce_gstin = row.get('_ecommerce_gstin')
            type_flag = 'OE'
            eco_gstin_clean = ''
            
            if ecommerce_gstin and not pd.isna(ecommerce_gstin):
                eco_gstin_clean = str(ecommerce_gstin).strip().upper()
                is_valid, error = self.validation_service.validate_gstin(eco_gstin_clean)
                if is_valid:
                    type_flag = 'E'
                else:
                    self.validation_tracker.add_warning(
                        row_num, 'E-Commerce GSTIN', f'Invalid GSTIN: {error}',
                        'Treated as OE type', eco_gstin_clean, 'Treated as OE'
                    )
                    eco_gstin_clean = ''
                    type_flag = 'OE'
            
            # Step 4: Get Applicable Tax Rate (65% reduction flag)
            applicable_tax_rate = row.get('_applicable_tax_rate', '')
            if applicable_tax_rate and not pd.isna(applicable_tax_rate):
                applicable_tax_rate = str(applicable_tax_rate).strip()
            else:
                applicable_tax_rate = ''
            
            # Step 5: Validate amounts
            taxable_value = row.get('_taxable_value', 0)
            if taxable_value is None or pd.isna(taxable_value):
                taxable_value = 0
            else:
                try:
                    taxable_value = float(taxable_value)
                    if taxable_value < 0:
                        self.validation_tracker.add_error(row_num, 'Taxable Value', 'Negative value', taxable_value)
                        self.validation_tracker.skipped_count += 1
                        continue
                except (ValueError, TypeError):
                    self.validation_tracker.add_error(row_num, 'Taxable Value', 'Invalid format', taxable_value)
                    self.validation_tracker.skipped_count += 1
                    continue
            
            # Step 6: Get tax amounts
            igst = row.get('_igst_amount', 0) or 0
            cgst = row.get('_cgst_amount', 0) or 0
            sgst = row.get('_sgst_amount', 0) or 0
            cess = row.get('_cess_amount', 0) or 0
            
            try:
                igst = float(igst) if igst and not pd.isna(igst) else 0
                cgst = float(cgst) if cgst and not pd.isna(cgst) else 0
                sgst = float(sgst) if sgst and not pd.isna(sgst) else 0
                cess = float(cess) if cess and not pd.isna(cess) else 0
            except (ValueError, TypeError):
                self.validation_tracker.add_warning(
                    row_num, 'Tax Amounts', 'Invalid tax amount format',
                    'Set to 0', 'Invalid', 0
                )
                igst = cgst = sgst = cess = 0
            
            # Step 7: Validate tax structure consistency
            has_igst = igst > 0
            has_cgst_sgst = (cgst > 0) or (sgst > 0)
            
            if has_igst and has_cgst_sgst:
                self.validation_tracker.add_error(
                    row_num, 'Tax Structure', 
                    'Mixed IGST and CGST/SGST - cannot aggregate', 
                    f'IGST={igst}, CGST={cgst}, SGST={sgst}'
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Step 8: Prevent double aggregation (check if already in B2CL)
            invoice_key = f"{row.get('_invoice_number', '')}_{row.get('_invoice_date', '')}"
            if invoice_key in self.b2cl_invoice_keys:
                self.validation_tracker.add_error(
                    row_num, 'Invoice', 
                    'Already in B2CL sheet - cannot include in B2CS', 
                    invoice_key
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Track this invoice in B2CS
            self.b2cs_invoice_keys.add(invoice_key)
            
            # Step 9: Build aggregation key
            agg_key = f"Type={type_flag}|POS={pos_code}|Rate={rate}|ApplicableTax={applicable_tax_rate}|ECO={eco_gstin_clean}"
            
            # Step 10: Aggregate
            if agg_key in aggregation_table:
                aggregation_table[agg_key]['taxable_value'] += taxable_value
                aggregation_table[agg_key]['igst'] += igst
                aggregation_table[agg_key]['cgst'] += cgst
                aggregation_table[agg_key]['sgst'] += sgst
                aggregation_table[agg_key]['cess'] += cess
                aggregation_table[agg_key]['invoice_count'] += 1
            else:
                aggregation_table[agg_key] = {
                    'type': type_flag,
                    'place_of_supply': pos_display,
                    'pos_code': pos_code,
                    'rate': rate,
                    'applicable_tax_rate': applicable_tax_rate,
                    'taxable_value': taxable_value,
                    'igst': igst,
                    'cgst': cgst,
                    'sgst': sgst,
                    'cess': cess,
                    'ecommerce_gstin': eco_gstin_clean,
                    'invoice_count': 1
                }
            
            self.validation_tracker.valid_count += 1
        
        # Step 11: Build output rows from aggregation table
        rows: List[Dict[str, object]] = []
        for agg_key, agg_data in aggregation_table.items():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2cs', 'type', agg_data['type'])
            self._set_field(payload, 'b2cs', 'place_of_supply', agg_data['place_of_supply'])
            
            # Only set applicable tax rate if present
            if agg_data['applicable_tax_rate']:
                self._set_field(payload, 'b2cs', 'applicable_tax_rate', agg_data['applicable_tax_rate'])
            
            self._set_field(payload, 'b2cs', 'rate', round(agg_data['rate'], 2))
            self._set_field(payload, 'b2cs', 'taxable_value', round(agg_data['taxable_value'], 2))
            self._set_field(payload, 'b2cs', 'igst', round(agg_data['igst'], 2))
            self._set_field(payload, 'b2cs', 'cgst', round(agg_data['cgst'], 2))
            self._set_field(payload, 'b2cs', 'sgst', round(agg_data['sgst'], 2))
            self._set_field(payload, 'b2cs', 'cess_amount', round(agg_data['cess'], 2))
            
            # Only set E-Commerce GSTIN for Type E
            if agg_data['type'] == 'E' and agg_data['ecommerce_gstin']:
                self._set_field(payload, 'b2cs', 'ecommerce_gstin', agg_data['ecommerce_gstin'])
            
            if payload:
                rows.append(payload)
        
        # Step 12: Sort output
        # Sort by: Type (E first), ECO GSTIN, POS, Rate
        def sort_key(row_dict):
            type_val = row_dict.get(self.template_field_headers['b2cs'].get('type', ''), 'OE')
            eco = row_dict.get(self.template_field_headers['b2cs'].get('ecommerce_gstin', ''), '')
            pos = row_dict.get(self.template_field_headers['b2cs'].get('place_of_supply', ''), '')
            rate = row_dict.get(self.template_field_headers['b2cs'].get('rate', ''), 0)
            
            # Extract numeric part from POS (e.g., "27-Maharashtra" -> 27)
            pos_num = 0
            if pos:
                match = re.match(r'^(\d+)', str(pos))
                if match:
                    pos_num = int(match.group(1))
            
            # Type E = 0, Type OE = 1
            type_sort = 0 if type_val == 'E' else 1
            
            return (type_sort, eco or '', pos_num, float(rate) if rate else 0)
        
        rows_sorted = sorted(rows, key=sort_key)
        
        logger.info(f"B2CS: Aggregated {len(subset)} invoices into {len(rows_sorted)} groups")
        
        return sheet_name, self._build_sheet_dataframe(rows_sorted, sheet_name)
    
    def _build_b2cla(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build B2CLA (B2C Large Amendment) sheet.
        Amendment logic: Validate original exists in B2CL, date ordering, value changes.
        Output side-by-side original vs revised.
        """
        sheet_name = self.sheet_mapping.get('b2cla')
        if not sheet_name:
            logger.info("B2CLA sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Identify B2CLA records: B2C Large + Amendment flag
        mask = (
            df['_is_amendment']
            & (~df['_has_valid_gstin'])
            & (df['_is_large_b2cl'])
            & (~df['_is_credit_or_debit'])
            & (~df['_is_export'])
        )
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        logger.info(f"B2CLA: Processing {len(subset)} amendment rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in subset.iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # Get original and revised invoice details
            original_invoice_number = row.get('_original_invoice_number')
            original_invoice_date = row.get('_original_invoice_date')
            revised_invoice_number = row.get('_invoice_number')
            revised_invoice_date = row.get('_invoice_date')
            
            # Validate original invoice data
            if not original_invoice_number or pd.isna(original_invoice_number):
                self.validation_tracker.add_error(
                    row_num, 'Original Invoice Number', 
                    'Missing for amendment', original_invoice_number
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if not original_invoice_date or pd.isna(original_invoice_date):
                self.validation_tracker.add_error(
                    row_num, 'Original Invoice Date', 
                    'Missing for amendment', original_invoice_date
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate revised invoice data
            if not revised_invoice_number or pd.isna(revised_invoice_number):
                self.validation_tracker.add_error(
                    row_num, 'Revised Invoice Number', 
                    'Missing for amendment', revised_invoice_number
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if not revised_invoice_date or pd.isna(revised_invoice_date):
                self.validation_tracker.add_error(
                    row_num, 'Revised Invoice Date', 
                    'Missing for amendment', revised_invoice_date
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate date ordering (revised should be same or after original)
            try:
                orig_date = pd.to_datetime(original_invoice_date)
                rev_date = pd.to_datetime(revised_invoice_date)
                if rev_date < orig_date:
                    self.validation_tracker.add_warning(
                        row_num, 'Invoice Date',
                        'Revised date is before original date',
                        'Accepted', f'Original: {orig_date}, Revised: {rev_date}',
                        'Accepted'
                    )
            except Exception as e:
                self.validation_tracker.add_warning(
                    row_num, 'Invoice Date',
                    f'Unable to validate date ordering: {e}',
                    'Accepted', '', ''
                )
            
            # Validate invoice value
            invoice_value = row.get('_invoice_value', 0)
            if not self._validate_amount_not_zero_negative(invoice_value, row_num, 'Invoice Value'):
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate Place of Supply
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(row_num, 'Place of Supply', 'Missing', pos_code)
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate Rate
            rate = row.get('_rate')
            if not self._validate_tax_rate(rate, row_num, None):
                self.validation_tracker.skipped_count += 1
                continue
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2cla', 'original_invoice_number', str(original_invoice_number).strip())
            self._set_field(payload, 'b2cla', 'original_invoice_date', original_invoice_date)
            self._set_field(payload, 'b2cla', 'original_place_of_supply', self._format_place_of_supply(pos_code))
            self._set_field(payload, 'b2cla', 'revised_invoice_number', str(revised_invoice_number).strip())
            self._set_field(payload, 'b2cla', 'revised_invoice_date', revised_invoice_date)
            self._set_field(payload, 'b2cla', 'revised_place_of_supply', self._format_place_of_supply(pos_code))
            self._set_field(payload, 'b2cla', 'invoice_value', round(invoice_value, 2))
            self._set_field(payload, 'b2cla', 'rate', round(rate, 2))
            
            # Set taxable value and cess
            taxable_value = row.get('_taxable_value', 0)
            cess_amount = row.get('_cess_amount', 0)
            self._set_field(payload, 'b2cla', 'taxable_value', round(taxable_value, 2) if taxable_value else 0)
            self._set_field(payload, 'b2cla', 'cess_amount', round(cess_amount, 2) if cess_amount else 0)
            
            # E-Commerce GSTIN (if applicable)
            ecommerce_gstin = row.get('_ecommerce_gstin')
            if ecommerce_gstin and not pd.isna(ecommerce_gstin):
                self._set_field(payload, 'b2cla', 'ecommerce_gstin', str(ecommerce_gstin).strip().upper())
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"B2CLA: Processed {len(rows)} amendment records")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_b2csa(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build B2CSA (B2C Small Amendment) sheet.
        Amendment logic follows same pattern as B2CLA but for aggregated B2CS transactions.
        """
        sheet_name = self.sheet_mapping.get('b2csa')
        if not sheet_name:
            logger.info("B2CSA sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Identify B2CSA records: B2C Small + Amendment flag
        mask = (
            df['_is_amendment']
            & (~df['_has_valid_gstin'])
            & (~df['_is_large_b2cl'])
            & (~df['_is_credit_or_debit'])
            & (~df['_is_export'])
        )
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        logger.info(f"B2CSA: Processing {len(subset)} amendment rows")
        
        # For B2CSA, we need to aggregate amendments similar to B2CS
        # But also show original vs revised data
        aggregation_table = {}
        
        for idx, row in subset.iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # Validate Place of Supply
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(row_num, 'Place of Supply', 'Missing', pos_code)
                self.validation_tracker.skipped_count += 1
                continue
            
            pos_display = self._format_place_of_supply(pos_code)
            
            # Validate Rate
            rate = row.get('_rate')
            if rate is None or pd.isna(rate):
                self.validation_tracker.add_error(row_num, 'Rate', 'Missing', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(row_num, 'Rate', 'Invalid format', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            # Determine Type (E or OE)
            ecommerce_gstin = row.get('_ecommerce_gstin')
            type_flag = 'OE'
            eco_gstin_clean = ''
            
            if ecommerce_gstin and not pd.isna(ecommerce_gstin):
                eco_gstin_clean = str(ecommerce_gstin).strip().upper()
                is_valid, error = self.validation_service.validate_gstin(eco_gstin_clean)
                if is_valid:
                    type_flag = 'E'
                else:
                    eco_gstin_clean = ''
                    type_flag = 'OE'
            
            # Get amounts
            taxable_value = row.get('_taxable_value', 0) or 0
            cess = row.get('_cess_amount', 0) or 0
            
            try:
                taxable_value = float(taxable_value)
                cess = float(cess)
            except (ValueError, TypeError):
                self.validation_tracker.add_warning(
                    row_num, 'Amounts', 'Invalid amount format',
                    'Set to 0', 'Invalid', 0
                )
                taxable_value = cess = 0
            
            # Build aggregation key
            applicable_tax_rate = row.get('_applicable_tax_rate', '')
            agg_key = f"Type={type_flag}|POS={pos_code}|Rate={rate}|ApplicableTax={applicable_tax_rate}|ECO={eco_gstin_clean}"
            
            # Aggregate
            if agg_key in aggregation_table:
                aggregation_table[agg_key]['taxable_value'] += taxable_value
                aggregation_table[agg_key]['cess'] += cess
                aggregation_table[agg_key]['amendment_count'] += 1
            else:
                aggregation_table[agg_key] = {
                    'type': type_flag,
                    'place_of_supply': pos_display,
                    'pos_code': pos_code,
                    'rate': rate,
                    'applicable_tax_rate': applicable_tax_rate,
                    'taxable_value': taxable_value,
                    'cess': cess,
                    'ecommerce_gstin': eco_gstin_clean,
                    'amendment_count': 1
                }
            
            self.validation_tracker.valid_count += 1
        
        # Build output rows
        rows: List[Dict[str, object]] = []
        for agg_key, agg_data in aggregation_table.items():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2csa', 'type', agg_data['type'])
            self._set_field(payload, 'b2csa', 'original_place_of_supply', agg_data['place_of_supply'])
            self._set_field(payload, 'b2csa', 'revised_place_of_supply', agg_data['place_of_supply'])
            
            if agg_data['applicable_tax_rate']:
                self._set_field(payload, 'b2csa', 'applicable_tax_rate', agg_data['applicable_tax_rate'])
            
            self._set_field(payload, 'b2csa', 'rate', round(agg_data['rate'], 2))
            self._set_field(payload, 'b2csa', 'taxable_value', round(agg_data['taxable_value'], 2))
            self._set_field(payload, 'b2csa', 'cess_amount', round(agg_data['cess'], 2))
            
            if agg_data['type'] == 'E' and agg_data['ecommerce_gstin']:
                self._set_field(payload, 'b2csa', 'ecommerce_gstin', agg_data['ecommerce_gstin'])
            
            if payload:
                rows.append(payload)
        
        logger.info(f"B2CSA: Aggregated {len(subset)} amendments into {len(rows)} groups")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnr(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build CDNR sheet (Credit/Debit Notes - Registered Recipients).
        Comprehensive validation with sign handling for note types.
        """
        sheet_name = self.sheet_mapping.get('cdnr')
        if not sheet_name:
            return None, pd.DataFrame()
        
        # Classification: Credit/Debit notes with valid registered GSTIN
        mask = (
            df['_is_credit_or_debit'] 
            & df['_has_valid_gstin']
            & (~df['_is_amendment'])  # Regular notes only, not amendments
        )
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"CDNR: Processing {mask.sum()} credit/debit note rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate GSTIN (MANDATORY, PRIMARY KEY)
            gstin = row.get('_gstin')
            if not gstin or pd.isna(gstin):
                self.validation_tracker.add_error(row_num, 'GSTIN', 'Missing for CDNR', gstin)
                self.validation_tracker.skipped_count += 1
                continue
            
            gstin_clean = str(gstin).strip().upper()
            is_valid_gstin, gstin_error = self.validation_service.validate_gstin(gstin_clean)
            if not is_valid_gstin:
                self.validation_tracker.add_error(row_num, 'GSTIN', gstin_error or 'Invalid GSTIN', gstin)
                self.validation_tracker.skipped_count += 1
                continue
            
            # 2. Validate Receiver Name (OPTIONAL but recommended)
            receiver_name = row.get('_receiver_name')
            if not receiver_name or pd.isna(receiver_name):
                receiver_name = 'Registered Recipient'
                self.validation_tracker.add_warning(
                    row_num, 'Receiver Name', 'Not provided',
                    'Using default', '', 'Registered Recipient'
                )
            else:
                receiver_name = self._validate_receiver_name(receiver_name, row_num)
            
            # 3. Validate Note Number (MANDATORY, UNIQUE)
            note_number = row.get('_note_number')
            if not self._validate_note_number(note_number, row_num):
                self.validation_tracker.skipped_count += 1
                continue
            
            note_number_clean = str(note_number).strip().upper()
            
            # Check uniqueness
            note_key = f"{gstin_clean}|{note_number_clean}|{row.get('_note_date', '')}"
            if note_key in self.cdnr_note_keys:
                self.validation_tracker.add_error(
                    row_num, 'Note Number',
                    'Duplicate note detected',
                    note_key
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            self.cdnr_note_keys.add(note_key)
            
            # 4. Validate Note Date (MANDATORY)
            note_date = row.get('_note_date')
            if not note_date or pd.isna(note_date):
                self.validation_tracker.add_error(row_num, 'Note Date', 'Missing', note_date)
                self.validation_tracker.skipped_count += 1
                continue
            
            if not self._validate_date_range(note_date, row_num):
                self.validation_tracker.skipped_count += 1
                continue
            
            # 5. Validate Note Type (MANDATORY: C, D, or R)
            note_type, note_sign = self._validate_note_type(row.get('_note_type'), row_num)
            if not note_type:
                self.validation_tracker.skipped_count += 1
                continue
            
            # 6. Validate Place of Supply (MANDATORY)
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(row_num, 'Place of Supply', 'Missing', pos_code)
                self.validation_tracker.skipped_count += 1
                continue
            
            pos_display = self._format_place_of_supply(pos_code)
            
            # Determine interstate status
            gstin_state = gstin_clean[:2]
            is_interstate = (gstin_state != pos_code)
            
            # 7. Validate Reverse Charge (MANDATORY)
            reverse_charge = self._normalize_reverse_charge(row.get('_reverse_charge', 'N'))
            
            # 8. Validate Rate (MANDATORY)
            rate = row.get('_rate')
            if not self._validate_tax_rate(rate, row_num, None):
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(row_num, 'Rate', 'Invalid format', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            # 9. Validate Taxable Value (MANDATORY)
            taxable_value = row.get('_taxable_value')
            if not self._validate_amount_not_zero_negative(taxable_value, row_num, 'Taxable Value'):
                self.validation_tracker.skipped_count += 1
                continue
            
            taxable_value = float(taxable_value)
            
            # 10. Validate Cess Amount (OPTIONAL)
            cess_amount = row.get('_cess_amount', 0)
            if cess_amount and not pd.isna(cess_amount):
                try:
                    cess_amount = float(cess_amount)
                    if cess_amount < 0:
                        self.validation_tracker.add_error(row_num, 'Cess Amount', 'Negative value', cess_amount)
                        self.validation_tracker.skipped_count += 1
                        continue
                except (ValueError, TypeError):
                    self.validation_tracker.add_warning(
                        row_num, 'Cess Amount', 'Invalid format',
                        'Set to 0', cess_amount, 0
                    )
                    cess_amount = 0
            else:
                cess_amount = 0
            
            # 11. Calculate Tax Amounts based on interstate and reverse charge
            igst, cgst, sgst = self._calculate_tax_amounts(
                taxable_value, rate, is_interstate, reverse_charge == 'Y', row_num
            )
            
            # 12. Apply Note Type Sign
            # Credit Note (C) and Refund Voucher (R): Negative amounts
            # Debit Note (D): Positive amounts
            if note_sign == 'NEGATIVE':
                igst = -abs(igst)
                cgst = -abs(cgst)
                sgst = -abs(sgst)
                cess_amount = -abs(cess_amount)
            
            # 13. Validate Note Value (MANDATORY, CROSS-CHECK)
            note_value = row.get('_note_value')
            if note_value and not pd.isna(note_value):
                try:
                    note_value_numeric = abs(float(note_value))  # Store as positive
                except (ValueError, TypeError):
                    self.validation_tracker.add_error(row_num, 'Note Value', 'Invalid format', note_value)
                    self.validation_tracker.skipped_count += 1
                    continue
            else:
                # Calculate note value if not provided
                tax_total = abs(igst) + abs(cgst) + abs(sgst) + abs(cess_amount)
                note_value_numeric = taxable_value + tax_total
            
            # Cross-check note value
            calculated_note_value = taxable_value + abs(igst) + abs(cgst) + abs(sgst) + abs(cess_amount)
            if abs(note_value_numeric - calculated_note_value) > 0.02:
                self.validation_tracker.add_warning(
                    row_num, 'Note Value',
                    f'Mismatch: Provided={note_value_numeric:.2f}, Calculated={calculated_note_value:.2f}',
                    'Using calculated', note_value_numeric, calculated_note_value
                )
                note_value_numeric = calculated_note_value
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'cdnr', 'gstin', gstin_clean)
            self._set_field(payload, 'cdnr', 'receiver_name', receiver_name)
            self._set_field(payload, 'cdnr', 'note_number', note_number_clean)
            self._set_field(payload, 'cdnr', 'note_date', note_date)
            self._set_field(payload, 'cdnr', 'note_type', note_type)
            self._set_field(payload, 'cdnr', 'place_of_supply', pos_display)
            self._set_field(payload, 'cdnr', 'reverse_charge', reverse_charge)
            self._set_field(payload, 'cdnr', 'note_value', round(note_value_numeric, 2))
            self._set_field(payload, 'cdnr', 'rate', round(rate, 2))
            self._set_field(payload, 'cdnr', 'taxable_value', round(taxable_value, 2))
            self._set_field(payload, 'cdnr', 'igst', round(igst, 2))
            self._set_field(payload, 'cdnr', 'cgst', round(cgst, 2))
            self._set_field(payload, 'cdnr', 'sgst', round(sgst, 2))
            self._set_field(payload, 'cdnr', 'cess_amount', round(cess_amount, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"CDNR: Processed {len(rows)} valid notes")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnra(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build CDNRA sheet (Amendments to Credit/Debit Notes - Registered).
        Cross-references with CDNR sheet for amendment validation.
        """
        sheet_name = self.sheet_mapping.get('cdnra')
        if not sheet_name:
            logger.info("CDNRA sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Classification: Amendments to credit/debit notes with valid GSTIN
        mask = (
            df['_is_amendment']
            & df['_is_credit_or_debit']
            & df['_has_valid_gstin']
        )
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"CDNRA: Processing {mask.sum()} amendment rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate GSTIN
            gstin = row.get('_gstin')
            if not gstin or pd.isna(gstin):
                self.validation_tracker.add_error(row_num, 'GSTIN', 'Missing for CDNRA', gstin)
                self.validation_tracker.skipped_count += 1
                continue
            
            gstin_clean = str(gstin).strip().upper()
            is_valid_gstin, gstin_error = self.validation_service.validate_gstin(gstin_clean)
            if not is_valid_gstin:
                self.validation_tracker.add_error(row_num, 'GSTIN', gstin_error or 'Invalid GSTIN', gstin)
                self.validation_tracker.skipped_count += 1
                continue
            
            # 2. Get original and revised note details
            original_note_number = row.get('_original_invoice_number')  # Using invoice fields for note fields
            original_note_date = row.get('_original_invoice_date')
            revised_note_number = row.get('_note_number')
            revised_note_date = row.get('_note_date')
            
            # Validate original note data
            if not original_note_number or pd.isna(original_note_number):
                self.validation_tracker.add_error(
                    row_num, 'Original Note Number',
                    'Missing for amendment', original_note_number
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if not original_note_date or pd.isna(original_note_date):
                self.validation_tracker.add_error(
                    row_num, 'Original Note Date',
                    'Missing for amendment', original_note_date
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate revised note data
            if not self._validate_note_number(revised_note_number, row_num):
                self.validation_tracker.skipped_count += 1
                continue
            
            revised_note_number_clean = str(revised_note_number).strip().upper()
            
            if not revised_note_date or pd.isna(revised_note_date):
                self.validation_tracker.add_error(
                    row_num, 'Revised Note Date',
                    'Missing for amendment', revised_note_date
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate date ordering
            try:
                orig_date = pd.to_datetime(original_note_date)
                rev_date = pd.to_datetime(revised_note_date)
                if rev_date < orig_date:
                    self.validation_tracker.add_warning(
                        row_num, 'Note Date',
                        'Revised date is before original date',
                        'Accepted', f'Original: {orig_date}, Revised: {rev_date}',
                        'Accepted'
                    )
            except Exception as e:
                self.validation_tracker.add_warning(
                    row_num, 'Note Date',
                    f'Unable to validate date ordering: {e}',
                    'Accepted', '', ''
                )
            
            # Cross-reference: Check if original note exists in CDNR
            original_note_key = f"{gstin_clean}|{str(original_note_number).strip().upper()}|{original_note_date}"
            if original_note_key not in self.cdnr_note_keys:
                self.validation_tracker.add_warning(
                    row_num, 'Original Note',
                    'Original note not found in CDNR sheet',
                    'Proceeding with amendment', original_note_key, 'Not found'
                )
            
            # Validate note type
            note_type, note_sign = self._validate_note_type(row.get('_note_type'), row_num)
            if not note_type:
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate POS
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(row_num, 'Place of Supply', 'Missing', pos_code)
                self.validation_tracker.skipped_count += 1
                continue
            
            pos_display = self._format_place_of_supply(pos_code)
            
            # Determine interstate
            gstin_state = gstin_clean[:2]
            is_interstate = (gstin_state != pos_code)
            
            # Validate reverse charge
            reverse_charge = self._normalize_reverse_charge(row.get('_reverse_charge', 'N'))
            
            # Validate rate
            rate = row.get('_rate')
            if not self._validate_tax_rate(rate, row_num, None):
                self.validation_tracker.skipped_count += 1
                continue
            
            rate = float(rate)
            
            # Validate taxable value
            taxable_value = row.get('_taxable_value')
            if not self._validate_amount_not_zero_negative(taxable_value, row_num, 'Taxable Value'):
                self.validation_tracker.skipped_count += 1
                continue
            
            taxable_value = float(taxable_value)
            
            # Validate cess
            cess_amount = row.get('_cess_amount', 0)
            if cess_amount and not pd.isna(cess_amount):
                try:
                    cess_amount = float(cess_amount)
                except (ValueError, TypeError):
                    cess_amount = 0
            else:
                cess_amount = 0
            
            # Calculate tax amounts
            igst, cgst, sgst = self._calculate_tax_amounts(
                taxable_value, rate, is_interstate, reverse_charge == 'Y', row_num
            )
            
            # Apply note type sign
            if note_sign == 'NEGATIVE':
                igst = -abs(igst)
                cgst = -abs(cgst)
                sgst = -abs(sgst)
                cess_amount = -abs(cess_amount)
            
            # Calculate note value
            note_value = row.get('_note_value')
            if note_value and not pd.isna(note_value):
                note_value_numeric = abs(float(note_value))
            else:
                tax_total = abs(igst) + abs(cgst) + abs(sgst) + abs(cess_amount)
                note_value_numeric = taxable_value + tax_total
            
            # Validate receiver name
            receiver_name = row.get('_receiver_name')
            if not receiver_name or pd.isna(receiver_name):
                receiver_name = 'Registered Recipient'
            else:
                receiver_name = self._validate_receiver_name(receiver_name, row_num)
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'cdnra', 'gstin', gstin_clean)
            self._set_field(payload, 'cdnra', 'receiver_name', receiver_name)
            self._set_field(payload, 'cdnra', 'original_note_number', str(original_note_number).strip().upper())
            self._set_field(payload, 'cdnra', 'original_note_date', original_note_date)
            self._set_field(payload, 'cdnra', 'revised_note_number', revised_note_number_clean)
            self._set_field(payload, 'cdnra', 'revised_note_date', revised_note_date)
            self._set_field(payload, 'cdnra', 'note_type', note_type)
            self._set_field(payload, 'cdnra', 'place_of_supply', pos_display)
            self._set_field(payload, 'cdnra', 'reverse_charge', reverse_charge)
            self._set_field(payload, 'cdnra', 'note_value', round(note_value_numeric, 2))
            self._set_field(payload, 'cdnra', 'rate', round(rate, 2))
            self._set_field(payload, 'cdnra', 'taxable_value', round(taxable_value, 2))
            self._set_field(payload, 'cdnra', 'cess_amount', round(cess_amount, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"CDNRA: Processed {len(rows)} amendment records")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnur(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build CDNUR sheet (Credit/Debit Notes - Unregistered Recipients).
        For B2CL, Export notes with value thresholds and UR Type validation.
        """
        sheet_name = self.sheet_mapping.get('cdnur')
        if not sheet_name:
            return None, pd.DataFrame()
        
        mask = (
            df['_is_credit_or_debit'] 
            & (~df['_has_valid_gstin'])
            & (~df['_is_amendment'])
        )
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"CDNUR: Processing {mask.sum()} unregistered credit/debit note rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate UR Type (MANDATORY for CDNUR)
            ur_type = row.get('_ur_type')
            if not ur_type or pd.isna(ur_type):
                # Try to infer from other fields
                if row.get('_is_export'):
                    ur_type = 'EXPWOP'  # Default to export without payment
                else:
                    ur_type = 'B2CL'  # Default to B2C Large
            
            ur_type_clean = str(ur_type).strip().upper()
            
            # Validate UR Type
            valid_ur_types = ['B2CL', 'EXPWP', 'EXPWOP']
            if ur_type_clean not in valid_ur_types:
                self.validation_tracker.add_error(
                    row_num, 'UR Type',
                    f'Invalid UR Type: {ur_type_clean}. Must be B2CL, EXPWP, or EXPWOP',
                    ur_type
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 2. Validate Note Number
            note_number = row.get('_note_number')
            if not self._validate_note_number(note_number, row_num):
                self.validation_tracker.skipped_count += 1
                continue
            
            note_number_clean = str(note_number).strip().upper()
            
            # Track uniqueness
            note_key = f"{ur_type_clean}|{note_number_clean}|{row.get('_note_date', '')}"
            if note_key in self.cdnur_note_keys:
                self.validation_tracker.add_error(
                    row_num, 'Note Number',
                    'Duplicate note detected',
                    note_key
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            self.cdnur_note_keys.add(note_key)
            
            # 3. Validate Note Date
            note_date = row.get('_note_date')
            if not note_date or pd.isna(note_date):
                self.validation_tracker.add_error(row_num, 'Note Date', 'Missing', note_date)
                self.validation_tracker.skipped_count += 1
                continue
            
            if not self._validate_date_range(note_date, row_num):
                self.validation_tracker.skipped_count += 1
                continue
            
            # Determine value threshold based on date
            try:
                date_obj = pd.to_datetime(note_date)
                if date_obj >= pd.Timestamp('2024-08-01'):
                    value_threshold = 100000
                else:
                    value_threshold = 250000
            except Exception:
                value_threshold = 100000  # Default to new threshold
            
            # 4. Validate Note Type
            note_type, note_sign = self._validate_note_type(row.get('_note_type'), row_num)
            if not note_type:
                self.validation_tracker.skipped_count += 1
                continue
            
            # 5. Validate Place of Supply (Optional for CDNUR)
            pos_code = row.get('_pos_code')
            if pos_code and not pd.isna(pos_code):
                pos_display = self._format_place_of_supply(pos_code)
            else:
                # POS is optional for CDNUR, especially for exports
                pos_display = None
                if ur_type_clean == 'B2CL':
                    self.validation_tracker.add_warning(
                        row_num, 'Place of Supply',
                        'POS recommended for B2CL notes',
                        'Proceeding without POS', '', ''
                    )
            
            # 6. Validate Note Value (MANDATORY with threshold)
            note_value = row.get('_note_value')
            if not note_value or pd.isna(note_value):
                self.validation_tracker.add_error(row_num, 'Note Value', 'Missing', note_value)
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                note_value_numeric = abs(float(note_value))
            except (ValueError, TypeError):
                self.validation_tracker.add_error(row_num, 'Note Value', 'Invalid format', note_value)
                self.validation_tracker.skipped_count += 1
                continue
            
            if note_value_numeric == 0:
                self.validation_tracker.add_error(row_num, 'Note Value', 'Cannot be zero', note_value)
                self.validation_tracker.skipped_count += 1
                continue
            
            # Check threshold
            if note_value_numeric < value_threshold:
                self.validation_tracker.add_error(
                    row_num, 'Note Value',
                    f'Below CDNUR threshold (₹{value_threshold:,.0f})',
                    note_value_numeric
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 7. Validate Rate based on UR Type
            rate = row.get('_rate')
            if rate is None or pd.isna(rate):
                self.validation_tracker.add_error(row_num, 'Rate', 'Missing', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(row_num, 'Rate', 'Invalid format', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate rate based on UR Type
            if ur_type_clean == 'B2CL':
                valid_rates = [0, 5, 12, 18, 28]
                if rate not in valid_rates:
                    self.validation_tracker.add_error(
                        row_num, 'Rate',
                        f'Invalid rate for B2CL: {rate}. Must be 0, 5, 12, 18, or 28',
                        rate
                    )
                    self.validation_tracker.skipped_count += 1
                    continue
            elif ur_type_clean in ['EXPWP', 'EXPWOP']:
                # Exports are typically zero-rated
                if rate != 0:
                    self.validation_tracker.add_warning(
                        row_num, 'Rate',
                        f'Non-zero rate for export: {rate}',
                        'Exports typically zero-rated', rate, 0
                    )
            
            # 8. Validate Taxable Value (Optional but recommended)
            taxable_value = row.get('_taxable_value')
            if not taxable_value or pd.isna(taxable_value):
                # Calculate from note value if not provided
                # Approximate: taxable_value ≈ note_value / (1 + rate/100)
                if rate > 0:
                    taxable_value = note_value_numeric / (1 + rate / 100)
                else:
                    taxable_value = note_value_numeric
                self.validation_tracker.add_warning(
                    row_num, 'Taxable Value',
                    'Not provided, calculated from note value',
                    'Calculated', 'Missing', round(taxable_value, 2)
                )
            else:
                try:
                    taxable_value = float(taxable_value)
                    if taxable_value <= 0:
                        self.validation_tracker.add_error(
                            row_num, 'Taxable Value',
                            'Must be positive', taxable_value
                        )
                        self.validation_tracker.skipped_count += 1
                        continue
                except (ValueError, TypeError):
                    self.validation_tracker.add_error(
                        row_num, 'Taxable Value',
                        'Invalid format', taxable_value
                    )
                    self.validation_tracker.skipped_count += 1
                    continue
            
            # 9. Cess Amount (Optional)
            cess_amount = row.get('_cess_amount', 0)
            if cess_amount and not pd.isna(cess_amount):
                try:
                    cess_amount = float(cess_amount)
                    if cess_amount < 0:
                        self.validation_tracker.add_error(
                            row_num, 'Cess Amount',
                            'Cannot be negative', cess_amount
                        )
                        cess_amount = 0
                except (ValueError, TypeError):
                    cess_amount = 0
            else:
                cess_amount = 0
            
            # 10. Calculate Tax Amounts - CDNUR uses IGST ONLY (interstate/unregistered)
            # Force IGST structure for CDNUR
            igst = (taxable_value * rate) / 100
            cgst = 0  # CDNUR always uses IGST only
            sgst = 0  # CDNUR always uses IGST only
            
            # Apply note type sign
            if note_sign == 'NEGATIVE':
                igst = -abs(igst)
                cess_amount = -abs(cess_amount)
            
            # Cross-check note value
            calculated_note_value = taxable_value + abs(igst) + abs(cess_amount)
            if abs(note_value_numeric - calculated_note_value) > 0.02:
                self.validation_tracker.add_warning(
                    row_num, 'Note Value',
                    f'Mismatch: Provided={note_value_numeric:.2f}, Calculated={calculated_note_value:.2f}',
                    'Using provided value', calculated_note_value, note_value_numeric
                )
            
            # Customer name
            customer_name = row.get('_receiver_name')
            if not customer_name or pd.isna(customer_name):
                customer_name = 'Unregistered Recipient'
            else:
                customer_name = self._validate_receiver_name(customer_name, row_num)
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'cdnur', 'ur_type', ur_type_clean)
            self._set_field(payload, 'cdnur', 'note_number', note_number_clean)
            self._set_field(payload, 'cdnur', 'note_date', note_date)
            self._set_field(payload, 'cdnur', 'note_type', note_type)
            
            if pos_display:
                self._set_field(payload, 'cdnur', 'place_of_supply', pos_display)
            
            self._set_field(payload, 'cdnur', 'note_value', round(note_value_numeric, 2))
            self._set_field(payload, 'cdnur', 'rate', round(rate, 2))
            self._set_field(payload, 'cdnur', 'taxable_value', round(taxable_value, 2))
            self._set_field(payload, 'cdnur', 'igst', round(igst, 2))
            self._set_field(payload, 'cdnur', 'cess_amount', round(cess_amount, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"CDNUR: Processed {len(rows)} valid notes (threshold: ₹{value_threshold if 'value_threshold' in locals() else '100,000'})")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnura(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build CDNURA sheet (Amendments to Credit/Debit Notes - Unregistered).
        Amendments follow same UR Type and value threshold rules as CDNUR.
        """
        sheet_name = self.sheet_mapping.get('cdnura')
        if not sheet_name:
            logger.info("CDNURA sheet not found in template mapping")
            return None, pd.DataFrame()
        
        mask = (
            df['_is_amendment']
            & df['_is_credit_or_debit']
            & (~df['_has_valid_gstin'])
        )
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"CDNURA: Processing {mask.sum()} amendment rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate UR Type (MANDATORY)
            ur_type = row.get('_ur_type')
            if not ur_type or pd.isna(ur_type):
                if row.get('_is_export'):
                    ur_type = 'EXPWOP'
                else:
                    ur_type = 'B2CL'
            
            ur_type_clean = str(ur_type).strip().upper()
            valid_ur_types = ['B2CL', 'EXPWP', 'EXPWOP']
            if ur_type_clean not in valid_ur_types:
                self.validation_tracker.add_error(
                    row_num, 'UR Type',
                    f'Invalid UR Type: {ur_type_clean}',
                    ur_type
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 2. Get original and revised details
            original_note_number = row.get('_original_invoice_number')
            original_note_date = row.get('_original_invoice_date')
            revised_note_number = row.get('_note_number')
            revised_note_date = row.get('_note_date')
            
            # Validate original
            if not original_note_number or pd.isna(original_note_number):
                self.validation_tracker.add_error(
                    row_num, 'Original Note Number',
                    'Missing for amendment', original_note_number
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if not original_note_date or pd.isna(original_note_date):
                self.validation_tracker.add_error(
                    row_num, 'Original Note Date',
                    'Missing for amendment', original_note_date
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate revised
            if not self._validate_note_number(revised_note_number, row_num):
                self.validation_tracker.skipped_count += 1
                continue
            
            revised_note_number_clean = str(revised_note_number).strip().upper()
            
            if not revised_note_date or pd.isna(revised_note_date):
                self.validation_tracker.add_error(
                    row_num, 'Revised Note Date',
                    'Missing for amendment', revised_note_date
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate date ordering
            try:
                orig_date = pd.to_datetime(original_note_date)
                rev_date = pd.to_datetime(revised_note_date)
                if rev_date < orig_date:
                    self.validation_tracker.add_warning(
                        row_num, 'Note Date',
                        'Revised date is before original date',
                        'Accepted', f'Original: {orig_date}, Revised: {rev_date}',
                        'Accepted'
                    )
            except Exception as e:
                self.validation_tracker.add_warning(
                    row_num, 'Note Date',
                    f'Unable to validate date ordering: {e}',
                    'Accepted', '', ''
                )
            
            # Cross-reference: Check if original note exists in CDNUR
            original_note_key = f"{ur_type_clean}|{str(original_note_number).strip().upper()}|{original_note_date}"
            if original_note_key not in self.cdnur_note_keys:
                self.validation_tracker.add_warning(
                    row_num, 'Original Note',
                    'Original note not found in CDNUR sheet',
                    'Proceeding with amendment', original_note_key, 'Not found'
                )
            
            # 3. Validate note type
            note_type, note_sign = self._validate_note_type(row.get('_note_type'), row_num)
            if not note_type:
                self.validation_tracker.skipped_count += 1
                continue
            
            # 4. Validate POS (Optional for CDNURA)
            pos_code = row.get('_pos_code')
            if pos_code and not pd.isna(pos_code):
                pos_display = self._format_place_of_supply(pos_code)
            else:
                pos_display = None
            
            # 5. Validate rate based on UR Type
            rate = row.get('_rate')
            if rate is None or pd.isna(rate):
                self.validation_tracker.add_error(row_num, 'Rate', 'Missing', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(row_num, 'Rate', 'Invalid format', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate rate based on UR Type
            if ur_type_clean == 'B2CL':
                valid_rates = [0, 5, 12, 18, 28]
                if rate not in valid_rates:
                    self.validation_tracker.add_warning(
                        row_num, 'Rate',
                        f'Unusual rate for B2CL: {rate}',
                        'Accepted', rate, rate
                    )
            elif ur_type_clean in ['EXPWP', 'EXPWOP']:
                if rate != 0:
                    self.validation_tracker.add_warning(
                        row_num, 'Rate',
                        f'Non-zero rate for export: {rate}',
                        'Exports typically zero-rated', rate, 0
                    )
            
            # 6. Validate taxable value
            taxable_value = row.get('_taxable_value')
            if not taxable_value or pd.isna(taxable_value):
                taxable_value = 0
                self.validation_tracker.add_warning(
                    row_num, 'Taxable Value',
                    'Not provided for amendment',
                    'Set to 0', 'Missing', 0
                )
            else:
                try:
                    taxable_value = float(taxable_value)
                    if taxable_value < 0:
                        taxable_value = abs(taxable_value)
                except (ValueError, TypeError):
                    taxable_value = 0
            
            # 7. Cess
            cess_amount = row.get('_cess_amount', 0)
            if cess_amount and not pd.isna(cess_amount):
                try:
                    cess_amount = float(cess_amount)
                except (ValueError, TypeError):
                    cess_amount = 0
            else:
                cess_amount = 0
            
            # 8. Calculate tax - CDNURA uses IGST ONLY
            igst = (taxable_value * rate) / 100
            cgst = 0  # CDNURA always uses IGST only
            sgst = 0  # CDNURA always uses IGST only
            
            # Apply sign
            if note_sign == 'NEGATIVE':
                igst = -abs(igst)
                cess_amount = -abs(cess_amount)
            
            # 9. Note value
            note_value = row.get('_note_value')
            if note_value and not pd.isna(note_value):
                note_value_numeric = abs(float(note_value))
            else:
                tax_total = abs(igst) + abs(cess_amount)
                note_value_numeric = taxable_value + tax_total
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'cdnura', 'ur_type', ur_type_clean)
            self._set_field(payload, 'cdnura', 'original_note_number', str(original_note_number).strip().upper())
            self._set_field(payload, 'cdnura', 'original_note_date', original_note_date)
            self._set_field(payload, 'cdnura', 'revised_note_number', revised_note_number_clean)
            self._set_field(payload, 'cdnura', 'revised_note_date', revised_note_date)
            self._set_field(payload, 'cdnura', 'note_type', note_type)
            
            if pos_display:
                self._set_field(payload, 'cdnura', 'place_of_supply', pos_display)
            
            self._set_field(payload, 'cdnura', 'note_value', round(note_value_numeric, 2))
            self._set_field(payload, 'cdnura', 'rate', round(rate, 2))
            self._set_field(payload, 'cdnura', 'taxable_value', round(taxable_value, 2))
            self._set_field(payload, 'cdnura', 'igst', round(igst, 2))
            self._set_field(payload, 'cdnura', 'cess_amount', round(cess_amount, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"CDNURA: Processed {len(rows)} amendment records")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_export(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build EXP sheet (Exports - WPAY/WOPAY).
        Comprehensive validation for export type, port code, shipping details.
        """
        sheet_name = self.sheet_mapping.get('export')
        if not sheet_name:
            return None, pd.DataFrame()
        
        mask = df['_is_export'] & (~df['_is_credit_or_debit']) & (~df['_is_amendment'])
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"EXP: Processing {mask.sum()} export rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate Export Type (MANDATORY: WPAY or WOPAY)
            export_type = row.get('_export_type')
            if not export_type or pd.isna(export_type):
                self.validation_tracker.add_error(
                    row_num, 'Export Type',
                    'Export type cannot be empty',
                    export_type
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            export_type_clean = str(export_type).strip().upper()
            valid_export_types = ['WPAY', 'WOPAY']
            if export_type_clean not in valid_export_types:
                self.validation_tracker.add_error(
                    row_num, 'Export Type',
                    f'Invalid export type: {export_type_clean}. Must be WPAY or WOPAY',
                    export_type
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Determine tax treatment based on export type
            if export_type_clean == 'WOPAY':
                tax_treatment = 'Zero_Rated'
                expected_rates = [0]
            else:  # WPAY
                tax_treatment = 'IGST_Applicable'
                expected_rates = [0, 5, 12, 18, 28]
            
            # 2. Validate Invoice Number (MANDATORY)
            invoice_number = row.get('_invoice_number')
            if not self._validate_invoice_number(invoice_number, row_num):
                self.validation_tracker.skipped_count += 1
                continue
            
            invoice_number_clean = str(invoice_number).strip().upper()
            
            # 3. Validate Invoice Date (MANDATORY)
            invoice_date = row.get('_invoice_date')
            if not invoice_date or pd.isna(invoice_date):
                self.validation_tracker.add_error(row_num, 'Invoice Date', 'Missing', invoice_date)
                self.validation_tracker.skipped_count += 1
                continue
            
            if not self._validate_date_range(invoice_date, row_num):
                self.validation_tracker.skipped_count += 1
                continue
            
            # Check uniqueness
            invoice_key = f"{invoice_number_clean}|{invoice_date}"
            if invoice_key in self.export_invoice_keys:
                self.validation_tracker.add_error(
                    row_num, 'Invoice',
                    'Duplicate export invoice',
                    invoice_key
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            self.export_invoice_keys.add(invoice_key)
            
            # 4. Validate Invoice Value (MANDATORY)
            invoice_value = row.get('_invoice_value')
            if not self._validate_amount_not_zero_negative(invoice_value, row_num, 'Invoice Value'):
                self.validation_tracker.skipped_count += 1
                continue
            
            invoice_value = float(invoice_value)
            
            # 5. Validate Port Code (OPTIONAL, 6 digits)
            port_code = row.get('_port_code')
            port_code_clean = None
            if port_code and not pd.isna(port_code):
                port_code_str = str(port_code).strip()
                
                # Validate length (exactly 6 digits)
                if len(port_code_str) != 6:
                    self.validation_tracker.add_warning(
                        row_num, 'Port Code',
                        f'Port code should be 6 digits, found {len(port_code_str)}',
                        'Skipping port code', port_code_str, ''
                    )
                    port_code_clean = None
                elif not port_code_str.isdigit():
                    self.validation_tracker.add_warning(
                        row_num, 'Port Code',
                        'Port code must contain only digits',
                        'Skipping port code', port_code_str, ''
                    )
                    port_code_clean = None
                else:
                    port_code_clean = port_code_str
            
            # 6. Validate Shipping Bill Number (OPTIONAL)
            shipping_bill_number = row.get('_shipping_bill_number')
            shipping_bill_number_clean = None
            if shipping_bill_number and not pd.isna(shipping_bill_number):
                sb_str = str(shipping_bill_number).strip().upper()
                # Validate format (alphanumeric, max 20 chars)
                if len(sb_str) > 20:
                    self.validation_tracker.add_warning(
                        row_num, 'Shipping Bill Number',
                        f'Exceeds 20 characters ({len(sb_str)}), truncated',
                        'Truncated', sb_str, sb_str[:20]
                    )
                    shipping_bill_number_clean = sb_str[:20]
                else:
                    shipping_bill_number_clean = sb_str
            
            # 7. Validate Shipping Bill Date (OPTIONAL)
            shipping_bill_date = row.get('_shipping_bill_date')
            shipping_bill_date_clean = None
            if shipping_bill_date and not pd.isna(shipping_bill_date):
                try:
                    sb_date = pd.to_datetime(shipping_bill_date)
                    
                    # Check not in future
                    if sb_date > pd.Timestamp.now():
                        self.validation_tracker.add_warning(
                            row_num, 'Shipping Bill Date',
                            'Future date not allowed',
                            'Skipping shipping date', shipping_bill_date, ''
                        )
                    # Check not before invoice date
                    elif sb_date < pd.to_datetime(invoice_date):
                        self.validation_tracker.add_warning(
                            row_num, 'Shipping Bill Date',
                            'Shipping date before invoice date',
                            'Skipping shipping date', shipping_bill_date, ''
                        )
                    else:
                        shipping_bill_date_clean = shipping_bill_date
                except Exception as e:
                    self.validation_tracker.add_warning(
                        row_num, 'Shipping Bill Date',
                        f'Unable to parse date: {e}',
                        'Skipping shipping date', shipping_bill_date, ''
                    )
            
            # 8. Validate Rate (MANDATORY, based on export type)
            rate = row.get('_rate')
            if rate is None or pd.isna(rate):
                self.validation_tracker.add_error(row_num, 'Rate', 'Missing', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(row_num, 'Rate', 'Invalid format', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate rate based on export type
            if export_type_clean == 'WOPAY' and rate != 0:
                self.validation_tracker.add_error(
                    row_num, 'Rate',
                    f'WOPAY exports must have 0% rate, found {rate}%',
                    rate
                )
                self.validation_tracker.skipped_count += 1
                continue
            elif export_type_clean == 'WPAY' and rate not in expected_rates:
                self.validation_tracker.add_warning(
                    row_num, 'Rate',
                    f'Unusual rate {rate}% for WPAY exports',
                    'Accepted', rate, rate
                )
            
            # 9. Validate Taxable Value (MANDATORY)
            taxable_value = row.get('_taxable_value')
            if not self._validate_amount_not_zero_negative(taxable_value, row_num, 'Taxable Value'):
                self.validation_tracker.skipped_count += 1
                continue
            
            taxable_value = float(taxable_value)
            
            # 10. Calculate Tax - Exports use IGST ONLY
            # For WOPAY: 0% rate, for WPAY: applicable rate
            igst = (taxable_value * rate) / 100
            cgst = 0  # Always 0 for exports
            sgst = 0  # Always 0 for exports
            
            # Check for 65% reduced rate (rare for exports)
            applicable_tax_rate = row.get('_applicable_tax_rate', '')
            if applicable_tax_rate and not pd.isna(applicable_tax_rate):
                applicable_str = str(applicable_tax_rate).strip()
                if applicable_str in ['65%', '65']:
                    if export_type_clean == 'WOPAY':
                        self.validation_tracker.add_warning(
                            row_num, 'Applicable Tax Rate',
                            '65% rate not typically applicable for zero-rated exports',
                            'Ignored for WOPAY', applicable_str, ''
                        )
                    else:
                        # Apply 65% reduction
                        igst = igst * 0.65
            
            # Cross-check invoice value
            calculated_invoice_value = taxable_value + igst
            if abs(invoice_value - calculated_invoice_value) > 0.02:
                self.validation_tracker.add_warning(
                    row_num, 'Invoice Value',
                    f'Mismatch: Provided={invoice_value:.2f}, Calculated={calculated_invoice_value:.2f}',
                    'Using provided value', calculated_invoice_value, invoice_value
                )
            
            # Validate invoice value >= taxable value
            if invoice_value < taxable_value:
                self.validation_tracker.add_error(
                    row_num, 'Invoice Value',
                    'Invoice value cannot be less than taxable value',
                    f'Invoice={invoice_value:.2f}, Taxable={taxable_value:.2f}'
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'export', 'export_type', export_type_clean)
            self._set_field(payload, 'export', 'invoice_number', invoice_number_clean)
            self._set_field(payload, 'export', 'invoice_date', invoice_date)
            self._set_field(payload, 'export', 'invoice_value', round(invoice_value, 2))
            
            if port_code_clean:
                self._set_field(payload, 'export', 'port_code', port_code_clean)
            
            if shipping_bill_number_clean:
                self._set_field(payload, 'export', 'shipping_bill_number', shipping_bill_number_clean)
            
            if shipping_bill_date_clean:
                self._set_field(payload, 'export', 'shipping_bill_date', shipping_bill_date_clean)
            
            self._set_field(payload, 'export', 'rate', round(rate, 2))
            self._set_field(payload, 'export', 'taxable_value', round(taxable_value, 2))
            self._set_field(payload, 'export', 'igst', round(igst, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"EXP: Processed {len(rows)} valid export invoices")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_expa(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build EXPA sheet (Amendments to Exports).
        Cross-references with EXP sheet for amendment validation.
        """
        sheet_name = self.sheet_mapping.get('expa')
        if not sheet_name:
            logger.info("EXPA sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Classification: Amendments to export invoices
        mask = (
            df['_is_amendment']
            & df['_is_export']
            & (~df['_is_credit_or_debit'])
        )
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"EXPA: Processing {mask.sum()} export amendment rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate Export Type (must be same as original)
            export_type = row.get('_export_type')
            if not export_type or pd.isna(export_type):
                self.validation_tracker.add_error(
                    row_num, 'Export Type',
                    'Missing for export amendment',
                    export_type
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            export_type_clean = str(export_type).strip().upper()
            if export_type_clean not in ['WPAY', 'WOPAY']:
                self.validation_tracker.add_error(
                    row_num, 'Export Type',
                    f'Invalid export type: {export_type_clean}',
                    export_type
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 2. Get original and revised invoice details
            original_invoice_number = row.get('_original_invoice_number')
            original_invoice_date = row.get('_original_invoice_date')
            revised_invoice_number = row.get('_invoice_number')
            revised_invoice_date = row.get('_invoice_date')
            
            # Validate original invoice data
            if not original_invoice_number or pd.isna(original_invoice_number):
                self.validation_tracker.add_error(
                    row_num, 'Original Invoice Number',
                    'Missing for amendment', original_invoice_number
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if not original_invoice_date or pd.isna(original_invoice_date):
                self.validation_tracker.add_error(
                    row_num, 'Original Invoice Date',
                    'Missing for amendment', original_invoice_date
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate revised invoice data
            if not self._validate_invoice_number(revised_invoice_number, row_num):
                self.validation_tracker.skipped_count += 1
                continue
            
            revised_invoice_number_clean = str(revised_invoice_number).strip().upper()
            
            if not revised_invoice_date or pd.isna(revised_invoice_date):
                self.validation_tracker.add_error(
                    row_num, 'Revised Invoice Date',
                    'Missing for amendment', revised_invoice_date
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate date ordering
            try:
                orig_date = pd.to_datetime(original_invoice_date)
                rev_date = pd.to_datetime(revised_invoice_date)
                if rev_date < orig_date:
                    self.validation_tracker.add_warning(
                        row_num, 'Invoice Date',
                        'Revised date is before original date',
                        'Accepted', f'Original: {orig_date}, Revised: {rev_date}',
                        'Accepted'
                    )
            except Exception as e:
                self.validation_tracker.add_warning(
                    row_num, 'Invoice Date',
                    f'Unable to validate date ordering: {e}',
                    'Accepted', '', ''
                )
            
            # Cross-reference: Check if original export exists in EXP
            original_invoice_key = f"{str(original_invoice_number).strip().upper()}|{original_invoice_date}"
            if original_invoice_key not in self.export_invoice_keys:
                self.validation_tracker.add_warning(
                    row_num, 'Original Invoice',
                    'Original export invoice not found in EXP sheet',
                    'Proceeding with amendment', original_invoice_key, 'Not found'
                )
            
            # 3. Validate Invoice Value
            invoice_value = row.get('_invoice_value')
            if not self._validate_amount_not_zero_negative(invoice_value, row_num, 'Invoice Value'):
                self.validation_tracker.skipped_count += 1
                continue
            
            invoice_value = float(invoice_value)
            
            # 4. Validate Rate
            rate = row.get('_rate')
            if rate is None or pd.isna(rate):
                self.validation_tracker.add_error(row_num, 'Rate', 'Missing', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(row_num, 'Rate', 'Invalid format', rate)
                self.validation_tracker.skipped_count += 1
                continue
            
            # Validate rate based on export type
            if export_type_clean == 'WOPAY' and rate != 0:
                self.validation_tracker.add_error(
                    row_num, 'Rate',
                    f'WOPAY exports must have 0% rate',
                    rate
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 5. Validate Taxable Value
            taxable_value = row.get('_taxable_value')
            if not self._validate_amount_not_zero_negative(taxable_value, row_num, 'Taxable Value'):
                self.validation_tracker.skipped_count += 1
                continue
            
            taxable_value = float(taxable_value)
            
            # 6. Get Port Code (optional, can be added/updated)
            port_code = row.get('_port_code')
            port_code_clean = None
            if port_code and not pd.isna(port_code):
                port_code_str = str(port_code).strip()
                if len(port_code_str) == 6 and port_code_str.isdigit():
                    port_code_clean = port_code_str
            
            # 7. Get Shipping Bill details (optional, can be added/updated)
            shipping_bill_number = row.get('_shipping_bill_number')
            shipping_bill_number_clean = None
            if shipping_bill_number and not pd.isna(shipping_bill_number):
                shipping_bill_number_clean = str(shipping_bill_number).strip().upper()[:20]
            
            shipping_bill_date = row.get('_shipping_bill_date')
            shipping_bill_date_clean = None
            if shipping_bill_date and not pd.isna(shipping_bill_date):
                try:
                    sb_date = pd.to_datetime(shipping_bill_date)
                    if sb_date <= pd.Timestamp.now() and sb_date >= pd.to_datetime(revised_invoice_date):
                        shipping_bill_date_clean = shipping_bill_date
                except Exception:
                    pass
            
            # 8. Calculate Tax - IGST only for exports
            igst = (taxable_value * rate) / 100
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'expa', 'export_type', export_type_clean)
            self._set_field(payload, 'expa', 'original_invoice_number', str(original_invoice_number).strip().upper())
            self._set_field(payload, 'expa', 'original_invoice_date', original_invoice_date)
            self._set_field(payload, 'expa', 'revised_invoice_number', revised_invoice_number_clean)
            self._set_field(payload, 'expa', 'revised_invoice_date', revised_invoice_date)
            self._set_field(payload, 'expa', 'invoice_value', round(invoice_value, 2))
            
            if port_code_clean:
                self._set_field(payload, 'expa', 'port_code', port_code_clean)
            
            if shipping_bill_number_clean:
                self._set_field(payload, 'expa', 'shipping_bill_number', shipping_bill_number_clean)
            
            if shipping_bill_date_clean:
                self._set_field(payload, 'expa', 'shipping_bill_date', shipping_bill_date_clean)
            
            self._set_field(payload, 'expa', 'rate', round(rate, 2))
            self._set_field(payload, 'expa', 'taxable_value', round(taxable_value, 2))
            self._set_field(payload, 'expa', 'igst', round(igst, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"EXPA: Processed {len(rows)} amendment records")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_at(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build AT sheet (Tax Liability on Advances - Unadjusted).
        Advances received before invoice issuance.
        """
        sheet_name = self.sheet_mapping.get('at')
        if not sheet_name:
            logger.info("AT sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Classification: Advance payments (not yet adjusted)
        # This requires detecting advance transactions from the data
        # For now, we'll check for specific doc_type or supply_type indicators
        mask = df.apply(self._detect_advance, axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"AT: Processing {mask.sum()} advance rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate Place of Supply (MANDATORY)
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(row_num, 'Place of Supply', 'Missing', pos_code)
                self.validation_tracker.skipped_count += 1
                continue
            
            pos_display = self._format_place_of_supply(pos_code)
            
            # Determine interstate status (need supplier state)
            # For now, assume we have _source_state_code
            source_state = row.get('_source_state_code', pos_code)
            is_interstate = (source_state != pos_code)
            
            # 2. Validate Rate (MANDATORY)
            rate = row.get('_rate')
            if not self._validate_tax_rate(rate, row_num, None):
                self.validation_tracker.skipped_count += 1
                continue
            
            rate = float(rate)
            
            # 3. Validate Gross Advance Received (MANDATORY, includes tax)
            gross_advance = row.get('_invoice_value')  # Gross advance is total amount received
            if not gross_advance or pd.isna(gross_advance):
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance',
                    'Gross advance received cannot be empty',
                    gross_advance
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                gross_advance = float(gross_advance)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance',
                    'Invalid format', gross_advance
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if gross_advance <= 0:
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance',
                    'Gross advance must be positive',
                    gross_advance
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 4. Calculate Taxable Advance from Gross Advance
            # Formula: Taxable_Advance = Gross_Advance / (1 + Rate/100)
            taxable_advance = gross_advance / (1 + rate / 100)
            
            # Verify calculation makes sense
            if taxable_advance >= gross_advance:
                self.validation_tracker.add_error(
                    row_num, 'Taxable Advance',
                    'Taxable advance cannot be >= gross advance (tax should be extracted)',
                    f'Gross={gross_advance:.2f}, Calculated Taxable={taxable_advance:.2f}'
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 5. Calculate Tax on Advance
            if is_interstate:
                igst = (taxable_advance * rate) / 100
                cgst = 0
                sgst = 0
            else:
                igst = 0
                cgst = (taxable_advance * rate / 2) / 100
                sgst = (taxable_advance * rate / 2) / 100
            
            # 6. Cess (optional)
            cess = row.get('_cess_amount', 0)
            if cess and not pd.isna(cess):
                try:
                    cess = float(cess)
                except (ValueError, TypeError):
                    cess = 0
            else:
                cess = 0
            
            # 7. Verify total calculation
            tax_total = igst + cgst + sgst + cess
            calculated_gross = taxable_advance + tax_total
            
            if abs(calculated_gross - gross_advance) > 0.02:
                self.validation_tracker.add_warning(
                    row_num, 'Tax Calculation',
                    f'Verification mismatch: Provided Gross={gross_advance:.2f}, Calculated={calculated_gross:.2f}',
                    'Using provided gross', calculated_gross, gross_advance
                )
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'at', 'place_of_supply', pos_display)
            self._set_field(payload, 'at', 'rate', round(rate, 2))
            self._set_field(payload, 'at', 'gross_advance_received', round(gross_advance, 2))
            self._set_field(payload, 'at', 'taxable_advance', round(taxable_advance, 2))
            self._set_field(payload, 'at', 'igst', round(igst, 2))
            self._set_field(payload, 'at', 'cgst', round(cgst, 2))
            self._set_field(payload, 'at', 'sgst', round(sgst, 2))
            self._set_field(payload, 'at', 'cess', round(cess, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"AT: Processed {len(rows)} advance records")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ata(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ATA sheet (Amended Tax Liability on Advances).
        Amendments to AT sheet entries.
        """
        sheet_name = self.sheet_mapping.get('ata')
        if not sheet_name:
            logger.info("ATA sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Classification: Amendments to advance records
        mask = df.apply(lambda row: self._detect_advance(row) and row.get('_is_amendment', False), axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"ATA: Processing {mask.sum()} advance amendment rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate Financial Year (MANDATORY for amendments)
            financial_year = self._get_value(row, 'financial_year')
            if not financial_year or pd.isna(financial_year):
                self.validation_tracker.add_error(
                    row_num, 'Financial Year',
                    'Financial year required for advance amendment',
                    financial_year
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            fy_str = str(financial_year).strip()
            # Validate FY format (e.g., "2023-24")
            if '-' not in fy_str or len(fy_str) < 7:
                self.validation_tracker.add_warning(
                    row_num, 'Financial Year',
                    f'Unusual financial year format: {fy_str}',
                    'Accepted', fy_str, fy_str
                )
            
            # 2. Validate Original Month (MANDATORY)
            original_month = self._get_value(row, 'original_month')
            if not original_month or pd.isna(original_month):
                self.validation_tracker.add_error(
                    row_num, 'Original Month',
                    'Original month required for advance amendment',
                    original_month
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                month_num = int(original_month)
                if month_num < 1 or month_num > 12:
                    self.validation_tracker.add_error(
                        row_num, 'Original Month',
                        f'Invalid month: {month_num}. Must be 1-12',
                        original_month
                    )
                    self.validation_tracker.skipped_count += 1
                    continue
            except (ValueError, TypeError):
                self.validation_tracker.add_error(
                    row_num, 'Original Month',
                    f'Invalid month format: {original_month}',
                    original_month
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 3. Validate Place of Supply (MANDATORY)
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(row_num, 'Place of Supply', 'Missing', pos_code)
                self.validation_tracker.skipped_count += 1
                continue
            
            pos_display = self._format_place_of_supply(pos_code)
            source_state = row.get('_source_state_code', pos_code)
            is_interstate = (source_state != pos_code)
            
            # 4. Validate Rate (MANDATORY)
            rate = row.get('_rate')
            if not self._validate_tax_rate(rate, row_num, None):
                self.validation_tracker.skipped_count += 1
                continue
            
            rate = float(rate)
            
            # 5. Validate Gross Advance (Revised amount)
            gross_advance = row.get('_invoice_value')
            if not gross_advance or pd.isna(gross_advance):
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance',
                    'Revised gross advance cannot be empty',
                    gross_advance
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                gross_advance = float(gross_advance)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance',
                    'Invalid format', gross_advance
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if gross_advance <= 0:
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance',
                    'Gross advance must be positive',
                    gross_advance
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 6. Calculate Taxable Advance
            taxable_advance = gross_advance / (1 + rate / 100)
            
            # 7. Calculate Tax
            if is_interstate:
                igst = (taxable_advance * rate) / 100
                cgst = 0
                sgst = 0
            else:
                igst = 0
                cgst = (taxable_advance * rate / 2) / 100
                sgst = (taxable_advance * rate / 2) / 100
            
            # 8. Cess (optional)
            cess = row.get('_cess_amount', 0)
            if cess and not pd.isna(cess):
                try:
                    cess = float(cess)
                except (ValueError, TypeError):
                    cess = 0
            else:
                cess = 0
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ata', 'financial_year', fy_str)
            self._set_field(payload, 'ata', 'original_month', str(month_num))
            self._set_field(payload, 'ata', 'place_of_supply', pos_display)
            self._set_field(payload, 'ata', 'rate', round(rate, 2))
            self._set_field(payload, 'ata', 'gross_advance_received', round(gross_advance, 2))
            self._set_field(payload, 'ata', 'taxable_advance', round(taxable_advance, 2))
            self._set_field(payload, 'ata', 'igst', round(igst, 2))
            self._set_field(payload, 'ata', 'cgst', round(cgst, 2))
            self._set_field(payload, 'ata', 'sgst', round(sgst, 2))
            self._set_field(payload, 'ata', 'cess', round(cess, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"ATA: Processed {len(rows)} advance amendment records")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_atadj(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ATADJ sheet (Advance Adjustments).
        When invoice is issued against advance, adjust the tax liability.
        """
        sheet_name = self.sheet_mapping.get('atadj')
        if not sheet_name:
            logger.info("ATADJ sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Classification: Advance adjustments (invoice issued against advance)
        # Look for "adjustment" or "adjusted" keywords in doc_type/supply_type
        mask = df.apply(lambda row: self._detect_advance_adjustment(row), axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"ATADJ: Processing {mask.sum()} advance adjustment rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate Place of Supply (MANDATORY)
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(row_num, 'Place of Supply', 'Missing', pos_code)
                self.validation_tracker.skipped_count += 1
                continue
            
            pos_display = self._format_place_of_supply(pos_code)
            source_state = row.get('_source_state_code', pos_code)
            is_interstate = (source_state != pos_code)
            
            # 2. Validate Rate (MANDATORY)
            rate = row.get('_rate')
            if not self._validate_tax_rate(rate, row_num, None):
                self.validation_tracker.skipped_count += 1
                continue
            
            rate = float(rate)
            
            # 3. Validate Gross Advance Adjusted (MANDATORY)
            gross_adjusted = row.get('_invoice_value')
            if not gross_adjusted or pd.isna(gross_adjusted):
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance Adjusted',
                    'Gross advance adjusted cannot be empty',
                    gross_adjusted
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                gross_adjusted = float(gross_adjusted)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance Adjusted',
                    'Invalid format', gross_adjusted
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if gross_adjusted < 0:
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance Adjusted',
                    'Cannot be negative',
                    gross_adjusted
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if gross_adjusted == 0:
                self.validation_tracker.add_warning(
                    row_num, 'Gross Advance Adjusted',
                    'Zero adjustment recorded',
                    'Accepted', 0, 0
                )
            
            # 4. Calculate Taxable Advance Adjusted
            if gross_adjusted > 0:
                taxable_adjusted = gross_adjusted / (1 + rate / 100)
            else:
                taxable_adjusted = 0
            
            # 5. Calculate Tax Adjustment
            if is_interstate:
                igst = (taxable_adjusted * rate) / 100
                cgst = 0
                sgst = 0
            else:
                igst = 0
                cgst = (taxable_adjusted * rate / 2) / 100
                sgst = (taxable_adjusted * rate / 2) / 100
            
            # 6. Cess (optional)
            cess = row.get('_cess_amount', 0)
            if cess and not pd.isna(cess):
                try:
                    cess = float(cess)
                except (ValueError, TypeError):
                    cess = 0
            else:
                cess = 0
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'atadj', 'place_of_supply', pos_display)
            self._set_field(payload, 'atadj', 'rate', round(rate, 2))
            self._set_field(payload, 'atadj', 'gross_advance_adjusted', round(gross_adjusted, 2))
            self._set_field(payload, 'atadj', 'taxable_advance_adjusted', round(taxable_adjusted, 2))
            self._set_field(payload, 'atadj', 'igst', round(igst, 2))
            self._set_field(payload, 'atadj', 'cgst', round(cgst, 2))
            self._set_field(payload, 'atadj', 'sgst', round(sgst, 2))
            self._set_field(payload, 'atadj', 'cess', round(cess, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"ATADJ: Processed {len(rows)} advance adjustment records")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_atadja(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ATADJA sheet (Amended Advance Adjustments).
        Amendments to ATADJ sheet entries.
        """
        sheet_name = self.sheet_mapping.get('atadja')
        if not sheet_name:
            logger.info("ATADJA sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Classification: Amendments to advance adjustment records
        mask = df.apply(lambda row: self._detect_advance_adjustment(row) and row.get('_is_amendment', False), axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"ATADJA: Processing {mask.sum()} advance adjustment amendment rows")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate Financial Year (MANDATORY)
            financial_year = self._get_value(row, 'financial_year')
            if not financial_year or pd.isna(financial_year):
                self.validation_tracker.add_error(
                    row_num, 'Financial Year',
                    'Financial year required for adjustment amendment',
                    financial_year
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            fy_str = str(financial_year).strip()
            
            # 2. Validate Original Month (MANDATORY)
            original_month = self._get_value(row, 'original_month')
            if not original_month or pd.isna(original_month):
                self.validation_tracker.add_error(
                    row_num, 'Original Month',
                    'Original month required',
                    original_month
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                month_num = int(original_month)
                if month_num < 1 or month_num > 12:
                    self.validation_tracker.add_error(
                        row_num, 'Original Month',
                        f'Invalid month: {month_num}',
                        original_month
                    )
                    self.validation_tracker.skipped_count += 1
                    continue
            except (ValueError, TypeError):
                self.validation_tracker.add_error(
                    row_num, 'Original Month',
                    f'Invalid month format: {original_month}',
                    original_month
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 3. Validate Place of Supply (MANDATORY)
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(row_num, 'Place of Supply', 'Missing', pos_code)
                self.validation_tracker.skipped_count += 1
                continue
            
            pos_display = self._format_place_of_supply(pos_code)
            source_state = row.get('_source_state_code', pos_code)
            is_interstate = (source_state != pos_code)
            
            # 4. Validate Rate (MANDATORY)
            rate = row.get('_rate')
            if not self._validate_tax_rate(rate, row_num, None):
                self.validation_tracker.skipped_count += 1
                continue
            
            rate = float(rate)
            
            # 5. Validate Gross Advance Adjusted (Revised amount)
            gross_adjusted = row.get('_invoice_value')
            if gross_adjusted is None or pd.isna(gross_adjusted):
                gross_adjusted = 0
            
            try:
                gross_adjusted = float(gross_adjusted)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance Adjusted',
                    'Invalid format', gross_adjusted
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if gross_adjusted < 0:
                self.validation_tracker.add_error(
                    row_num, 'Gross Advance Adjusted',
                    'Cannot be negative',
                    gross_adjusted
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 6. Calculate Taxable Advance Adjusted
            if gross_adjusted > 0:
                taxable_adjusted = gross_adjusted / (1 + rate / 100)
            else:
                taxable_adjusted = 0
            
            # 7. Calculate Tax
            if is_interstate:
                igst = (taxable_adjusted * rate) / 100
                cgst = 0
                sgst = 0
            else:
                igst = 0
                cgst = (taxable_adjusted * rate / 2) / 100
                sgst = (taxable_adjusted * rate / 2) / 100
            
            # 8. Cess (optional)
            cess = row.get('_cess_amount', 0)
            if cess and not pd.isna(cess):
                try:
                    cess = float(cess)
                except (ValueError, TypeError):
                    cess = 0
            else:
                cess = 0
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'atadja', 'financial_year', fy_str)
            self._set_field(payload, 'atadja', 'original_month', str(month_num))
            self._set_field(payload, 'atadja', 'place_of_supply', pos_display)
            self._set_field(payload, 'atadja', 'rate', round(rate, 2))
            self._set_field(payload, 'atadja', 'gross_advance_adjusted', round(gross_adjusted, 2))
            self._set_field(payload, 'atadja', 'igst', round(igst, 2))
            self._set_field(payload, 'atadja', 'cgst', round(cgst, 2))
            self._set_field(payload, 'atadja', 'sgst', round(sgst, 2))
            self._set_field(payload, 'atadja', 'cess', round(cess, 2))
            
            if payload:
                rows.append(payload)
                self.validation_tracker.valid_count += 1
        
        logger.info(f"ATADJA: Processed {len(rows)} adjustment amendment records")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_exemp(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build EXEMP sheet (Nil Rated, Exempted, Non-GST Supplies).
        Aggregates supplies by category with proper validation.
        """
        sheet_name = self.sheet_mapping.get('exemp')
        if not sheet_name:
            logger.info("EXEMP sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Classification: Nil rated, exempted, or non-GST supplies
        # These are typically identified by rate=0 and supply category
        mask = df.apply(lambda row: self._detect_exemp_supply(row), axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        logger.info(f"EXEMP: Processing {mask.sum()} exempted/nil-rated supply rows")
        
        # Aggregate by category
        category_totals = {
            'Nil Rated': 0.0,
            'Exempted': 0.0,
            'Non-GST': 0.0
        }
        
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Determine supply category
            supply_category = self._get_exemp_category(row)
            if not supply_category:
                self.validation_tracker.add_error(
                    row_num, 'Supply Category',
                    'Unable to determine supply category (Nil Rated/Exempted/Non-GST)',
                    ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 2. Validate supply value
            supply_value = row.get('_invoice_value') or row.get('_taxable_value', 0)
            if supply_value is None or pd.isna(supply_value):
                supply_value = 0
            
            try:
                supply_value = float(supply_value)
            except (ValueError, TypeError):
                self.validation_tracker.add_error(
                    row_num, 'Supply Value',
                    'Invalid format', supply_value
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            if supply_value < 0:
                self.validation_tracker.add_error(
                    row_num, 'Supply Value',
                    'Cannot be negative',
                    supply_value
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 3. Prevent double reporting (check if already in B2B/B2CL)
            invoice_key = f"{row.get('_invoice_number', '')}_{row.get('_invoice_date', '')}"
            if invoice_key in self.seen_invoice_keys or invoice_key in self.b2cl_invoice_keys:
                self.validation_tracker.add_warning(
                    row_num, 'Double Reporting',
                    'Item already reported in B2B/B2CL, skipping from EXEMP',
                    'Skipped', invoice_key, ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 4. Aggregate by category
            category_totals[supply_category] += supply_value
            self.validation_tracker.valid_count += 1
        
        # Build output rows (one per category with values)
        rows: List[Dict[str, object]] = []
        
        if category_totals['Nil Rated'] > 0:
            payload: Dict[str, object] = {}
            self._set_field(payload, 'exemp', 'description', 'Nil Rated Supplies')
            self._set_field(payload, 'exemp', 'nil_rated_supplies', round(category_totals['Nil Rated'], 2))
            self._set_field(payload, 'exemp', 'exempted_supplies', 0)
            self._set_field(payload, 'exemp', 'non_gst_supplies', 0)
            if payload:
                rows.append(payload)
        
        if category_totals['Exempted'] > 0:
            payload: Dict[str, object] = {}
            self._set_field(payload, 'exemp', 'description', 'Exempted')
            self._set_field(payload, 'exemp', 'nil_rated_supplies', 0)
            self._set_field(payload, 'exemp', 'exempted_supplies', round(category_totals['Exempted'], 2))
            self._set_field(payload, 'exemp', 'non_gst_supplies', 0)
            if payload:
                rows.append(payload)
        
        if category_totals['Non-GST'] > 0:
            payload: Dict[str, object] = {}
            self._set_field(payload, 'exemp', 'description', 'Non GST Supplies')
            self._set_field(payload, 'exemp', 'nil_rated_supplies', 0)
            self._set_field(payload, 'exemp', 'exempted_supplies', 0)
            self._set_field(payload, 'exemp', 'non_gst_supplies', round(category_totals['Non-GST'], 2))
            if payload:
                rows.append(payload)
        
        logger.info(f"EXEMP: Aggregated into {len(rows)} category rows - "
                   f"Nil Rated: ₹{category_totals['Nil Rated']:.2f}, "
                   f"Exempted: ₹{category_totals['Exempted']:.2f}, "
                   f"Non-GST: ₹{category_totals['Non-GST']:.2f}")
        
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_hsn(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build HSN(B2B) sheet - HSN Summary of B2B Supplies.
        Aggregates B2B line items by HSN code + Rate.
        """
        sheet_name = self.sheet_mapping.get('hsn')
        if not sheet_name:
            logger.info("HSN sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Filter to B2B invoices only (has valid GSTIN, not export/amendment/CN/DN)
        mask = (
            df['_has_valid_gstin'] &
            (~df['_is_export']) &
            (~df['_is_credit_or_debit']) &
            (~df['_is_amendment'])
        )
        
        if not mask.any():
            logger.info("HSN: No B2B data available for aggregation")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"HSN: Aggregating {mask.sum()} B2B line items by HSN+Rate")
        
        # Aggregation dictionary: key = HSN|Rate|POS
        aggregation_dict: Dict[str, Dict[str, object]] = {}
        
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate HSN Code
            hsn_code = self._get_value(row, 'hsn_code')
            if not hsn_code or pd.isna(hsn_code):
                # HSN is mandatory from May 2021 onwards
                # For now, skip rows without HSN (can add date check later)
                self.validation_tracker.add_warning(
                    row_num, 'HSN Code',
                    'HSN code missing, skipping from HSN summary',
                    'Skipped', '', ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            hsn_str = str(hsn_code).strip()
            
            # Validate HSN format (4-8 digits)
            if not hsn_str.isdigit() or len(hsn_str) < 4 or len(hsn_str) > 8:
                self.validation_tracker.add_error(
                    row_num, 'HSN Code',
                    f'HSN must be 4-8 numeric digits',
                    hsn_code
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Determine if goods or services (99XXXX = services)
            is_service = hsn_str.startswith('99')
            
            # 2. Get Description (optional, can be looked up)
            description = self._get_value(row, 'description') or ''
            if description and not pd.isna(description):
                description = str(description).strip()[:255]
            else:
                description = ''
            
            # 3. Validate UQC (mandatory for goods, optional for services)
            uqc = self._get_value(row, 'uqc') or ''
            if uqc and not pd.isna(uqc):
                uqc_str = str(uqc).strip().upper()
                if not self._validate_uqc(uqc_str, row_num):
                    uqc_str = 'NOS'  # Default to NOS if invalid
            else:
                if not is_service:
                    self.validation_tracker.add_warning(
                        row_num, 'UQC',
                        'UQC missing for goods, defaulting to NOS',
                        'Defaulting', '', 'NOS'
                    )
                uqc_str = 'NOS' if not is_service else ''
            
            # 4. Get Quantity (mandatory for goods, optional for services)
            quantity = self._get_value(row, 'quantity', 0)
            if quantity and not pd.isna(quantity):
                try:
                    quantity = float(quantity)
                except (ValueError, TypeError):
                    quantity = 0
            else:
                quantity = 0
            
            if quantity < 0:
                self.validation_tracker.add_warning(
                    row_num, 'Quantity',
                    'Negative quantity, setting to 0',
                    'Corrected', quantity, 0
                )
                quantity = 0
            
            # 5. Get Rate
            rate = row.get('_rate')
            if rate is None or pd.isna(rate):
                self.validation_tracker.add_warning(
                    row_num, 'Rate',
                    'Rate missing, skipping from HSN summary',
                    'Skipped', '', ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
            except (ValueError, TypeError):
                self.validation_tracker.skipped_count += 1
                continue
            
            # 6. Get Taxable Value (mandatory)
            taxable_value = row.get('_taxable_value')
            if taxable_value is None or pd.isna(taxable_value):
                self.validation_tracker.add_warning(
                    row_num, 'Taxable Value',
                    'Taxable value missing, skipping from HSN summary',
                    'Skipped', '', ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                taxable_value = float(taxable_value)
            except (ValueError, TypeError):
                self.validation_tracker.skipped_count += 1
                continue
            
            if taxable_value < 0:
                self.validation_tracker.add_warning(
                    row_num, 'Taxable Value',
                    'Negative taxable value, skipping',
                    'Skipped', taxable_value, ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 7. Get Tax Amounts
            igst = row.get('_igst_amount', 0)
            cgst = row.get('_cgst_amount', 0)
            sgst = row.get('_sgst_amount', 0)
            cess = row.get('_cess_amount', 0)
            
            for tax_name, tax_val in [('igst', igst), ('cgst', cgst), ('sgst', sgst), ('cess', cess)]:
                if tax_val and not pd.isna(tax_val):
                    try:
                        if tax_name == 'igst':
                            igst = float(tax_val)
                        elif tax_name == 'cgst':
                            cgst = float(tax_val)
                        elif tax_name == 'sgst':
                            sgst = float(tax_val)
                        elif tax_name == 'cess':
                            cess = float(tax_val)
                    except (ValueError, TypeError):
                        pass
            
            # Default to 0 if still None
            igst = igst if igst and not pd.isna(igst) else 0
            cgst = cgst if cgst and not pd.isna(cgst) else 0
            sgst = sgst if sgst and not pd.isna(sgst) else 0
            cess = cess if cess and not pd.isna(cess) else 0
            
            # 8. Get Place of Supply for aggregation key
            pos_code = row.get('_pos_code', '99')
            
            # 9. Create aggregation key: HSN|Rate|POS
            agg_key = f"{hsn_str}|{rate}|{pos_code}"
            
            # 10. Aggregate
            if agg_key in aggregation_dict:
                aggregation_dict[agg_key]['quantity'] += quantity
                aggregation_dict[agg_key]['taxable_value'] += taxable_value
                aggregation_dict[agg_key]['igst'] += igst
                aggregation_dict[agg_key]['cgst'] += cgst
                aggregation_dict[agg_key]['sgst'] += sgst
                aggregation_dict[agg_key]['cess'] += cess
            else:
                aggregation_dict[agg_key] = {
                    'hsn': hsn_str,
                    'description': description,
                    'uqc': uqc_str,
                    'quantity': quantity,
                    'rate': rate,
                    'taxable_value': taxable_value,
                    'igst': igst,
                    'cgst': cgst,
                    'sgst': sgst,
                    'cess': cess,
                    'is_service': is_service
                }
            
            self.validation_tracker.valid_count += 1
        
        # Build output rows
        rows: List[Dict[str, object]] = []
        for agg_key, agg_data in aggregation_dict.items():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'hsn', 'hsn_code', agg_data['hsn'])
            self._set_field(payload, 'hsn', 'description', agg_data['description'])
            self._set_field(payload, 'hsn', 'uqc', agg_data['uqc'])
            self._set_field(payload, 'hsn', 'quantity', round(agg_data['quantity'], 2))
            self._set_field(payload, 'hsn', 'total_value', 0)  # Ignored from May 2021
            self._set_field(payload, 'hsn', 'rate', round(agg_data['rate'], 2))
            self._set_field(payload, 'hsn', 'taxable_value', round(agg_data['taxable_value'], 2))
            self._set_field(payload, 'hsn', 'igst_amount', round(agg_data['igst'], 2))
            self._set_field(payload, 'hsn', 'cgst_amount', round(agg_data['cgst'], 2))
            self._set_field(payload, 'hsn', 'sgst_amount', round(agg_data['sgst'], 2))
            self._set_field(payload, 'hsn', 'cess_amount', round(agg_data['cess'], 2))
            
            if payload:
                rows.append(payload)
        
        logger.info(f"HSN: Aggregated {len(aggregation_dict)} unique HSN+Rate combinations from B2B data")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_hsnb2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build HSN(B2C) sheet - HSN Summary of B2C Supplies.
        Aggregates B2CL and B2CS line items by HSN code + Rate.
        B2C is always interstate (IGST only).
        """
        sheet_name = self.sheet_mapping.get('hsnb2c')
        if not sheet_name:
            logger.info("HSN(B2C) sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # Filter to B2C invoices: B2CL (large) or B2CS (small)
        mask = (
            (df['_is_large_b2cl'] | (~df['_has_valid_gstin'])) &
            (~df['_is_export']) &
            (~df['_is_credit_or_debit']) &
            (~df['_is_amendment'])
        )
        
        if not mask.any():
            logger.info("HSN(B2C): No B2C data available for aggregation")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"HSN(B2C): Aggregating {mask.sum()} B2C line items by HSN+Rate")
        
        # Aggregation dictionary: key = HSN|Rate (no POS for B2C)
        aggregation_dict: Dict[str, Dict[str, object]] = {}
        
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate HSN Code
            hsn_code = self._get_value(row, 'hsn_code')
            if not hsn_code or pd.isna(hsn_code):
                self.validation_tracker.add_warning(
                    row_num, 'HSN Code',
                    'HSN code missing, skipping from HSN(B2C) summary',
                    'Skipped', '', ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            hsn_str = str(hsn_code).strip()
            
            # Validate HSN format (4-8 digits)
            if not hsn_str.isdigit() or len(hsn_str) < 4 or len(hsn_str) > 8:
                self.validation_tracker.add_error(
                    row_num, 'HSN Code',
                    f'HSN must be 4-8 numeric digits',
                    hsn_code
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # Determine if goods or services
            is_service = hsn_str.startswith('99')
            
            # 2. Get Description
            description = self._get_value(row, 'description') or ''
            if description and not pd.isna(description):
                description = str(description).strip()[:255]
            else:
                description = ''
            
            # 3. UQC (optional for B2C, often not tracked)
            uqc = self._get_value(row, 'uqc') or ''
            if uqc and not pd.isna(uqc):
                uqc_str = str(uqc).strip().upper()
            else:
                uqc_str = ''
            
            # 4. Quantity (typically 0 for B2C)
            quantity = self._get_value(row, 'quantity', 0)
            if quantity and not pd.isna(quantity):
                try:
                    quantity = float(quantity)
                except (ValueError, TypeError):
                    quantity = 0
            else:
                quantity = 0
            
            # 5. Get Rate
            rate = row.get('_rate')
            if rate is None or pd.isna(rate):
                self.validation_tracker.add_warning(
                    row_num, 'Rate',
                    'Rate missing, skipping from HSN(B2C) summary',
                    'Skipped', '', ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
            except (ValueError, TypeError):
                self.validation_tracker.skipped_count += 1
                continue
            
            # 6. Get Taxable Value
            taxable_value = row.get('_taxable_value')
            if taxable_value is None or pd.isna(taxable_value):
                self.validation_tracker.add_warning(
                    row_num, 'Taxable Value',
                    'Taxable value missing, skipping from HSN(B2C) summary',
                    'Skipped', '', ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                taxable_value = float(taxable_value)
            except (ValueError, TypeError):
                self.validation_tracker.skipped_count += 1
                continue
            
            if taxable_value < 0:
                self.validation_tracker.skipped_count += 1
                continue
            
            # 7. Get Tax Amounts (B2C is IGST only)
            igst = row.get('_igst_amount', 0)
            cess = row.get('_cess_amount', 0)
            
            igst = float(igst) if igst and not pd.isna(igst) else 0
            cess = float(cess) if cess and not pd.isna(cess) else 0
            
            # 8. Create aggregation key: HSN|Rate (no POS for B2C)
            agg_key = f"{hsn_str}|{rate}"
            
            # 9. Aggregate
            if agg_key in aggregation_dict:
                aggregation_dict[agg_key]['quantity'] += quantity
                aggregation_dict[agg_key]['taxable_value'] += taxable_value
                aggregation_dict[agg_key]['igst'] += igst
                aggregation_dict[agg_key]['cess'] += cess
            else:
                aggregation_dict[agg_key] = {
                    'hsn': hsn_str,
                    'description': description,
                    'uqc': uqc_str,
                    'quantity': quantity,
                    'rate': rate,
                    'taxable_value': taxable_value,
                    'igst': igst,
                    'cgst': 0,  # B2C always IGST only
                    'sgst': 0,  # B2C always IGST only
                    'cess': cess,
                    'is_service': is_service
                }
            
            self.validation_tracker.valid_count += 1
        
        # Build output rows
        rows: List[Dict[str, object]] = []
        for agg_key, agg_data in aggregation_dict.items():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'hsnb2c', 'hsn_code', agg_data['hsn'])
            self._set_field(payload, 'hsnb2c', 'description', agg_data['description'])
            self._set_field(payload, 'hsnb2c', 'uqc', agg_data['uqc'])
            self._set_field(payload, 'hsnb2c', 'quantity', round(agg_data['quantity'], 2) if agg_data['quantity'] > 0 else 0)
            self._set_field(payload, 'hsnb2c', 'total_value', 0)
            self._set_field(payload, 'hsnb2c', 'rate', round(agg_data['rate'], 2))
            self._set_field(payload, 'hsnb2c', 'taxable_value', round(agg_data['taxable_value'], 2))
            self._set_field(payload, 'hsnb2c', 'igst_amount', round(agg_data['igst'], 2))
            self._set_field(payload, 'hsnb2c', 'cgst_amount', 0)  # Always 0 for B2C
            self._set_field(payload, 'hsnb2c', 'sgst_amount', 0)  # Always 0 for B2C
            self._set_field(payload, 'hsnb2c', 'cess_amount', round(agg_data['cess'], 2))
            
            if payload:
                rows.append(payload)
        
        logger.info(f"HSN(B2C): Aggregated {len(aggregation_dict)} unique HSN+Rate combinations from B2C data")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_docs(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build DOCS sheet - List of Documents Issued.
        Tracks document series issued during tax period.
        """
        sheet_name = self.sheet_mapping.get('docs')
        if not sheet_name:
            logger.info("DOCS sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # DOCS data is typically provided separately, not derived from invoices
        # Check if we have dedicated DOCS data
        mask = df.apply(lambda row: self._detect_docs_entry(row), axis=1)
        
        if not mask.any():
            logger.info("DOCS: No document series data available")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"DOCS: Processing {mask.sum()} document series entries")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            is_valid = True
            
            # 1. Validate Nature of Document (MANDATORY)
            nature_of_doc = self._get_value(row, 'nature_of_document')
            if not nature_of_doc or pd.isna(nature_of_doc):
                self.validation_tracker.add_error(
                    row_num, 'Nature of Document',
                    'Nature of document cannot be empty',
                    nature_of_doc
                )
                is_valid = False
            else:
                nod_str = str(nature_of_doc).strip().upper()
                valid_doc_types = ['INV', 'DBN', 'CDN', 'BIL', 'EWB']
                
                # Try to map common variations
                nod_mapping = {
                    'INVOICE': 'INV',
                    'DEBIT NOTE': 'DBN',
                    'CREDIT NOTE': 'CDN',
                    'BILL OF SUPPLY': 'BIL',
                    'E-WAY BILL': 'EWB',
                    'EWAY BILL': 'EWB'
                }
                
                nod_output = nod_mapping.get(nod_str, nod_str)
                
                if nod_output not in valid_doc_types:
                    self.validation_tracker.add_error(
                        row_num, 'Nature of Document',
                        f'Invalid document type: {nature_of_doc}',
                        nature_of_doc
                    )
                    is_valid = False
            
            # 2. Validate Sr. No From (MANDATORY)
            sr_from = self._get_value(row, 'sr_no_from')
            if not sr_from or pd.isna(sr_from):
                self.validation_tracker.add_error(
                    row_num, 'Sr. No From',
                    'Series start number cannot be empty',
                    sr_from
                )
                is_valid = False
            else:
                sr_from_str = str(sr_from).strip()
                sr_from_numeric = self._extract_numeric_portion(sr_from_str)
                
                if sr_from_numeric is None:
                    self.validation_tracker.add_error(
                        row_num, 'Sr. No From',
                        'Series start must contain numeric portion',
                        sr_from
                    )
                    is_valid = False
            
            # 3. Validate Sr. No To (MANDATORY)
            sr_to = self._get_value(row, 'sr_no_to')
            if not sr_to or pd.isna(sr_to):
                self.validation_tracker.add_error(
                    row_num, 'Sr. No To',
                    'Series end number cannot be empty',
                    sr_to
                )
                is_valid = False
            else:
                sr_to_str = str(sr_to).strip()
                sr_to_numeric = self._extract_numeric_portion(sr_to_str)
                
                if sr_to_numeric is None:
                    self.validation_tracker.add_error(
                        row_num, 'Sr. No To',
                        'Series end must contain numeric portion',
                        sr_to
                    )
                    is_valid = False
                elif sr_from_numeric is not None and sr_to_numeric < sr_from_numeric:
                    self.validation_tracker.add_error(
                        row_num, 'Sr. No To',
                        f'Series end ({sr_to_numeric}) cannot be less than start ({sr_from_numeric})',
                        sr_to
                    )
                    is_valid = False
            
            # 4. Validate Total Number (MANDATORY)
            total_number = self._get_value(row, 'total_number')
            if total_number is None or pd.isna(total_number):
                self.validation_tracker.add_error(
                    row_num, 'Total Number',
                    'Total number of documents cannot be empty',
                    total_number
                )
                is_valid = False
            else:
                try:
                    total_num = int(total_number)
                    if total_num <= 0:
                        self.validation_tracker.add_error(
                            row_num, 'Total Number',
                            'Total number must be positive',
                            total_number
                        )
                        is_valid = False
                    elif sr_from_numeric is not None and sr_to_numeric is not None:
                        series_range = sr_to_numeric - sr_from_numeric + 1
                        if total_num != series_range:
                            self.validation_tracker.add_warning(
                                row_num, 'Total Number',
                                f'Total number ({total_num}) does not match series range ({series_range})',
                                'Using provided value', total_number, total_num
                            )
                except (ValueError, TypeError):
                    self.validation_tracker.add_error(
                        row_num, 'Total Number',
                        'Total number is not numeric',
                        total_number
                    )
                    is_valid = False
            
            # 5. Validate Cancelled (OPTIONAL)
            cancelled = self._get_value(row, 'cancelled', 0)
            if cancelled and not pd.isna(cancelled):
                try:
                    cancelled_num = int(cancelled)
                    if cancelled_num < 0:
                        self.validation_tracker.add_error(
                            row_num, 'Cancelled',
                            'Cancelled count cannot be negative',
                            cancelled
                        )
                        is_valid = False
                    elif total_num and cancelled_num > total_num:
                        self.validation_tracker.add_error(
                            row_num, 'Cancelled',
                            f'Cancelled ({cancelled_num}) cannot exceed total ({total_num})',
                            cancelled
                        )
                        is_valid = False
                except (ValueError, TypeError):
                    self.validation_tracker.add_warning(
                        row_num, 'Cancelled',
                        'Cancelled count is not numeric, setting to 0',
                        'Defaulting', cancelled, 0
                    )
                    cancelled_num = 0
            else:
                cancelled_num = 0
            
            if not is_valid:
                self.validation_tracker.skipped_count += 1
                continue
            
            self.validation_tracker.valid_count += 1
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'docs', 'nature_of_document', nod_output)
            self._set_field(payload, 'docs', 'sr_no_from', sr_from_str)
            self._set_field(payload, 'docs', 'sr_no_to', sr_to_str)
            self._set_field(payload, 'docs', 'total_number', total_num)
            self._set_field(payload, 'docs', 'cancelled', cancelled_num)
            
            if payload:
                rows.append(payload)
        
        logger.info(f"DOCS: Processed {len(rows)} document series entries")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_eco(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECO sheet - Supplies Through E-Commerce Operator.
        Tracks supplies where tax collection/payment is done by ECO.
        """
        sheet_name = self.sheet_mapping.get('eco')
        if not sheet_name:
            logger.info("ECO sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # ECO data is typically identified by nature of supply (TCS or 9(5))
        mask = df.apply(lambda row: self._detect_eco_supply(row), axis=1)
        
        if not mask.any():
            logger.info("ECO: No e-commerce operator supply data available")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"ECO: Processing {mask.sum()} e-commerce operator supply entries")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            is_valid = True
            
            # 1. Validate Nature of Supply (MANDATORY)
            nature_of_supply = self._get_value(row, 'nature_of_supply')
            if not nature_of_supply or pd.isna(nature_of_supply):
                self.validation_tracker.add_error(
                    row_num, 'Nature of Supply',
                    'Nature of supply cannot be empty',
                    nature_of_supply
                )
                is_valid = False
            else:
                nos_str = str(nature_of_supply).strip().upper()
                
                # Normalize format
                nos_mapping = {
                    'TCS': 'TCS',
                    'SEC_52': 'TCS',
                    'SECTION 52': 'TCS',
                    '9(5)': '9(5)',
                    'SEC_9_5': '9(5)',
                    'SECTION 9(5)': '9(5)'
                }
                
                nos_output = nos_mapping.get(nos_str, nos_str)
                
                if nos_output not in ['TCS', '9(5)']:
                    self.validation_tracker.add_error(
                        row_num, 'Nature of Supply',
                        f'Invalid nature of supply: {nature_of_supply}',
                        nature_of_supply
                    )
                    is_valid = False
            
            # 2. Validate ECO GSTIN (MANDATORY)
            eco_gstin = self._get_value(row, 'eco_gstin')
            if not eco_gstin or pd.isna(eco_gstin):
                self.validation_tracker.add_error(
                    row_num, 'ECO GSTIN',
                    'ECO GSTIN cannot be empty',
                    eco_gstin
                )
                is_valid = False
            else:
                eco_gstin_str = str(eco_gstin).strip().upper()
                
                # Validate GSTIN length and format
                if len(eco_gstin_str) != 15:
                    self.validation_tracker.add_error(
                        row_num, 'ECO GSTIN',
                        f'ECO GSTIN must be 15 characters',
                        eco_gstin
                    )
                    is_valid = False
                elif not ValidationService.validate_gstin(eco_gstin_str):
                    self.validation_tracker.add_error(
                        row_num, 'ECO GSTIN',
                        'Invalid ECO GSTIN format',
                        eco_gstin
                    )
                    is_valid = False
            
            # 3. Validate ECO Name (OPTIONAL)
            eco_name = self._get_value(row, 'eco_name') or ''
            if eco_name and not pd.isna(eco_name):
                eco_name_str = str(eco_name).strip()
                if len(eco_name_str) > 255:
                    self.validation_tracker.add_warning(
                        row_num, 'ECO Name',
                        'ECO name exceeds 255 characters, truncating',
                        'Truncated', eco_name, eco_name_str[:255]
                    )
                    eco_name_str = eco_name_str[:255]
            else:
                eco_name_str = ''
            
            # 4. Validate Net Value of Supplies (MANDATORY)
            net_value = self._get_value(row, 'net_value_of_supplies')
            if net_value is None or pd.isna(net_value):
                # Try to get from invoice_value or taxable_value
                net_value = row.get('_invoice_value') or row.get('_taxable_value')
            
            if net_value is None or pd.isna(net_value):
                self.validation_tracker.add_error(
                    row_num, 'Net Value',
                    'Net value of supplies cannot be empty',
                    net_value
                )
                is_valid = False
            else:
                try:
                    net_value_num = float(net_value)
                    if net_value_num <= 0:
                        self.validation_tracker.add_error(
                            row_num, 'Net Value',
                            'Net value must be positive and non-zero',
                            net_value
                        )
                        is_valid = False
                except (ValueError, TypeError):
                    self.validation_tracker.add_error(
                        row_num, 'Net Value',
                        'Net value is not numeric',
                        net_value
                    )
                    is_valid = False
            
            # 5. Get Tax Amounts (OPTIONAL, typically IGST for interstate)
            igst = row.get('_igst_amount', 0)
            cgst = row.get('_cgst_amount', 0)
            sgst = row.get('_sgst_amount', 0)
            cess = row.get('_cess_amount', 0)
            
            igst = float(igst) if igst and not pd.isna(igst) else 0
            cgst = float(cgst) if cgst and not pd.isna(cgst) else 0
            sgst = float(sgst) if sgst and not pd.isna(sgst) else 0
            cess = float(cess) if cess and not pd.isna(cess) else 0
            
            if not is_valid:
                self.validation_tracker.skipped_count += 1
                continue
            
            self.validation_tracker.valid_count += 1
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'eco', 'nature_of_supply', nos_output)
            self._set_field(payload, 'eco', 'eco_gstin', eco_gstin_str)
            self._set_field(payload, 'eco', 'eco_name', eco_name_str)
            self._set_field(payload, 'eco', 'net_value_of_supplies', round(net_value_num, 2))
            self._set_field(payload, 'eco', 'igst_amount', round(igst, 2))
            self._set_field(payload, 'eco', 'cgst_amount', round(cgst, 2))
            self._set_field(payload, 'eco', 'sgst_amount', round(sgst, 2))
            self._set_field(payload, 'eco', 'cess_amount', round(cess, 2))
            
            if payload:
                rows.append(payload)
        
        logger.info(f"ECO: Processed {len(rows)} e-commerce operator supply entries")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoa(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECOA sheet - Amended Supplies Through E-Commerce Operator.
        Amendments to ECO sheet entries.
        """
        sheet_name = self.sheet_mapping.get('ecoa')
        if not sheet_name:
            logger.info("ECOA sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # ECOA: ECO supply with amendment flag
        mask = df.apply(lambda row: self._detect_eco_supply(row) and row.get('_is_amendment', False), axis=1)
        
        if not mask.any():
            logger.info("ECOA: No e-commerce operator amendment data available")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"ECOA: Processing {mask.sum()} e-commerce operator amendment entries")
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            is_valid = True
            
            # 1. Validate Financial Year (MANDATORY)
            financial_year = self._get_value(row, 'financial_year')
            if not financial_year or pd.isna(financial_year):
                self.validation_tracker.add_error(
                    row_num, 'Financial Year',
                    'Financial year required for ECO amendment',
                    financial_year
                )
                is_valid = False
            else:
                fy_str = str(financial_year).strip()
            
            # 2. Validate Original Month (MANDATORY)
            original_month = self._get_value(row, 'original_month')
            if not original_month or pd.isna(original_month):
                self.validation_tracker.add_error(
                    row_num, 'Original Month',
                    'Original month required',
                    original_month
                )
                is_valid = False
            else:
                try:
                    month_num = int(original_month)
                    if month_num < 1 or month_num > 12:
                        self.validation_tracker.add_error(
                            row_num, 'Original Month',
                            f'Invalid month: {month_num}',
                            original_month
                        )
                        is_valid = False
                except (ValueError, TypeError):
                    self.validation_tracker.add_error(
                        row_num, 'Original Month',
                        f'Invalid month format: {original_month}',
                        original_month
                    )
                    is_valid = False
            
            # 3. Validate Nature of Supply (MANDATORY, same as ECO)
            nature_of_supply = self._get_value(row, 'nature_of_supply')
            if not nature_of_supply or pd.isna(nature_of_supply):
                self.validation_tracker.add_error(
                    row_num, 'Nature of Supply',
                    'Nature of supply cannot be empty',
                    nature_of_supply
                )
                is_valid = False
            else:
                nos_str = str(nature_of_supply).strip().upper()
                nos_mapping = {
                    'TCS': 'TCS',
                    'SEC_52': 'TCS',
                    'SECTION 52': 'TCS',
                    '9(5)': '9(5)',
                    'SEC_9_5': '9(5)',
                    'SECTION 9(5)': '9(5)'
                }
                nos_output = nos_mapping.get(nos_str, nos_str)
                
                if nos_output not in ['TCS', '9(5)']:
                    self.validation_tracker.add_error(
                        row_num, 'Nature of Supply',
                        f'Invalid nature of supply: {nature_of_supply}',
                        nature_of_supply
                    )
                    is_valid = False
            
            # 4. Validate ECO GSTIN (MANDATORY)
            eco_gstin = self._get_value(row, 'eco_gstin')
            if not eco_gstin or pd.isna(eco_gstin):
                self.validation_tracker.add_error(
                    row_num, 'ECO GSTIN',
                    'ECO GSTIN cannot be empty',
                    eco_gstin
                )
                is_valid = False
            else:
                eco_gstin_str = str(eco_gstin).strip().upper()
                
                if len(eco_gstin_str) != 15:
                    self.validation_tracker.add_error(
                        row_num, 'ECO GSTIN',
                        f'ECO GSTIN must be 15 characters',
                        eco_gstin
                    )
                    is_valid = False
                elif not ValidationService.validate_gstin(eco_gstin_str):
                    self.validation_tracker.add_error(
                        row_num, 'ECO GSTIN',
                        'Invalid ECO GSTIN format',
                        eco_gstin
                    )
                    is_valid = False
            
            # 5. Validate ECO Name (OPTIONAL)
            eco_name = self._get_value(row, 'eco_name') or ''
            if eco_name and not pd.isna(eco_name):
                eco_name_str = str(eco_name).strip()[:255]
            else:
                eco_name_str = ''
            
            # 6. Validate Net Value (Revised) (MANDATORY)
            net_value = self._get_value(row, 'net_value_of_supplies')
            if net_value is None or pd.isna(net_value):
                net_value = row.get('_invoice_value') or row.get('_taxable_value')
            
            if net_value is None or pd.isna(net_value):
                self.validation_tracker.add_error(
                    row_num, 'Net Value',
                    'Net value of supplies cannot be empty',
                    net_value
                )
                is_valid = False
            else:
                try:
                    net_value_num = float(net_value)
                    if net_value_num <= 0:
                        self.validation_tracker.add_error(
                            row_num, 'Net Value',
                            'Net value must be positive',
                            net_value
                        )
                        is_valid = False
                except (ValueError, TypeError):
                    self.validation_tracker.add_error(
                        row_num, 'Net Value',
                        'Net value is not numeric',
                        net_value
                    )
                    is_valid = False
            
            # 7. Get Tax Amounts (Revised)
            igst = row.get('_igst_amount', 0)
            cgst = row.get('_cgst_amount', 0)
            sgst = row.get('_sgst_amount', 0)
            cess = row.get('_cess_amount', 0)
            
            igst = float(igst) if igst and not pd.isna(igst) else 0
            cgst = float(cgst) if cgst and not pd.isna(cgst) else 0
            sgst = float(sgst) if sgst and not pd.isna(sgst) else 0
            cess = float(cess) if cess and not pd.isna(cess) else 0
            
            if not is_valid:
                self.validation_tracker.skipped_count += 1
                continue
            
            self.validation_tracker.valid_count += 1
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecoa', 'nature_of_supply', nos_output)
            self._set_field(payload, 'ecoa', 'financial_year', fy_str)
            self._set_field(payload, 'ecoa', 'original_month', str(month_num))
            self._set_field(payload, 'ecoa', 'eco_gstin', eco_gstin_str)
            self._set_field(payload, 'ecoa', 'eco_name', eco_name_str)
            self._set_field(payload, 'ecoa', 'net_value_of_supplies', round(net_value_num, 2))
            self._set_field(payload, 'ecoa', 'igst_amount', round(igst, 2))
            self._set_field(payload, 'ecoa', 'cgst_amount', round(cgst, 2))
            self._set_field(payload, 'ecoa', 'sgst_amount', round(sgst, 2))
            self._set_field(payload, 'ecoa', 'cess_amount', round(cess, 2))
            
            if payload:
                rows.append(payload)
        
        logger.info(f"ECOA: Processed {len(rows)} e-commerce operator amendment entries")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecob2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECOB2B sheet - Supplies U/s 9(5)-15-B2B.
        Section 9(5) B2B supplies through e-commerce operator where supplier pays tax.
        """
        sheet_name = self.sheet_mapping.get('ecob2b')
        if not sheet_name:
            logger.info("ECOB2B sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # ECOB2B: Section 9(5) supplies with both supplier and recipient GSTIN
        mask = df.apply(lambda row: self._detect_ecob2b_supply(row), axis=1)
        
        if not mask.any():
            logger.info("ECOB2B: No Section 9(5) B2B supply data available")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"ECOB2B: Processing {mask.sum()} Section 9(5) B2B supply entries")
        
        # Track uniqueness: Supplier_GSTIN|Recipient_GSTIN|Doc_Number|Doc_Date
        ecob2b_keys = set()
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            is_valid = True
            
            # 1. Validate Supplier GSTIN (MANDATORY)
            supplier_gstin = self._get_value(row, 'supplier_gstin')
            if not supplier_gstin or pd.isna(supplier_gstin):
                self.validation_tracker.add_error(
                    row_num, 'Supplier GSTIN',
                    'Supplier GSTIN cannot be empty',
                    supplier_gstin
                )
                is_valid = False
            else:
                supplier_gstin_str = str(supplier_gstin).strip().upper()
                
                if len(supplier_gstin_str) != 15:
                    self.validation_tracker.add_error(
                        row_num, 'Supplier GSTIN',
                        f'Supplier GSTIN must be 15 characters',
                        supplier_gstin
                    )
                    is_valid = False
                elif not ValidationService.validate_gstin(supplier_gstin_str):
                    self.validation_tracker.add_error(
                        row_num, 'Supplier GSTIN',
                        'Invalid Supplier GSTIN format',
                        supplier_gstin
                    )
                    is_valid = False
            
            # 2. Validate Supplier Name (OPTIONAL)
            supplier_name = self._get_value(row, 'supplier_name') or 'Registered Supplier'
            if supplier_name and not pd.isna(supplier_name):
                supplier_name_str = str(supplier_name).strip()[:255]
            else:
                supplier_name_str = 'Registered Supplier'
            
            # 3. Validate Recipient GSTIN (MANDATORY)
            recipient_gstin = self._get_value(row, 'recipient_gstin')
            if not recipient_gstin or pd.isna(recipient_gstin):
                # Try to get from _gstin (main GSTIN field)
                recipient_gstin = row.get('_gstin')
            
            if not recipient_gstin or pd.isna(recipient_gstin):
                self.validation_tracker.add_error(
                    row_num, 'Recipient GSTIN',
                    'Recipient GSTIN cannot be empty',
                    recipient_gstin
                )
                is_valid = False
            else:
                recipient_gstin_str = str(recipient_gstin).strip().upper()
                
                if len(recipient_gstin_str) != 15:
                    self.validation_tracker.add_error(
                        row_num, 'Recipient GSTIN',
                        f'Recipient GSTIN must be 15 characters',
                        recipient_gstin
                    )
                    is_valid = False
                elif not ValidationService.validate_gstin(recipient_gstin_str):
                    self.validation_tracker.add_error(
                        row_num, 'Recipient GSTIN',
                        'Invalid Recipient GSTIN format',
                        recipient_gstin
                    )
                    is_valid = False
                elif supplier_gstin_str and supplier_gstin_str == recipient_gstin_str:
                    self.validation_tracker.add_error(
                        row_num, 'Recipient GSTIN',
                        'Recipient GSTIN cannot be same as Supplier GSTIN',
                        recipient_gstin
                    )
                    is_valid = False
            
            # 4. Validate Recipient Name (OPTIONAL)
            recipient_name = self._get_value(row, 'recipient_name')
            if not recipient_name or pd.isna(recipient_name):
                recipient_name = row.get('_receiver_name', 'Registered Recipient')
            
            recipient_name_str = str(recipient_name).strip()[:255] if recipient_name else 'Registered Recipient'
            
            # 5. Validate Document Number (MANDATORY)
            document_number = self._get_value(row, 'document_number')
            if not document_number or pd.isna(document_number):
                document_number = row.get('_invoice_number')
            
            if not document_number or pd.isna(document_number):
                self.validation_tracker.add_error(
                    row_num, 'Document Number',
                    'Document number cannot be empty',
                    document_number
                )
                is_valid = False
            else:
                doc_num_str = str(document_number).strip().upper()
                
                if len(doc_num_str) > 16:
                    self.validation_tracker.add_error(
                        row_num, 'Document Number',
                        f'Document number exceeds 16 characters',
                        document_number
                    )
                    is_valid = False
            
            # 6. Validate Document Date (MANDATORY)
            document_date = self._get_value(row, 'document_date')
            if not document_date or pd.isna(document_date):
                document_date = row.get('_invoice_date')
            
            if not document_date or pd.isna(document_date):
                self.validation_tracker.add_error(
                    row_num, 'Document Date',
                    'Document date cannot be empty',
                    document_date
                )
                is_valid = False
            
            # 7. Validate Value of Supplies (OPTIONAL)
            value_of_supplies = self._get_value(row, 'value_of_supplies')
            if not value_of_supplies or pd.isna(value_of_supplies):
                value_of_supplies = row.get('_invoice_value', 0)
            
            try:
                value_of_supplies_num = float(value_of_supplies) if value_of_supplies else 0
                if value_of_supplies_num < 0:
                    self.validation_tracker.add_warning(
                        row_num, 'Value of Supplies',
                        'Value of supplies cannot be negative, setting to 0',
                        'Defaulting', value_of_supplies, 0
                    )
                    value_of_supplies_num = 0
            except (ValueError, TypeError):
                value_of_supplies_num = 0
            
            # 8. Validate Place of Supply (MANDATORY)
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(
                    row_num, 'Place of Supply',
                    'Place of supply cannot be empty',
                    pos_code
                )
                is_valid = False
            else:
                pos_display = self._format_place_of_supply(pos_code)
                
                # Determine tax structure
                recipient_state = recipient_gstin_str[:2] if recipient_gstin_str else '99'
                is_interstate = (recipient_state != pos_code)
            
            # 9. Validate Document Type (MANDATORY)
            document_type = self._get_value(row, 'document_type') or 'B2B'
            doc_type_str = str(document_type).strip().upper()
            
            valid_doc_types = ['B2B', 'DEEMED_EXPORT', 'SEZ', 'SEZ_WOP', 'SEZ_WP', 'NA']
            if doc_type_str not in valid_doc_types:
                self.validation_tracker.add_warning(
                    row_num, 'Document Type',
                    f'Invalid document type, defaulting to B2B',
                    'Defaulting', document_type, 'B2B'
                )
                doc_type_str = 'B2B'
            
            # 10. Validate Rate (MANDATORY)
            rate = row.get('_rate')
            if not self._validate_tax_rate(rate, row_num, None):
                is_valid = False
            else:
                rate = float(rate)
            
            # 11. Validate Taxable Value (MANDATORY)
            taxable_value = row.get('_taxable_value')
            if not self._validate_amount_not_zero_negative(taxable_value, row_num, 'Taxable Value'):
                is_valid = False
            else:
                taxable_value = float(taxable_value)
            
            # 12. Validate Cess (OPTIONAL)
            cess = row.get('_cess_amount', 0)
            cess = float(cess) if cess and not pd.isna(cess) else 0
            
            # Calculate tax amounts
            if is_interstate:
                igst = (taxable_value * rate) / 100
                cgst = 0
                sgst = 0
            else:
                igst = 0
                cgst = (taxable_value * rate / 2) / 100
                sgst = (taxable_value * rate / 2) / 100
            
            # Track uniqueness
            if supplier_gstin_str and recipient_gstin_str and doc_num_str and document_date:
                unique_key = f"{supplier_gstin_str}|{recipient_gstin_str}|{doc_num_str}|{document_date}"
                if unique_key in ecob2b_keys:
                    self.validation_tracker.add_error(
                        row_num, 'Document Number',
                        'Duplicate ECOB2B entry detected',
                        unique_key
                    )
                    is_valid = False
                else:
                    ecob2b_keys.add(unique_key)
            
            if not is_valid:
                self.validation_tracker.skipped_count += 1
                continue
            
            self.validation_tracker.valid_count += 1
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecob2b', 'supplier_gstin', supplier_gstin_str)
            self._set_field(payload, 'ecob2b', 'supplier_name', supplier_name_str)
            self._set_field(payload, 'ecob2b', 'recipient_gstin', recipient_gstin_str)
            self._set_field(payload, 'ecob2b', 'recipient_name', recipient_name_str)
            self._set_field(payload, 'ecob2b', 'document_number', doc_num_str)
            self._set_field(payload, 'ecob2b', 'document_date', document_date)
            self._set_field(payload, 'ecob2b', 'value_of_supplies', round(value_of_supplies_num, 2))
            self._set_field(payload, 'ecob2b', 'place_of_supply', pos_display)
            self._set_field(payload, 'ecob2b', 'document_type', doc_type_str)
            self._set_field(payload, 'ecob2b', 'rate', round(rate, 2))
            self._set_field(payload, 'ecob2b', 'taxable_value', round(taxable_value, 2))
            self._set_field(payload, 'ecob2b', 'igst_amount', round(igst, 2))
            self._set_field(payload, 'ecob2b', 'cgst_amount', round(cgst, 2))
            self._set_field(payload, 'ecob2b', 'sgst_amount', round(sgst, 2))
            self._set_field(payload, 'ecob2b', 'cess_amount', round(cess, 2))
            
            if payload:
                rows.append(payload)
        
        logger.info(f"ECOB2B: Processed {len(rows)} Section 9(5) B2B supply entries")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecourp2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECOURP2B sheet - Supplies U/s 9(5)-15-URP2B.
        Unregistered Person to Registered Person B2B through e-commerce operator.
        Supplier is unregistered, recipient is registered.
        """
        sheet_name = self.sheet_mapping.get('ecourp2b')
        if not sheet_name:
            logger.info("ECOURP2B sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # ECOURP2B: Section 9(5) with recipient GSTIN but NO supplier GSTIN (unregistered supplier)
        mask = df.apply(lambda row: self._detect_ecourp2b_supply(row), axis=1)
        
        if not mask.any():
            logger.info("ECOURP2B: No unregistered to registered B2B supply data available")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"ECOURP2B: Processing {mask.sum()} unregistered to registered B2B supply entries")
        
        # Track uniqueness: Recipient_GSTIN|Doc_Number|Doc_Date
        ecourp2b_keys = set()
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            is_valid = True
            
            # 1. Validate Recipient GSTIN (MANDATORY - PRIMARY KEY)
            recipient_gstin = self._get_value(row, 'recipient_gstin')
            if not recipient_gstin or pd.isna(recipient_gstin):
                recipient_gstin = row.get('_gstin')
            
            if not recipient_gstin or pd.isna(recipient_gstin):
                self.validation_tracker.add_error(
                    row_num, 'Recipient GSTIN',
                    'Recipient GSTIN cannot be empty',
                    recipient_gstin
                )
                is_valid = False
            else:
                recipient_gstin_str = str(recipient_gstin).strip().upper()
                
                if len(recipient_gstin_str) != 15:
                    self.validation_tracker.add_error(
                        row_num, 'Recipient GSTIN',
                        f'Recipient GSTIN must be 15 characters',
                        recipient_gstin
                    )
                    is_valid = False
                elif not ValidationService.validate_gstin(recipient_gstin_str):
                    self.validation_tracker.add_error(
                        row_num, 'Recipient GSTIN',
                        'Invalid Recipient GSTIN format',
                        recipient_gstin
                    )
                    is_valid = False
            
            # 2. Validate Recipient Name (OPTIONAL)
            recipient_name = self._get_value(row, 'recipient_name')
            if not recipient_name or pd.isna(recipient_name):
                recipient_name = row.get('_receiver_name', 'Registered Recipient')
            
            recipient_name_str = str(recipient_name).strip()[:255] if recipient_name else 'Registered Recipient'
            
            # 3. Validate Document Number (MANDATORY)
            document_number = self._get_value(row, 'document_number')
            if not document_number or pd.isna(document_number):
                document_number = row.get('_invoice_number')
            
            if not document_number or pd.isna(document_number):
                self.validation_tracker.add_error(
                    row_num, 'Document Number',
                    'Document number cannot be empty',
                    document_number
                )
                is_valid = False
            else:
                doc_num_str = str(document_number).strip().upper()
                
                if len(doc_num_str) > 16:
                    self.validation_tracker.add_error(
                        row_num, 'Document Number',
                        f'Document number exceeds 16 characters',
                        document_number
                    )
                    is_valid = False
            
            # 4. Validate Document Date (MANDATORY)
            document_date = self._get_value(row, 'document_date')
            if not document_date or pd.isna(document_date):
                document_date = row.get('_invoice_date')
            
            if not document_date or pd.isna(document_date):
                self.validation_tracker.add_error(
                    row_num, 'Document Date',
                    'Document date cannot be empty',
                    document_date
                )
                is_valid = False
            
            # 5. Validate Value of Supplies (OPTIONAL)
            value_of_supplies = self._get_value(row, 'value_of_supplies')
            if not value_of_supplies or pd.isna(value_of_supplies):
                value_of_supplies = row.get('_invoice_value', 0)
            
            try:
                value_of_supplies_num = float(value_of_supplies) if value_of_supplies else 0
                if value_of_supplies_num < 0:
                    value_of_supplies_num = 0
            except (ValueError, TypeError):
                value_of_supplies_num = 0
            
            # 6. Validate Place of Supply (MANDATORY)
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_error(
                    row_num, 'Place of Supply',
                    'Place of supply cannot be empty',
                    pos_code
                )
                is_valid = False
            else:
                pos_display = self._format_place_of_supply(pos_code)
                recipient_state = recipient_gstin_str[:2] if recipient_gstin_str else '99'
                is_interstate = (recipient_state != pos_code)
            
            # 7. Validate Document Type (MANDATORY)
            document_type = self._get_value(row, 'document_type') or 'B2B'
            doc_type_str = str(document_type).strip().upper()
            
            valid_doc_types = ['B2B', 'DEEMED_EXPORT', 'SEZ', 'SEZ_WOP', 'SEZ_WP', 'NA']
            if doc_type_str not in valid_doc_types:
                doc_type_str = 'B2B'
            
            # 8. Validate Rate (MANDATORY)
            rate = row.get('_rate')
            if not self._validate_tax_rate(rate, row_num, None):
                is_valid = False
            else:
                rate = float(rate)
            
            # 9. Validate Taxable Value (MANDATORY)
            taxable_value = row.get('_taxable_value')
            if not self._validate_amount_not_zero_negative(taxable_value, row_num, 'Taxable Value'):
                is_valid = False
            else:
                taxable_value = float(taxable_value)
            
            # 10. Validate Cess (OPTIONAL)
            cess = row.get('_cess_amount', 0)
            cess = float(cess) if cess and not pd.isna(cess) else 0
            
            # Calculate tax amounts
            if is_interstate:
                igst = (taxable_value * rate) / 100
                cgst = 0
                sgst = 0
            else:
                igst = 0
                cgst = (taxable_value * rate / 2) / 100
                sgst = (taxable_value * rate / 2) / 100
            
            # Track uniqueness (no supplier GSTIN, just recipient)
            if recipient_gstin_str and doc_num_str and document_date:
                unique_key = f"{recipient_gstin_str}|{doc_num_str}|{document_date}"
                if unique_key in ecourp2b_keys:
                    self.validation_tracker.add_error(
                        row_num, 'Document Number',
                        'Duplicate ECOURP2B entry detected',
                        unique_key
                    )
                    is_valid = False
                else:
                    ecourp2b_keys.add(unique_key)
            
            if not is_valid:
                self.validation_tracker.skipped_count += 1
                continue
            
            self.validation_tracker.valid_count += 1
            
            # Build payload
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecourp2b', 'recipient_gstin', recipient_gstin_str)
            self._set_field(payload, 'ecourp2b', 'recipient_name', recipient_name_str)
            self._set_field(payload, 'ecourp2b', 'document_number', doc_num_str)
            self._set_field(payload, 'ecourp2b', 'document_date', document_date)
            self._set_field(payload, 'ecourp2b', 'value_of_supplies', round(value_of_supplies_num, 2))
            self._set_field(payload, 'ecourp2b', 'place_of_supply', pos_display)
            self._set_field(payload, 'ecourp2b', 'document_type', doc_type_str)
            self._set_field(payload, 'ecourp2b', 'rate', round(rate, 2))
            self._set_field(payload, 'ecourp2b', 'taxable_value', round(taxable_value, 2))
            self._set_field(payload, 'ecourp2b', 'igst_amount', round(igst, 2))
            self._set_field(payload, 'ecourp2b', 'cgst_amount', round(cgst, 2))
            self._set_field(payload, 'ecourp2b', 'sgst_amount', round(sgst, 2))
            self._set_field(payload, 'ecourp2b', 'cess_amount', round(cess, 2))
            
            if payload:
                rows.append(payload)
        
        logger.info(f"ECOURP2B: Processed {len(rows)} unregistered to registered B2B supply entries")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecob2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECOB2C sheet - Supplies U/s 9(5)-15-B2C.
        B2C supplies through e-commerce operator where supplier pays tax.
        Aggregated by Supplier GSTIN + POS + Rate.
        """
        sheet_name = self.sheet_mapping.get('ecob2c')
        if not sheet_name:
            logger.info("ECOB2C sheet not found in template mapping")
            return None, pd.DataFrame()
        
        # ECOB2C: Section 9(5) B2C supplies (supplier registered, no recipient GSTIN)
        mask = df.apply(lambda row: self._detect_ecob2c_supply(row), axis=1)
        
        if not mask.any():
            logger.info("ECOB2C: No Section 9(5) B2C supply data available")
            return sheet_name, pd.DataFrame()
        
        logger.info(f"ECOB2C: Aggregating {mask.sum()} Section 9(5) B2C supply entries")
        
        # Aggregation dictionary: Supplier_GSTIN|POS|Rate
        aggregation_dict: Dict[str, Dict[str, object]] = {}
        
        for idx, row in df[mask].iterrows():
            self.validation_tracker.processed_count += 1
            original_idx = row.name if hasattr(row, 'name') else idx
            row_num = original_idx + 2
            
            # 1. Validate Supplier GSTIN (MANDATORY)
            supplier_gstin = self._get_value(row, 'supplier_gstin')
            if not supplier_gstin or pd.isna(supplier_gstin):
                self.validation_tracker.add_warning(
                    row_num, 'Supplier GSTIN',
                    'Supplier GSTIN missing for ECOB2C, skipping',
                    'Skipped', '', ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            supplier_gstin_str = str(supplier_gstin).strip().upper()
            
            if len(supplier_gstin_str) != 15 or not ValidationService.validate_gstin(supplier_gstin_str):
                self.validation_tracker.add_warning(
                    row_num, 'Supplier GSTIN',
                    'Invalid Supplier GSTIN, skipping',
                    'Skipped', supplier_gstin, ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            # 2. Get Supplier Name (OPTIONAL)
            supplier_name = self._get_value(row, 'supplier_name') or 'Registered Supplier'
            supplier_name_str = str(supplier_name).strip()[:255] if supplier_name else 'Registered Supplier'
            
            # 3. Validate Place of Supply (MANDATORY)
            pos_code = row.get('_pos_code')
            if not pos_code or pd.isna(pos_code):
                self.validation_tracker.add_warning(
                    row_num, 'Place of Supply',
                    'POS missing, skipping',
                    'Skipped', '', ''
                )
                self.validation_tracker.skipped_count += 1
                continue
            
            pos_display = self._format_place_of_supply(pos_code)
            supplier_state = supplier_gstin_str[:2]
            is_interstate = (supplier_state != pos_code)
            
            # 4. Validate Rate (MANDATORY)
            rate = row.get('_rate')
            if rate is None or pd.isna(rate):
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                rate = float(rate)
            except (ValueError, TypeError):
                self.validation_tracker.skipped_count += 1
                continue
            
            # 5. Validate Taxable Value (MANDATORY)
            taxable_value = row.get('_taxable_value')
            if taxable_value is None or pd.isna(taxable_value):
                self.validation_tracker.skipped_count += 1
                continue
            
            try:
                taxable_value = float(taxable_value)
                if taxable_value < 0:
                    self.validation_tracker.skipped_count += 1
                    continue
            except (ValueError, TypeError):
                self.validation_tracker.skipped_count += 1
                continue
            
            # 6. Get Cess (OPTIONAL)
            cess = row.get('_cess_amount', 0)
            cess = float(cess) if cess and not pd.isna(cess) else 0
            
            # Calculate tax amounts (B2C typically intrastate)
            if is_interstate:
                igst = (taxable_value * rate) / 100
                cgst = 0
                sgst = 0
            else:
                igst = 0
                cgst = (taxable_value * rate / 2) / 100
                sgst = (taxable_value * rate / 2) / 100
            
            # Aggregation key: Supplier_GSTIN|POS|Rate
            agg_key = f"{supplier_gstin_str}|{pos_code}|{rate}"
            
            # Aggregate
            if agg_key in aggregation_dict:
                aggregation_dict[agg_key]['taxable_value'] += taxable_value
                aggregation_dict[agg_key]['igst'] += igst
                aggregation_dict[agg_key]['cgst'] += cgst
                aggregation_dict[agg_key]['sgst'] += sgst
                aggregation_dict[agg_key]['cess'] += cess
            else:
                aggregation_dict[agg_key] = {
                    'supplier_gstin': supplier_gstin_str,
                    'supplier_name': supplier_name_str,
                    'pos': pos_display,
                    'rate': rate,
                    'taxable_value': taxable_value,
                    'igst': igst,
                    'cgst': cgst,
                    'sgst': sgst,
                    'cess': cess
                }
            
            self.validation_tracker.valid_count += 1
        
        # Build output rows
        rows: List[Dict[str, object]] = []
        for agg_key, agg_data in aggregation_dict.items():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecob2c', 'supplier_gstin', agg_data['supplier_gstin'])
            self._set_field(payload, 'ecob2c', 'supplier_name', agg_data['supplier_name'])
            self._set_field(payload, 'ecob2c', 'place_of_supply', agg_data['pos'])
            self._set_field(payload, 'ecob2c', 'rate', round(agg_data['rate'], 2))
            self._set_field(payload, 'ecob2c', 'taxable_value', round(agg_data['taxable_value'], 2))
            self._set_field(payload, 'ecob2c', 'igst_amount', round(agg_data['igst'], 2))
            self._set_field(payload, 'ecob2c', 'cgst_amount', round(agg_data['cgst'], 2))
            self._set_field(payload, 'ecob2c', 'sgst_amount', round(agg_data['sgst'], 2))
            self._set_field(payload, 'ecob2c', 'cess_amount', round(agg_data['cess'], 2))
            
            if payload:
                rows.append(payload)
        
        logger.info(f"ECOB2C: Aggregated {len(aggregation_dict)} unique Supplier+POS+Rate combinations")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecourp2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECOURP2C sheet - Supplies U/s 9(5)-15-URP2C.
        Unregistered to Unregistered B2C (minimal details, aggregated by POS+Rate).
        """
        sheet_name = self.sheet_mapping.get('ecourp2c')
        if not sheet_name:
            return None, pd.DataFrame()
        
        # ECOURP2C: No supplier GSTIN, no recipient GSTIN (both unregistered)
        mask = df.apply(lambda row: (
            not self._get_value(row, 'supplier_gstin') and
            not self._get_value(row, 'recipient_gstin') and
            not row.get('_has_valid_gstin', False) and
            self._get_value(row, 'nature_of_supply') and
            '9(5)' in str(self._get_value(row, 'nature_of_supply')).upper()
        ), axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        # Aggregation by POS+Rate
        aggregation_dict: Dict[str, Dict[str, object]] = {}
        
        for idx, row in df[mask].iterrows():
            pos_code = row.get('_pos_code')
            rate = row.get('_rate')
            taxable_value = row.get('_taxable_value')
            cess = row.get('_cess_amount', 0)
            
            if not all([pos_code, rate is not None, taxable_value]):
                continue
            
            agg_key = f"{pos_code}|{rate}"
            pos_display = self._format_place_of_supply(pos_code)
            
            if agg_key in aggregation_dict:
                aggregation_dict[agg_key]['taxable_value'] += float(taxable_value)
                aggregation_dict[agg_key]['cess'] += float(cess) if cess else 0
            else:
                aggregation_dict[agg_key] = {
                    'pos': pos_display,
                    'rate': float(rate),
                    'taxable_value': float(taxable_value),
                    'cess': float(cess) if cess else 0
                }
        
        rows: List[Dict[str, object]] = []
        for agg_data in aggregation_dict.values():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecourp2c', 'place_of_supply', agg_data['pos'])
            self._set_field(payload, 'ecourp2c', 'rate', round(agg_data['rate'], 2))
            self._set_field(payload, 'ecourp2c', 'taxable_value', round(agg_data['taxable_value'], 2))
            self._set_field(payload, 'ecourp2c', 'cess_amount', round(agg_data['cess'], 2))
            if payload:
                rows.append(payload)
        
        logger.info(f"ECOURP2C: Aggregated {len(rows)} POS+Rate combinations")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoab2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECOAB2B sheet - Amended Supplies U/s 9(5)-15A-B2B.
        Amendments to ECOB2B entries.
        """
        sheet_name = self.sheet_mapping.get('ecoab2b')
        if not sheet_name:
            return None, pd.DataFrame()
        
        # ECOAB2B: ECOB2B with amendment flag
        mask = df.apply(lambda row: (
            self._detect_ecob2b_supply(row) and
            row.get('_is_amendment', False)
        ), axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            # Use ECOB2B logic but with original/revised fields
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecoab2b', 'supplier_gstin', self._get_value(row, 'supplier_gstin') or '')
            self._set_field(payload, 'ecoab2b', 'recipient_gstin', self._get_value(row, 'recipient_gstin') or row.get('_gstin', ''))
            self._set_field(payload, 'ecoab2b', 'original_document_number', self._get_value(row, 'original_invoice_number') or self._get_value(row, 'original_document_number') or '')
            self._set_field(payload, 'ecoab2b', 'original_document_date', self._get_value(row, 'original_invoice_date') or self._get_value(row, 'original_document_date') or '')
            self._set_field(payload, 'ecoab2b', 'revised_document_number', row.get('_invoice_number', ''))
            self._set_field(payload, 'ecoab2b', 'revised_document_date', row.get('_invoice_date', ''))
            self._set_field(payload, 'ecoab2b', 'taxable_value', round(float(row.get('_taxable_value', 0)), 2))
            if payload:
                rows.append(payload)
        
        logger.info(f"ECOAB2B: Processed {len(rows)} amended B2B entries")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoaurp2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECOAURP2B sheet - Amended Supplies U/s 9(5)-15A-URP2B.
        Amendments to ECOURP2B entries.
        """
        sheet_name = self.sheet_mapping.get('ecoaurp2b')
        if not sheet_name:
            return None, pd.DataFrame()
        
        mask = df.apply(lambda row: (
            self._detect_ecourp2b_supply(row) and
            row.get('_is_amendment', False)
        ), axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecoaurp2b', 'recipient_gstin', self._get_value(row, 'recipient_gstin') or row.get('_gstin', ''))
            self._set_field(payload, 'ecoaurp2b', 'original_document_number', self._get_value(row, 'original_invoice_number') or self._get_value(row, 'original_document_number') or '')
            self._set_field(payload, 'ecoaurp2b', 'original_document_date', self._get_value(row, 'original_invoice_date') or self._get_value(row, 'original_document_date') or '')
            self._set_field(payload, 'ecoaurp2b', 'revised_document_number', row.get('_invoice_number', ''))
            self._set_field(payload, 'ecoaurp2b', 'revised_document_date', row.get('_invoice_date', ''))
            self._set_field(payload, 'ecoaurp2b', 'taxable_value', round(float(row.get('_taxable_value', 0)), 2))
            if payload:
                rows.append(payload)
        
        logger.info(f"ECOAURP2B: Processed {len(rows)} amended URP2B entries")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoab2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECOAB2C sheet - Amended Supplies U/s 9(5)-15A-B2C.
        Amendments to ECOB2C entries.
        """
        sheet_name = self.sheet_mapping.get('ecoab2c')
        if not sheet_name:
            return None, pd.DataFrame()
        
        mask = df.apply(lambda row: (
            self._detect_ecob2c_supply(row) and
            row.get('_is_amendment', False)
        ), axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecoab2c', 'financial_year', self._get_value(row, 'financial_year') or '')
            self._set_field(payload, 'ecoab2c', 'original_month', self._get_value(row, 'original_month') or '')
            self._set_field(payload, 'ecoab2c', 'supplier_gstin', self._get_value(row, 'supplier_gstin') or '')
            self._set_field(payload, 'ecoab2c', 'place_of_supply', self._format_place_of_supply(row.get('_pos_code', '99')))
            self._set_field(payload, 'ecoab2c', 'rate', round(float(row.get('_rate', 0)), 2))
            self._set_field(payload, 'ecoab2c', 'taxable_value', round(float(row.get('_taxable_value', 0)), 2))
            if payload:
                rows.append(payload)
        
        logger.info(f"ECOAB2C: Processed {len(rows)} amended B2C entries")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoaurp2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        """
        Build ECOAURP2C sheet - Amended Supplies U/s 9(5)-15A-URP2C.
        Amendments to ECOURP2C entries.
        """
        sheet_name = self.sheet_mapping.get('ecoaurp2c')
        if not sheet_name:
            return None, pd.DataFrame()
        
        mask = df.apply(lambda row: (
            not self._get_value(row, 'supplier_gstin') and
            not self._get_value(row, 'recipient_gstin') and
            row.get('_is_amendment', False) and
            self._get_value(row, 'nature_of_supply') and
            '9(5)' in str(self._get_value(row, 'nature_of_supply')).upper()
        ), axis=1)
        
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for idx, row in df[mask].iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecoaurp2c', 'financial_year', self._get_value(row, 'financial_year') or '')
            self._set_field(payload, 'ecoaurp2c', 'original_month', self._get_value(row, 'original_month') or '')
            self._set_field(payload, 'ecoaurp2c', 'place_of_supply', self._format_place_of_supply(row.get('_pos_code', '99')))
            self._set_field(payload, 'ecoaurp2c', 'rate', round(float(row.get('_rate', 0)), 2))
            self._set_field(payload, 'ecoaurp2c', 'taxable_value', round(float(row.get('_taxable_value', 0)), 2))
            if payload:
                rows.append(payload)
        
        logger.info(f"ECOAURP2C: Processed {len(rows)} amended URP2C entries")
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    def _set_field(self, payload: Dict[str, object], sheet_key: str, field_key: str, value):
        header = self.template_field_headers.get(sheet_key, {}).get(field_key)
        if not header:
            return
        if value is None:
            return
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return
        payload[header] = value
    
    def _build_sheet_dataframe(self, rows: List[Dict[str, object]], sheet_name: str) -> pd.DataFrame:
        headers = self.template_structure.get(sheet_name, {}).get('headers', [])
        df = pd.DataFrame(rows)
        if headers:
            for header in headers:
                if header not in df.columns:
                    df[header] = None
            df = df[headers]
        return df
    
    def _match_column(self, columns: List[str], keywords: List[str]) -> Optional[str]:
        normalized_columns = [(col, normalize_label(col)) for col in columns]
        best_match: Optional[str] = None
        best_score: Optional[Tuple[int, int, int]] = None
        for priority, keyword in enumerate(keywords):
            normalized_keyword = normalize_label(keyword)
            if not normalized_keyword:
                continue
            for idx, (original, label) in enumerate(normalized_columns):
                match_level: Optional[int] = None
                if label == normalized_keyword:
                    match_level = 0
                elif label.startswith(normalized_keyword):
                    match_level = 1
                elif normalized_keyword in label:
                    match_level = 2
                if match_level is None:
                    continue
                score = (match_level, priority, idx)
                if best_score is None or score < best_score:
                    best_score = score
                    best_match = original
        return best_match
    
    def _header_matches(self, normalized_header: str, field_key: str, keywords: List[str]) -> bool:
        for keyword in keywords:
            normalized_keyword = normalize_label(keyword)
            if not normalized_keyword:
                continue
            if normalized_keyword in normalized_header:
                if field_key == 'type' and 'note' in normalized_header:
                    continue
                if field_key == 'note_type' and 'note' not in normalized_header:
                    continue
                if field_key == 'note_value' and 'note' not in normalized_header:
                    continue
                return True
        return False
    
    def _get_value(self, row: pd.Series, field_key: str):
        column = self.column_map.get(field_key)
        if column and column in row:
            return row[column]
        return None
    
    @staticmethod
    def _safe_string(value) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return ''
        string_value = str(value).strip()
        if string_value.lower() in ('nan', 'none'):
            return ''
        return string_value
    
    @staticmethod
    def _truncate(value: str, max_length: int) -> str:
        if not value:
            return ''
        if len(value) <= max_length:
            return value
        return value[:max_length]
    
    @staticmethod
    def _clean_gstin_value(value) -> str:
        clean_value = SheetMapper._safe_string(value).upper()
        if len(clean_value) != 15:
            return ''
        return clean_value
    
    def _is_valid_gstin(self, gstin: str) -> bool:
        if not gstin:
            return False
        is_valid, _ = self.validation_service.validate_gstin(gstin)
        return is_valid
    
    def _parse_date(self, value) -> Optional[date]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        parsed = pd.to_datetime(value, errors='coerce')
        if pd.isna(parsed):
            return None
        return parsed.date()
    
    def _resolve_invoice_value(self, row: pd.Series) -> Optional[float]:
        invoice_value = self._to_float(self._get_value(row, 'invoice_value'))
        if invoice_value is not None:
            return invoice_value
        candidates = [
            self._get_value(row, 'gross_amount'),
            self._get_value(row, 'mrp_value'),
        ]
        for value in candidates:
            numeric = self._to_float(value)
            if numeric is not None:
                return numeric
        taxable = self._to_float(self._get_value(row, 'taxable_value'))
        tax_total = row.get('_tax_total')
        if tax_total is None:
            tax_total = self._extract_tax_total(row)
        if taxable is not None and tax_total is not None:
            return taxable + tax_total
        if taxable is not None:
            return taxable
        return None
    
    def _resolve_taxable_value(self, row: pd.Series, invoice_value: Optional[float]) -> Optional[float]:
        taxable = self._to_float(self._get_value(row, 'taxable_value'))
        if taxable is not None:
            return taxable
        if invoice_value is None:
            return None
        tax_total = row.get('_tax_total')
        if tax_total is None:
            tax_total = self._extract_tax_total(row)
        if tax_total is None:
            return invoice_value
        return invoice_value - tax_total
    
    def _resolve_rate(self, row: pd.Series) -> Optional[float]:
        igst_rate = self._to_float(self._get_value(row, 'igst_rate'))
        if igst_rate:
            return igst_rate
        cgst_rate = self._to_float(self._get_value(row, 'cgst_rate')) or 0
        sgst_rate = self._to_float(self._get_value(row, 'sgst_rate')) or 0
        if cgst_rate or sgst_rate:
            return cgst_rate + sgst_rate
        generic_rate = self._to_float(self._get_value(row, 'rate'))
        if generic_rate:
            return generic_rate
        taxable = self._to_float(self._get_value(row, 'taxable_value'))
        tax_total = row.get('_tax_total')
        if tax_total is None:
            tax_total = self._extract_tax_total(row)
        if taxable and tax_total:
            try:
                return round((tax_total / taxable) * 100, 2)
            except ZeroDivisionError:
                return None
        return None
    
    def _resolve_cess_amount(self, row: pd.Series) -> float:
        value = self._to_float(self._get_value(row, 'cess_amount'))
        if value is not None:
            return value
        return 0.0
    
    def _extract_tax_total(self, row: pd.Series) -> Optional[float]:
        explicit_total = self._to_float(self._get_value(row, 'tax_total'))
        if explicit_total is not None:
            return explicit_total
        return self._sum_tax_amounts(row)
    
    def _sum_tax_amounts(self, row: pd.Series) -> Optional[float]:
        amounts = [
            self._to_float(self._get_value(row, 'igst_amount')),
            self._to_float(self._get_value(row, 'cgst_amount')),
            self._to_float(self._get_value(row, 'sgst_amount')),
        ]
        valid = [amt for amt in amounts if amt is not None]
        if not valid:
            return None
        return sum(valid)
    
    def _resolve_note_value(self, row: pd.Series) -> Optional[float]:
        note_value = self._to_float(self._get_value(row, 'note_value'))
        if note_value is not None:
            return note_value
        taxable = row.get('_taxable_value')
        tax_total = row.get('_tax_total')
        taxable_abs = abs(taxable) if taxable is not None else 0
        tax_total_abs = abs(tax_total) if tax_total is not None else 0
        if taxable_abs or tax_total_abs:
            return taxable_abs + tax_total_abs
        if row['_invoice_value'] is not None:
            return abs(row['_invoice_value'])
        return None
    
    def _determine_note_type(self, doc_type: str, supply_text: str, note_value: Optional[float]) -> Optional[str]:
        doc_type_lower = f"{doc_type or ''} {supply_text or ''}".lower()
        if 'credit' in doc_type_lower or 'cn' in doc_type_lower:
            return 'C'
        if 'debit' in doc_type_lower or 'dn' in doc_type_lower:
            return 'D'
        return None
    
    def _is_credit_or_debit(self, doc_type: str, supply_text: str) -> bool:
        lowered = f"{doc_type or ''} {supply_text or ''}".lower()
        return any(keyword in lowered for keyword in ('credit', 'debit', 'cn', 'dn'))
    
    def _detect_export(self, row: pd.Series) -> bool:
        if row.get('_is_credit_or_debit'):
            return False
        candidates = [
            self._safe_string(self._get_value(row, 'sales_channel')),
            row['_doc_type'],
            self._safe_string(self._get_value(row, 'source_of_supply')),
            self._safe_string(self._get_value(row, 'unique_type')),
            row.get('_supply_text', ''),
        ]
        for value in candidates:
            lowered = (value or '').lower()
            if 'export' in lowered or lowered.startswith('exp '):
                return True
        return False
    
    def _detect_amendment(self, row: pd.Series) -> bool:
        """
        Detect if row is an amendment based on:
        - Amendment flag
        - Original invoice number/date present
        - Revised invoice number/date present
        """
        # Check explicit amendment flag
        amendment_flag = self._get_value(row, 'amendment_flag')
        if amendment_flag:
            flag_str = str(amendment_flag).strip().upper()
            if flag_str in ['Y', 'YES', '1', 'TRUE', 'AMENDED']:
                return True
        
        # Check if original and revised fields are present
        original_inv_no = self._get_value(row, 'original_invoice_number')
        revised_inv_no = self._get_value(row, 'revised_invoice_number')
        
        # If both original and revised invoice numbers exist, it's an amendment
        if original_inv_no and revised_inv_no:
            return True
        
        # Check doc_type or supply_type for amendment keywords
        doc_type = self._safe_string(self._get_value(row, 'doc_type'))
        supply_type = self._safe_string(self._get_value(row, 'supply_type'))
        
        combined = f"{doc_type} {supply_type}".lower()
        if 'amend' in combined or 'revision' in combined or 'revised' in combined:
            return True
        
        return False
    
    def _detect_advance(self, row: pd.Series) -> bool:
        """
        Detect if row is an advance payment (unadjusted).
        Advances are payments received before invoice issuance.
        """
        # Check doc_type or supply_type for advance keywords
        doc_type = self._safe_string(self._get_value(row, 'doc_type'))
        supply_type = self._safe_string(self._get_value(row, 'supply_type'))
        
        combined = f"{doc_type} {supply_type}".lower()
        if 'advance' in combined or 'unadjusted' in combined:
            return True
        
        # Check if it's a credit/debit note or export (exclude from advances)
        if row.get('_is_credit_or_debit') or row.get('_is_export'):
            return False
        
        # Check if it's an amendment (exclude from advances)
        if row.get('_is_amendment'):
            return False
        
        return False
    
    def _detect_advance_adjustment(self, row: pd.Series) -> bool:
        """
        Detect if row is an advance adjustment (when invoice is issued against advance).
        """
        # Check doc_type or supply_type for adjustment keywords
        doc_type = self._safe_string(self._get_value(row, 'doc_type'))
        supply_type = self._safe_string(self._get_value(row, 'supply_type'))
        
        combined = f"{doc_type} {supply_type}".lower()
        if 'adjustment' in combined or 'adjusted' in combined:
            # Further check it's advance-related
            if 'advance' in combined:
                return True
        
        # Check if it's a credit/debit note or export (exclude from adjustments)
        if row.get('_is_credit_or_debit') or row.get('_is_export'):
            return False
        
        return False
    
    def _detect_exemp_supply(self, row: pd.Series) -> bool:
        """
        Detect if row is a nil rated, exempted, or non-GST supply.
        These are typically rate=0 supplies with specific supply categories.
        """
        # Check rate is 0
        rate = row.get('_rate', None)
        if rate is None or pd.isna(rate):
            return False
        
        try:
            rate = float(rate)
            if rate != 0:
                return False
        except (ValueError, TypeError):
            return False
        
        # Check if already classified as B2B/B2CL/Export/CN/DN
        if (row.get('_has_valid_gstin') or 
            row.get('_is_large_b2cl') or 
            row.get('_is_export') or 
            row.get('_is_credit_or_debit') or
            row.get('_is_amendment')):
            return False
        
        # Check doc_type or supply_type for exemp keywords
        doc_type = self._safe_string(self._get_value(row, 'doc_type'))
        supply_type = self._safe_string(self._get_value(row, 'supply_type'))
        
        combined = f"{doc_type} {supply_type}".lower()
        exemp_keywords = ['nil rated', 'exempted', 'exempt', 'non-gst', 'non gst', 'nongst']
        
        for keyword in exemp_keywords:
            if keyword in combined:
                return True
        
        # If rate=0 and not classified elsewhere, could be exempted
        return False
    
    def _get_exemp_category(self, row: pd.Series) -> Optional[str]:
        """
        Determine the EXEMP supply category: Nil Rated, Exempted, or Non-GST.
        """
        doc_type = self._safe_string(self._get_value(row, 'doc_type'))
        supply_type = self._safe_string(self._get_value(row, 'supply_type'))
        
        combined = f"{doc_type} {supply_type}".lower()
        
        if 'nil rated' in combined or 'nil rate' in combined:
            return 'Nil Rated'
        elif 'exempt' in combined:
            return 'Exempted'
        elif 'non-gst' in combined or 'non gst' in combined or 'nongst' in combined:
            return 'Non-GST'
        
        # Default to Exempted if rate=0 and not otherwise specified
        return 'Exempted'
    
    def _validate_uqc(self, uqc: str, row_num: int) -> bool:
        """
        Validate UQC (Unit Quantity Code) against standard GST UQC codes.
        Returns True if valid, False otherwise (with error logged).
        """
        valid_uqc_codes = [
            'BAG', 'BAL', 'BDL', 'BKL', 'BOX', 'BOY', 'BUN', 'CAR',
            'CMS', 'CTN', 'DAY', 'DOZ', 'DRM', 'DZN', 'FTN', 'GMS',
            'GRS', 'GYD', 'HRS', 'KGM', 'KLR', 'KT', 'LTR', 'MMS',
            'MTK', 'MTR', 'MTS', 'NAR', 'NOS', 'PAC', 'PCS', 'PK',
            'PKT', 'PRS', 'QTL', 'ROL', 'SET', 'SQM', 'SQY', 'STN',
            'TON', 'TUB', 'UGS', 'UNT', 'YDS'
        ]
        
        uqc_upper = uqc.upper().strip()
        
        if uqc_upper not in valid_uqc_codes:
            self.validation_tracker.add_warning(
                row_num, 'UQC',
                f'Invalid UQC code: {uqc}',
                'Using NOS', uqc, 'NOS'
            )
            return False
        
        return True
    
    def _detect_docs_entry(self, row: pd.Series) -> bool:
        """
        Detect if row is a DOCS (document series) entry.
        DOCS entries have nature of document, series start/end, and total number.
        """
        # Check if row has DOCS-specific fields
        nature_of_doc = self._get_value(row, 'nature_of_document')
        sr_from = self._get_value(row, 'sr_no_from')
        sr_to = self._get_value(row, 'sr_no_to')
        total_number = self._get_value(row, 'total_number')
        
        # If any DOCS field is present, consider it a DOCS entry
        if nature_of_doc and not pd.isna(nature_of_doc):
            return True
        if sr_from and not pd.isna(sr_from):
            return True
        if sr_to and not pd.isna(sr_to):
            return True
        if total_number and not pd.isna(total_number):
            return True
        
        return False
    
    def _extract_numeric_portion(self, value: str) -> Optional[int]:
        """
        Extract numeric portion from a string (e.g., "INV-001" -> 1).
        Returns None if no numeric portion found.
        """
        import re
        
        # Try to extract all digits from the string
        digits = re.findall(r'\d+', value)
        
        if not digits:
            return None
        
        # Take the last numeric sequence (typically the series number)
        try:
            return int(digits[-1])
        except (ValueError, IndexError):
            return None
    
    def _detect_eco_supply(self, row: pd.Series) -> bool:
        """
        Detect if row is an e-commerce operator (ECO) supply.
        ECO supplies have nature of supply (TCS or 9(5)) and ECO GSTIN.
        """
        # Check if row has nature of supply field
        nature_of_supply = self._get_value(row, 'nature_of_supply')
        if nature_of_supply and not pd.isna(nature_of_supply):
            nos_str = str(nature_of_supply).strip().upper()
            if 'TCS' in nos_str or '9(5)' in nos_str or 'SEC' in nos_str:
                return True
        
        # Check if row has ECO GSTIN
        eco_gstin = self._get_value(row, 'eco_gstin')
        if eco_gstin and not pd.isna(eco_gstin):
            return True
        
        # Check doc_type or supply_type for ECO keywords
        doc_type = self._safe_string(self._get_value(row, 'doc_type'))
        supply_type = self._safe_string(self._get_value(row, 'supply_type'))
        
        combined = f"{doc_type} {supply_type}".lower()
        eco_keywords = ['ecommerce', 'e-commerce', 'eco', 'tcs', 'section 52', 'section 9(5)']
        
        for keyword in eco_keywords:
            if keyword in combined:
                return True
        
        return False
    
    def _detect_ecob2b_supply(self, row: pd.Series) -> bool:
        """
        Detect if row is a Section 9(5) B2B supply through e-commerce operator.
        ECOB2B supplies have both supplier and recipient GSTIN, and supplier pays tax.
        """
        # Check if row has supplier GSTIN (key indicator for ECOB2B)
        supplier_gstin = self._get_value(row, 'supplier_gstin')
        if supplier_gstin and not pd.isna(supplier_gstin):
            # Also need recipient GSTIN for B2B
            recipient_gstin = self._get_value(row, 'recipient_gstin')
            if not recipient_gstin or pd.isna(recipient_gstin):
                # Try main GSTIN field
                recipient_gstin = row.get('_gstin')
            
            if recipient_gstin and not pd.isna(recipient_gstin):
                return True
        
        # Check if nature of supply indicates 9(5) and has both GSTINs
        nature_of_supply = self._get_value(row, 'nature_of_supply')
        if nature_of_supply and not pd.isna(nature_of_supply):
            nos_str = str(nature_of_supply).strip().upper()
            if '9(5)' in nos_str or 'SEC_9_5' in nos_str or 'SECTION 9(5)' in nos_str:
                # Check for B2B scenario (both have GSTIN)
                if row.get('_has_valid_gstin', False):
                    return True
        
        # Check doc_type or supply_type for Section 9(5) keywords
        doc_type = self._safe_string(self._get_value(row, 'doc_type'))
        supply_type = self._safe_string(self._get_value(row, 'supply_type'))
        
        combined = f"{doc_type} {supply_type}".lower()
        if 'section 9(5)' in combined or 'sec 9(5)' in combined or '9(5)' in combined:
            if 'b2b' in combined or row.get('_has_valid_gstin', False):
                return True
        
        return False
    
    def _detect_ecourp2b_supply(self, row: pd.Series) -> bool:
        """
        Detect if row is an unregistered to registered B2B supply through e-commerce operator.
        ECOURP2B supplies have recipient GSTIN but NO supplier GSTIN (unregistered supplier).
        """
        # Key indicator: Has recipient GSTIN but NO supplier GSTIN
        supplier_gstin = self._get_value(row, 'supplier_gstin')
        has_supplier_gstin = supplier_gstin and not pd.isna(supplier_gstin)
        
        # Must have recipient GSTIN
        recipient_gstin = self._get_value(row, 'recipient_gstin')
        if not recipient_gstin or pd.isna(recipient_gstin):
            recipient_gstin = row.get('_gstin')
        
        has_recipient_gstin = recipient_gstin and not pd.isna(recipient_gstin)
        
        # ECOURP2B: Has recipient but NO supplier (unregistered supplier scenario)
        if not has_supplier_gstin and has_recipient_gstin:
            # Check for Section 9(5) indicators
            nature_of_supply = self._get_value(row, 'nature_of_supply')
            if nature_of_supply and not pd.isna(nature_of_supply):
                nos_str = str(nature_of_supply).strip().upper()
                if '9(5)' in nos_str or 'SEC_9_5' in nos_str or 'SECTION 9(5)' in nos_str:
                    return True
            
            # Check doc_type for Section 9(5) or URP2B keywords
            doc_type = self._safe_string(self._get_value(row, 'doc_type'))
            supply_type = self._safe_string(self._get_value(row, 'supply_type'))
            
            combined = f"{doc_type} {supply_type}".lower()
            if '9(5)' in combined or 'urp2b' in combined or 'unregistered' in combined:
                return True
        
        return False
    
    def _detect_ecob2c_supply(self, row: pd.Series) -> bool:
        """
        Detect if row is a Section 9(5) B2C supply through e-commerce operator.
        ECOB2C supplies have supplier GSTIN but NO recipient GSTIN (consumer).
        """
        # Key indicator: Has supplier GSTIN but NO recipient GSTIN (B2C consumer scenario)
        supplier_gstin = self._get_value(row, 'supplier_gstin')
        has_supplier_gstin = supplier_gstin and not pd.isna(supplier_gstin)
        
        # Must NOT have recipient GSTIN (B2C consumer)
        recipient_gstin = self._get_value(row, 'recipient_gstin')
        if not recipient_gstin or pd.isna(recipient_gstin):
            recipient_gstin = row.get('_gstin')
        
        has_recipient_gstin = recipient_gstin and not pd.isna(recipient_gstin)
        
        # ECOB2C: Has supplier but NO recipient (B2C consumer scenario)
        if has_supplier_gstin and not has_recipient_gstin:
            # Check for Section 9(5) indicators
            nature_of_supply = self._get_value(row, 'nature_of_supply')
            if nature_of_supply and not pd.isna(nature_of_supply):
                nos_str = str(nature_of_supply).strip().upper()
                if '9(5)' in nos_str or 'SEC_9_5' in nos_str or 'SECTION 9(5)' in nos_str:
                    return True
            
            # Check doc_type for Section 9(5) or B2C keywords
            doc_type = self._safe_string(self._get_value(row, 'doc_type'))
            supply_type = self._safe_string(self._get_value(row, 'supply_type'))
            
            combined = f"{doc_type} {supply_type}".lower()
            if ('9(5)' in combined or 'ecob2c' in combined) and 'b2c' in combined:
                return True
        
        return False
    
    def _resolve_export_type(self, row: pd.Series) -> str:
        supply_text = (row.get('_supply_text') or '').lower()
        if 'wpay' in supply_text or 'with payment' in supply_text:
            return 'WPAY'
        return 'WOPAY'
    
    @staticmethod
    def _detect_sez(supply_text: str) -> bool:
        lowered = (supply_text or '').lower()
        return any(keyword in lowered for keyword in ('sez', 'special economic zone', 'deemed export'))
    
    def _determine_invoice_type(self, is_sez: bool, supply_text: str) -> str:
        if is_sez:
            lowered = (supply_text or '').lower()
            if 'without' in lowered and 'payment' in lowered:
                return 'SEZ supplies without payment'
            return 'SEZ supplies with payment'
        return 'Regular'
    
    def _resolve_source_state_code(self, row: pd.Series) -> Optional[str]:
        value = self._get_value(row, 'source_of_supply')
        code = self._state_code_from_value(value)
        if code:
            return code
        ecommerce_gstin = row.get('_ecommerce_gstin')
        if ecommerce_gstin:
            numeric = ecommerce_gstin[:2]
            return STATE_NUMERIC_TO_CODE.get(numeric)
        return None
    
    def _state_code_from_value(self, value) -> Optional[str]:
        candidate = self._safe_string(value)
        if not candidate:
            return None
        upper = candidate.upper()
        if upper in STATE_DETAILS:
            return upper
        normalized = normalize_label(candidate)
        if normalized in STATE_NAME_TO_CODE:
            return STATE_NAME_TO_CODE[normalized]
        if '-' in candidate:
            prefix = candidate.split('-')[0]
            digits = ''.join(ch for ch in prefix if ch.isdigit())
            if len(digits) == 2 and digits in STATE_NUMERIC_TO_CODE:
                return STATE_NUMERIC_TO_CODE[digits]
        digits = ''.join(ch for ch in candidate if ch.isdigit())
        if len(digits) == 2 and digits in STATE_NUMERIC_TO_CODE:
            return STATE_NUMERIC_TO_CODE[digits]
        return None
    
    @staticmethod
    def _format_place_of_supply(state_code: Optional[str]) -> Optional[str]:
        if not state_code:
            return None
        detail = STATE_DETAILS.get(state_code)
        if not detail:
            return state_code
        return f"{detail['code']}-{detail['name']}"
    
    @staticmethod
    def _is_amendment_sheet(sheet_name: str) -> bool:
        lowered = sheet_name.lower()
        return 'amend' in lowered or lowered.endswith('a')
    
    @staticmethod
    def _canonical_sheet_key(sheet_name: str) -> Optional[str]:
        simplified = normalize_label(sheet_name)
        # Check for amendment sheets first (more specific)
        if simplified.startswith('b2ba') or ('b2b' in simplified and ('amend' in simplified or simplified.endswith('a'))):
            return 'b2ba'
        if simplified.startswith('b2b'):
            return 'b2b'
        if simplified.startswith('b2cla') or ('b2cl' in simplified and ('amend' in simplified or simplified.endswith('a'))):
            return 'b2cla'
        if simplified.startswith('b2cl'):
            return 'b2cl'
        if simplified.startswith('b2csa') or ('b2cs' in simplified and ('amend' in simplified or simplified.endswith('a'))):
            return 'b2csa'
        if simplified.startswith('b2cs'):
            return 'b2cs'
        if simplified.startswith('cdnra') or ('cdnr' in simplified and ('amend' in simplified or simplified.endswith('a'))):
            return 'cdnra'
        if simplified.startswith('cdnr'):
            return 'cdnr'
        if simplified.startswith('cdnura') or ('cdnur' in simplified and ('amend' in simplified or simplified.endswith('a'))):
            return 'cdnura'
        if simplified.startswith('cdnur'):
            return 'cdnur'
        if simplified.startswith('expa') or ('exp' in simplified and ('amend' in simplified or simplified.endswith('a'))):
            return 'expa'
        if simplified.startswith('exp'):
            return 'export'
        if simplified.startswith('export'):
            return 'export'
        if simplified.startswith('atadja') or ('atadj' in simplified and 'amend' in simplified):
            return 'atadja'
        if simplified.startswith('atadj') or ('advance' in simplified and 'adjustment' in simplified):
            return 'atadj'
        if simplified.startswith('ata') or ('at' in simplified and ('amend' in simplified or simplified.endswith('a'))):
            return 'ata'
        if simplified.startswith('at') and len(simplified) == 2:
            return 'at'
        if 'advance' in simplified and 'tax' in simplified:
            return 'at'
        if simplified.startswith('exemp') or 'exempt' in simplified or 'nil' in simplified:
            return 'exemp'
        if simplified.startswith('hsnb2c') or ('hsn' in simplified and 'b2c' in simplified):
            return 'hsnb2c'
        if simplified.startswith('hsn') or 'hsn' in simplified:
            return 'hsn'
        if simplified.startswith('docs') or 'document' in simplified:
            return 'docs'
        if simplified.startswith('ecoa') or ('eco' in simplified and ('amend' in simplified or simplified.endswith('a'))):
            return 'ecoa'
        if simplified.startswith('ecob2b') or ('eco' in simplified and 'b2b' in simplified):
            return 'ecob2b'
        if simplified.startswith('ecourp2b') or ('eco' in simplified and 'urp2b' in simplified):
            return 'ecourp2b'
        if simplified.startswith('ecob2c') or ('eco' in simplified and 'b2c' in simplified):
            return 'ecob2c'
        if simplified.startswith('ecourp2c') or ('eco' in simplified and 'urp2c' in simplified):
            return 'ecourp2c'
        if simplified.startswith('ecoab2b') or ('ecoa' in simplified and 'b2b' in simplified):
            return 'ecoab2b'
        if simplified.startswith('ecoaurp2b') or ('ecoa' in simplified and 'urp2b' in simplified):
            return 'ecoaurp2b'
        if simplified.startswith('ecoab2c') or ('ecoa' in simplified and 'b2c' in simplified):
            return 'ecoab2c'
        if simplified.startswith('ecoaurp2c') or ('ecoa' in simplified and 'urp2c' in simplified):
            return 'ecoaurp2c'
        if simplified.startswith('eco') or 'ecommerce' in simplified or 'e-commerce' in simplified:
            return 'eco'
        return None
    
    @staticmethod
    def _round_money(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return round(float(value), 2)
    
    @staticmethod
    def _is_large_b2cl(invoice_value: Optional[float], is_interstate: bool, invoice_date: Optional[date] = None) -> bool:
        """
        Determine if invoice qualifies as B2CL based on:
        - Interstate transaction
        - Value threshold (₹100,000 from Aug 2024, ₹250,000 before)
        """
        if invoice_value is None or not is_interstate:
            return False
        
        # Determine threshold based on date
        threshold = 250000  # Default threshold (pre-Aug 2024)
        
        if invoice_date:
            # From August 2024 onwards, threshold is ₹100,000
            if invoice_date >= date(2024, 8, 1):
                threshold = 100000
        
        return abs(invoice_value) >= threshold
    
    @staticmethod
    def _to_float(value) -> Optional[float]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, str):
            stripped = value.replace(',', '').strip()
            if not stripped:
                return None
            try:
                return float(stripped)
            except ValueError:
                return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    
    # ------------------------------------------------------------------
    # New validation and calculation methods
    # ------------------------------------------------------------------
    
    def _normalize_reverse_charge(self, value) -> str:
        """
        Normalize reverse charge value to Y or N
        Accepts: Y, N, YES, NO, 1, 0, True, False
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 'N'  # Default to No
        
        value_str = str(value).strip().upper()
        
        if value_str in ['Y', 'YES', '1', 'TRUE']:
            return 'Y'
        elif value_str in ['N', 'NO', '0', 'FALSE', '']:
            return 'N'
        else:
            # Invalid value, log warning and default to N
            return 'N'
    
    def _parse_applicable_tax_rate(self, value) -> Optional[str]:
        """
        Parse applicable % of tax rate (65% feature)
        Returns '65%' if applicable, None otherwise
        """
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        
        value_str = str(value).strip().upper()
        
        if value_str in ['65%', '65']:
            return '65%'
        elif value_str in ['0%', '0', '']:
            return None
        else:
            return None
    
    def _calculate_igst(self, row: pd.Series) -> Optional[float]:
        """
        Calculate IGST amount based on:
        - Reverse charge (if Y, then 0)
        - Interstate flag (if True, use IGST)
        - Applicable tax rate (if 65%, apply reduction)
        """
        # Step 1: Check reverse charge
        if row.get('_reverse_charge') == 'Y':
            return 0.0
        
        # Step 2: Check if interstate
        if not row.get('_is_interstate', False):
            return 0.0  # Intrastate uses CGST+SGST
        
        # Step 3: Get taxable value and rate
        taxable_value = row.get('_taxable_value')
        rate = row.get('_rate')
        
        if taxable_value is None or rate is None:
            return 0.0
        
        # Step 4: Apply 65% reduction if applicable
        effective_rate = rate
        if row.get('_applicable_tax_rate') == '65%':
            effective_rate = rate * 0.65
        
        # Step 5: Calculate IGST
        igst = (taxable_value * effective_rate) / 100
        return round(igst, 2)
    
    def _calculate_cgst(self, row: pd.Series) -> Optional[float]:
        """
        Calculate CGST amount (intrastate only)
        """
        # Step 1: Check reverse charge
        if row.get('_reverse_charge') == 'Y':
            return 0.0
        
        # Step 2: Check if intrastate
        if row.get('_is_interstate', False):
            return 0.0  # Interstate uses IGST
        
        # Step 3: Get taxable value and rate
        taxable_value = row.get('_taxable_value')
        rate = row.get('_rate')
        
        if taxable_value is None or rate is None:
            return 0.0
        
        # Step 4: Apply 65% reduction if applicable
        effective_rate = rate / 2  # CGST is half of total rate
        if row.get('_applicable_tax_rate') == '65%':
            effective_rate = effective_rate * 0.65
        
        # Step 5: Calculate CGST
        cgst = (taxable_value * effective_rate) / 100
        return round(cgst, 2)
    
    def _calculate_sgst(self, row: pd.Series) -> Optional[float]:
        """
        Calculate SGST amount (intrastate only)
        Same as CGST for standard transactions
        """
        # Step 1: Check reverse charge
        if row.get('_reverse_charge') == 'Y':
            return 0.0
        
        # Step 2: Check if intrastate
        if row.get('_is_interstate', False):
            return 0.0  # Interstate uses IGST
        
        # Step 3: Get taxable value and rate
        taxable_value = row.get('_taxable_value')
        rate = row.get('_rate')
        
        if taxable_value is None or rate is None:
            return 0.0
        
        # Step 4: Apply 65% reduction if applicable
        effective_rate = rate / 2  # SGST is half of total rate
        if row.get('_applicable_tax_rate') == '65%':
            effective_rate = effective_rate * 0.65
        
        # Step 5: Calculate SGST
        sgst = (taxable_value * effective_rate) / 100
        return round(sgst, 2)
    
    def _validate_invoice_number(self, invoice_number: str, row_number: int) -> bool:
        """
        Validate invoice number per spec:
        - NOT NULL
        - Max 16 characters
        - Alphanumeric + / + - only
        """
        if not invoice_number or invoice_number == '':
            self.validation_tracker.add_error(
                row_number, 'Invoice Number', 
                'Invoice number cannot be empty', invoice_number
            )
            return False
        
        if len(invoice_number) > 16:
            self.validation_tracker.add_error(
                row_number, 'Invoice Number',
                f'Invoice number exceeds 16 characters: {len(invoice_number)}',
                invoice_number
            )
            return False
        
        # Validate format
        pattern = r'^[a-zA-Z0-9/\-]{1,16}$'
        if not re.match(pattern, invoice_number):
            self.validation_tracker.add_error(
                row_number, 'Invoice Number',
                'Invoice number has invalid characters. Allowed: A-Z, 0-9, /, -',
                invoice_number
            )
            return False
        
        return True
    
    def _validate_amount_not_zero_negative(self, amount: Optional[float], 
                                          field_name: str, row_number: int) -> bool:
        """
        Validate amount is not zero or negative
        """
        if amount is None:
            self.validation_tracker.add_error(
                row_number, field_name,
                f'{field_name} cannot be empty', amount
            )
            return False
        
        if amount < 0:
            self.validation_tracker.add_error(
                row_number, field_name,
                f'{field_name} cannot be negative: {amount}', amount
            )
            return False
        
        if amount == 0:
            self.validation_tracker.add_error(
                row_number, field_name,
                f'{field_name} cannot be zero', amount
            )
            return False
        
        return True
    
    def _validate_tax_rate(self, rate: Optional[float], invoice_type: str, row_number: int) -> bool:
        """
        Validate tax rate against allowed GST rates
        """
        if rate is None:
            self.validation_tracker.add_error(
                row_number, 'Rate',
                'Tax rate cannot be empty', rate
            )
            return False
        
        # For exports/SEZ, rate must be 0
        if invoice_type in ['SEZ supplies without payment', 'SEZ supplies with payment', 'Deemed Export']:
            if rate != 0:
                self.validation_tracker.add_warning(
                    row_number, 'Rate',
                    f'Export/SEZ rate should be 0%, found {rate}%',
                    'Set to 0%', rate, 0
                )
        else:
            # Regular B2B - validate against allowed rates
            if rate not in self.VALID_GST_RATES:
                self.validation_tracker.add_warning(
                    row_number, 'Rate',
                    f'Unusual tax rate: {rate}%. Valid rates: {self.VALID_GST_RATES}',
                    'Accepted with warning', rate, rate
                )
        
        return True
    
    def _validate_date_range(self, invoice_date: Optional[date], row_number: int) -> bool:
        """
        Validate invoice date is not in future and within reasonable range
        """
        if invoice_date is None:
            self.validation_tracker.add_error(
                row_number, 'Invoice Date',
                'Invoice date cannot be empty', invoice_date
            )
            return False
        
        current_date = date.today()
        
        # Check future date
        if invoice_date > current_date:
            self.validation_tracker.add_error(
                row_number, 'Invoice Date',
                'Invoice date is in future', invoice_date
            )
            return False
        
        # Check if too old (more than 5 years)
        years_old = (current_date - invoice_date).days / 365
        if years_old > 5:
            self.validation_tracker.add_warning(
                row_number, 'Invoice Date',
                f'Invoice date is {int(years_old)} years old',
                'Accepted with warning', invoice_date, invoice_date
            )
        
        return True
    
    def _check_duplicate_invoice(self, gstin: str, invoice_number: str, 
                                  invoice_date: Optional[date], row_number: int) -> bool:
        """
        Check for duplicate invoice (GSTIN + Invoice Number + Date)
        """
        if not gstin or not invoice_number or invoice_date is None:
            return True  # Skip duplicate check if key fields missing
        
        # Create uniqueness key
        key = f"{gstin}|{invoice_number}|{invoice_date}"
        
        if key in self.seen_invoice_keys:
            self.validation_tracker.add_error(
                row_number, 'Invoice Number',
                'Duplicate invoice: same GSTIN, Invoice Number, and Date',
                invoice_number
            )
            return False
        
        self.seen_invoice_keys.add(key)
        return True
    
    def _validate_cess_rate(self, cess_amount: float, taxable_value: float, row_number: int) -> bool:
        """
        Validate cess amount and rate
        """
        if cess_amount < 0:
            self.validation_tracker.add_error(
                row_number, 'Cess Amount',
                'Cess amount cannot be negative', cess_amount
            )
            return False
        
        if cess_amount > 0 and taxable_value > 0:
            cess_rate = (cess_amount / taxable_value) * 100
            if cess_rate not in self.VALID_CESS_RATES:
                self.validation_tracker.add_warning(
                    row_number, 'Cess Amount',
                    f'Unusual cess rate: {cess_rate:.2f}%',
                    'Accepted with warning', cess_amount, cess_amount
                )
        
        return True
    
    def _validate_receiver_name(self, name: str, row_number: int) -> str:
        """
        Validate and sanitize receiver name
        - Max 255 characters
        - Remove invalid special characters
        """
        if not name or name == '':
            return 'N/A'  # Default if empty
        
        # Truncate to 255
        if len(name) > 255:
            original = name
            name = name[:255]
            self.validation_tracker.add_warning(
                row_number, 'Name of Recipient',
                'Name exceeds 255 characters, truncated',
                'Truncated to 255 chars', original, name
            )
        
        # Check for special characters (allowed: a-zA-Z0-9 &\-.,\'())
        allowed_pattern = r'[^a-zA-Z0-9 &\-.,\'()]'
        if re.search(allowed_pattern, name):
            original = name
            name = re.sub(allowed_pattern, '', name)
            self.validation_tracker.add_warning(
                row_number, 'Name of Recipient',
                'Name contains invalid characters, sanitized',
                'Removed invalid chars', original, name
            )
        
        return name
    
    def _validate_note_number(self, note_number: str, row_number: int) -> bool:
        """
        Validate note number per spec:
        - NOT NULL
        - Max 16 characters
        - Alphanumeric + / + - only
        """
        if not note_number or note_number == '' or pd.isna(note_number):
            self.validation_tracker.add_error(
                row_number, 'Note Number', 
                'Note number cannot be empty', note_number
            )
            return False
        
        note_str = str(note_number).strip()
        
        if len(note_str) > 16:
            self.validation_tracker.add_error(
                row_number, 'Note Number',
                f'Note number exceeds 16 characters: {len(note_str)}',
                note_number
            )
            return False
        
        # Validate format: Alphanumeric + / + - only
        pattern = r'^[a-zA-Z0-9/\-]{1,16}$'
        if not re.match(pattern, note_str):
            self.validation_tracker.add_error(
                row_number, 'Note Number',
                'Note number has invalid characters. Allowed: A-Z, 0-9, /, -',
                note_number
            )
            return False
        
        return True
    
    def _validate_note_type(self, note_type: str, row_number: int) -> Tuple[Optional[str], Optional[str]]:
        """
        Validate note type and determine sign for amounts.
        Returns: (note_type, note_sign) where sign is 'POSITIVE' or 'NEGATIVE'
        """
        if not note_type or pd.isna(note_type):
            self.validation_tracker.add_error(
                row_number, 'Note Type',
                'Note type cannot be empty',
                note_type
            )
            return None, None
        
        note_type_clean = str(note_type).strip().upper()
        
        # Valid types: C (Credit Note), D (Debit Note), R (Refund Voucher)
        valid_types = ['C', 'D', 'R']
        if note_type_clean not in valid_types:
            self.validation_tracker.add_error(
                row_number, 'Note Type',
                f'Invalid note type: {note_type_clean}. Must be C, D, or R',
                note_type
            )
            return None, None
        
        # Determine sign:
        # C (Credit Note): NEGATIVE (reduces tax liability)
        # D (Debit Note): POSITIVE (increases tax liability)
        # R (Refund Voucher): NEGATIVE (treated as credit)
        if note_type_clean in ['C', 'R']:
            note_sign = 'NEGATIVE'
        else:  # D
            note_sign = 'POSITIVE'
        
        return note_type_clean, note_sign
    
    def _validate_cn_against_original_invoice(
        self, 
        customer_gstin: str,
        note_date: date,
        note_type: str,
        place_of_supply: str,
        reverse_charge: str,
        taxable_value: float,
        row_number: int,
        original_invoice_number: Optional[str] = None,
        original_invoice_date: Optional[date] = None
    ) -> bool:
        """
        Validate Credit/Debit Note against original B2B/B2CL invoice.
        This is a framework method for cross-sheet validation.
        
        Note: Full implementation requires access to previously processed B2B/B2CL data,
        which may need to be maintained in a separate tracking structure.
        """
        # If no original invoice context is provided, skip this validation
        if not original_invoice_number or not original_invoice_date:
            self.validation_tracker.add_warning(
                row_number, 'Original Invoice',
                'Original invoice reference not provided for CN validation',
                'Skipping cross-reference validation', '', ''
            )
            return True
        
        # Validate note date is not before original invoice date
        try:
            if pd.to_datetime(note_date) < pd.to_datetime(original_invoice_date):
                self.validation_tracker.add_error(
                    row_number, 'Note Date',
                    'CN date cannot be before original invoice date',
                    f'Note: {note_date}, Invoice: {original_invoice_date}'
                )
                return False
        except Exception as e:
            self.validation_tracker.add_warning(
                row_number, 'Date Validation',
                f'Unable to validate date ordering: {e}',
                'Accepted', '', ''
            )
        
        # Framework for future enhancement: Cross-reference with B2B/B2CL sheets
        # This would require maintaining a dictionary of processed invoices:
        # invoice_key = f"{customer_gstin}|{original_invoice_number}|{original_invoice_date}"
        # if invoice_key in self.b2b_invoice_data:
        #     original_data = self.b2b_invoice_data[invoice_key]
        #     
        #     # Validate GSTIN matches
        #     if customer_gstin != original_data['gstin']:
        #         error...
        #     
        #     # Validate POS matches
        #     if place_of_supply != original_data['pos']:
        #         error...
        #     
        #     # Validate RC matches
        #     if reverse_charge != original_data['rc']:
        #         warning...
        #     
        #     # For Credit Note: taxable value should not exceed original
        #     if note_type == 'C' and taxable_value > original_data['taxable_value']:
        #         error...
        
        return True
    
    def _reconcile_cn_against_invoice(
        self,
        customer_gstin: str,
        original_invoice_number: str,
        note_type: str,
        taxable_value: float,
        row_number: int
    ) -> bool:
        """
        Reconcile cumulative Credit/Debit Notes against original invoice.
        This is a framework method for advanced reconciliation logic.
        
        Note: Full implementation requires maintaining cumulative CN tracking
        across the entire processing session.
        """
        # Framework for future enhancement: Track cumulative CNs per invoice
        # cn_tracking_key = f"{customer_gstin}|{original_invoice_number}"
        # 
        # if cn_tracking_key not in self.cn_reconciliation_tracker:
        #     self.cn_reconciliation_tracker[cn_tracking_key] = {
        #         'total_cn_amount': 0,
        #         'total_dn_amount': 0,
        #         'cn_count': 0,
        #         'last_cn_date': None,
        #         'original_invoice_amount': None  # Would need to lookup from B2B/B2CL
        #     }
        # 
        # tracker = self.cn_reconciliation_tracker[cn_tracking_key]
        # 
        # # Update cumulative totals
        # if note_type == 'C':
        #     tracker['total_cn_amount'] += taxable_value
        #     tracker['cn_count'] += 1
        # elif note_type == 'D':
        #     tracker['total_dn_amount'] += taxable_value
        # 
        # # Validate cumulative CN does not exceed original invoice
        # if tracker['original_invoice_amount']:
        #     if tracker['total_cn_amount'] > tracker['original_invoice_amount']:
        #         self.validation_tracker.add_warning(
        #             row_number, 'CN Reconciliation',
        #             f'Cumulative CN (₹{tracker["total_cn_amount"]:.2f}) exceeds original invoice (₹{tracker["original_invoice_amount"]:.2f})',
        #             'Flagged for audit', '', ''
        #         )
        # 
        # # Check for multiple CNs in short period
        # if tracker['last_cn_date']:
        #     days_since_last = (datetime.now() - tracker['last_cn_date']).days
        #     if days_since_last < 7:
        #         self.validation_tracker.add_warning(
        #             row_number, 'CN Frequency',
        #             'Multiple CN within 7 days for same invoice',
        #             'Flagged for audit', '', ''
        #         )
        # 
        # tracker['last_cn_date'] = datetime.now()
        
        return True
