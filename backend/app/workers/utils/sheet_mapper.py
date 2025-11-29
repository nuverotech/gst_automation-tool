import re
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from app.services.template_service import TemplateService
from app.services.validation_service import ValidationService
from app.utils.logger import setup_logger
from app.workers.utils.rules_loader import GSTR1RuleBook, get_rule_book

logger = setup_logger(__name__)


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

VALID_UQC_CODES = {
    'BAG', 'BAL', 'BDL', 'BKL', 'BOU', 'BOX', 'BOY', 'BUN', 'CAN', 'CAR', 'CMS', 'CTN',
    'DAY', 'DOZ', 'DRM', 'DZN', 'FT', 'FTK', 'FTL', 'GMS', 'GRS', 'GYD', 'HRS', 'KGS',
    'KLR', 'KME', 'KGM', 'KLT', 'KMT', 'LTR', 'MTR', 'MLT', 'MMS', 'NOS', 'PAC', 'PCS',
    'PKT', 'PRS', 'QTL', 'ROL', 'SET', 'SQF', 'SQM', 'SQY', 'TBS', 'TGM', 'TNE', 'TON',
    'TUB', 'UGS', 'UNT', 'YDS'
}

TCS_KEYWORDS = {'tcs', 'eco_collected', 'eco collected'}


FIELD_KEYWORDS: Dict[str, List[str]] = {
    'gstin': ['customer gstin', 'customer gstn', 'gstin/uin', 'gstin', 'gstn'],
    'customer_name': ['customer name', 'receiver name', 'trade name'],
    'receiver_name': ['receiver name', 'trade name', 'customer name', 'name of recipient'],
    'invoice_number': ['invoice number', 'invoice no', 'invoice #', 'invoice id', 'document number'],
    'invoice_date': ['invoice date', 'date of invoice', 'invoice dt', 'document date'],
    'invoice_value': ['invoice value', 'value of invoice', 'invoice amount'],
    'place_of_supply': ['place of supply', 'pos'],
    'reverse_charge': ['reverse charge', 'rcm'],
    'invoice_type': ['invoice type'],
    'rate': ['gst%', 'tax rate', 'rate'],
    'taxable_value': ['taxable value', 'taxable amount'],
    'cess_amount': ['cess amount', 'cess'],
    'export_type': ['type of export', 'export type'],
    'type': ['type (e/oe)', 'type e/oe', 'type'],
    'note_number': ['note number', 'note no', 'dr./ cr. note no', 'dr./ cr. no.'],
    'note_date': ['note date', 'dr./ cr. note date', 'dr./cr. date'],
    'note_type': ['note type', 'type of note', 'dr./ cr.'],
    'note_value': ['note value', 'dr./ cr. value'],
    'ur_type': ['ur type', 'supply type'],
    'ecommerce_gstin': ['gstin of e-commerce', 'e-commerce gstin'],
    'applicable_percentage': ['applicable % of tax rate', 'applicable percentage', 'applicable percent', '65%'],
    'original_invoice_number': ['original invoice number', 'orig invoice no', 'previous invoice number'],
    'original_invoice_date': ['original invoice date', 'orig invoice date'],
    'revised_invoice_number': ['revised invoice number', 'new invoice number', 'amended invoice number'],
    'revised_invoice_date': ['revised invoice date', 'new invoice date'],
    'original_place_of_supply': ['original place of supply', 'original pos'],
    'revised_place_of_supply': ['revised place of supply', 'revised pos'],
    'original_rate': ['original rate', 'original tax rate'],
    'revised_rate': ['revised rate', 'revised tax rate'],
    'original_taxable_value': ['original taxable value', 'original taxable'],
    'revised_taxable_value': ['revised taxable value', 'revised taxable'],
    'original_cess_amount': ['original cess', 'original cess amount'],
    'revised_cess_amount': ['revised cess', 'revised cess amount'],
    'financial_year': ['financial year', 'fy'],
    'original_month': ['original month', 'return period', 'tax period'],
    'original_note_number': ['original note number', 'orig note number', 'prev note number'],
    'original_note_date': ['original note date', 'orig note date'],
    'revised_note_number': ['revised note number', 'new note number'],
    'revised_note_date': ['revised note date', 'new note date'],
    'note_supply_type': ['note supply type', 'supply type of note'],
    'port_code': ['port code'],
    'shipping_bill_number': ['shipping bill number', 'sb number'],
    'shipping_bill_date': ['shipping bill date', 'sb date'],
    'original_igst_amount': ['original igst amount', 'original igst'],
    'revised_igst_amount': ['revised igst amount', 'revised igst'],
    'gross_advance': ['gross advance received', 'advance amount', 'gross advance'],
    'gross_advance_adjusted': ['gross advance adjusted', 'advance adjusted amount'],
    'nil_rated_value': ['nil rated supplies', 'nil rated value'],
    'exempted_value': ['exempted (other than nil rated/non-gst)', 'exempted supplies'],
    'non_gst_value': ['non gst supplies', 'non-gst supplies'],
    'hsn_code': ['hsn', 'hsn code'],
    'description': ['description'],
    'uqc': ['uqc', 'unit'],
    'total_quantity': ['total quantity', 'quantity'],
    'total_taxable_value': ['total taxable value', 'taxable value'],
    'integrated_tax_amount': ['integrated tax amount', 'igst amount'],
    'central_tax_amount': ['central tax amount', 'cgst amount'],
    'state_tax_amount': ['state/ut tax amount', 'sgst amount'],
    'total_value': ['total value'],
    'document_type': ['nature of document', 'document type'],
    'document_start_number': ['sr. no from', 'sr no from', 'series from'],
    'document_end_number': ['sr. no to', 'sr no to', 'series to'],
    'document_total': ['total number'],
    'document_cancelled': ['cancelled'],
    'net_value': ['net value of supplies', 'net value'],
    'supplier_gstin': ['supplier gstin', 'gstin of supplier'],
    'supplier_name': ['supplier name'],
    'recipient_gstin': ['recipient gstin', 'gstin of recipient'],
    'recipient_name': ['recipient name'],
    'value_of_supplies': ['value of supplies', 'value'],
    'nature_of_supply': ['nature of supply'],
    'ecommerce_operator_gstin': ['e-commerce operator gstin', 'eco operator gstin'],
    'ecommerce_operator_name': ['e-commerce operator name', 'eco operator name'],
    'document_number': ['document number'],
    'document_date': ['document date'],
    'document_start_number': ['sr. no from', 'sr no from', 'series from'],
    'document_end_number': ['sr. no to', 'sr no to', 'series to'],
    'document_total': ['total number'],
    'document_cancelled': ['cancelled'],
    'original_document_number': ['original document number', 'original doc number'],
    'original_document_date': ['original document date', 'original doc date'],
    'revised_document_number': ['revised document number', 'revised doc number'],
    'revised_document_date': ['revised document date', 'revised doc date'],
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
    'note_number': ['cn number', 'dn number', 'note number', 'credit note number', 'debit note number', 'dr./ cr. no.'],
    'note_date': ['note date', 'cn date', 'dn date', 'credit note date', 'debit note date', 'dr./ cr. date'],
    'note_value': ['note value', 'credit amount', 'debit amount', 'dr./ cr. note value', 'dr./ cr. value', 'gross sales after discount'],
    'igst_rate': ['igst tax%', 'igst%', 'igst rate'],
    'cgst_rate': ['cgst tax%', 'cgst%', 'cgst rate'],
    'sgst_rate': ['sgst tax%', 'sgst%', 'sgst rate'],
    'rate': ['tax rate', 'tax percent', 'rate'],
    'igst_amount': ['igst amount'],
    'cgst_amount': ['cgst amount'],
    'sgst_amount': ['sgst amount'],
    'cess_amount': ['cess amount', 'cess'],
    'ecommerce_gstin': ['e-commerce gstin', 'ecommerce gstin', 'eco gstin'],
    'unique_type': ['unique', 'transaction type'],
    'export_flag': ['export'],
    'amendment_type': ['amendment type', 'amendment flag', 'amendment category'],
    'original_invoice_number': ['original invoice number', 'orig invoice', 'previous invoice'],
    'original_invoice_date': ['original invoice date', 'previous invoice date'],
    'revised_invoice_number': ['revised invoice number', 'new invoice number'],
    'revised_invoice_date': ['revised invoice date', 'new invoice date'],
    'original_taxable_value': ['original taxable value', 'original taxable'],
    'revised_taxable_value': ['revised taxable value', 'revised taxable'],
    'original_rate': ['original rate', 'original tax%'],
    'revised_rate': ['revised rate', 'revised tax%'],
    'original_cess_amount': ['original cess', 'original cess amount'],
    'revised_cess_amount': ['revised cess', 'revised cess amount'],
    'original_place_of_supply': ['original place of supply', 'original pos'],
    'revised_place_of_supply': ['revised place of supply', 'revised pos'],
    'financial_year': ['financial year', 'fy'],
    'original_month': ['original month', 'return period'],
    'applicable_percentage': ['applicable % of tax rate', 'applicable percentage', 'applicable percent', '65% flag'],
    'reverse_charge': ['reverse charge', 'rcm applicable', 'rcm'],
    'note_supply_type': ['note supply type', 'note supply', 'note classification'],
    'original_note_number': ['original note number', 'orig note no', 'previous note number'],
    'original_note_date': ['original note date', 'orig note date'],
    'revised_note_number': ['revised note number', 'new note number'],
    'revised_note_date': ['revised note date', 'new note date'],
    'port_code': ['port code'],
    'shipping_bill_number': ['shipping bill number', 'sb number'],
    'shipping_bill_date': ['shipping bill date', 'sb date'],
    'revised_invoice_value': ['revised invoice value', 'new invoice value'],
    'original_igst_amount': ['original igst amount', 'original igst'],
    'revised_igst_amount': ['revised igst amount', 'revised igst'],
    'advance_amount': ['advance amount', 'gross advance received'],
    'advance_adjusted_amount': ['advance adjusted amount', 'gross advance adjusted'],
    'advance_cess_amount': ['advance cess', 'advance cess amount'],
    'is_adjusted': ['is adjusted', 'adjusted flag'],
    'supply_category': ['supply category', 'supply type category'],
    'supply_value': ['supply value', 'value of supply'],
    'hsn_code': ['hsn', 'hsn code'],
    'hsn_description': ['hsn description', 'description of goods'],
    'uom': ['uom', 'uqc', 'unit'],
    'item_quantity': ['item quantity', 'quantity'],
    'document_number': ['document number', 'doc number'],
    'document_date': ['document date', 'doc date'],
    'document_type': ['document type', 'nature of document'],
    'document_cancelled': ['is cancelled', 'cancelled'],
    'net_value': ['net value of supplies', 'net value'],
    'ecommerce_operator_gstin': ['e-commerce operator gstin', 'eco operator gstin'],
    'ecommerce_operator_name': ['e-commerce operator name', 'eco operator name'],
    'nature_of_supply': ['nature of supply'],
    'supplier_gstin': ['supplier gstin', 'gstin of supplier'],
    'supplier_name': ['supplier name'],
    'recipient_gstin': ['recipient gstin', 'gstin of recipient'],
    'recipient_name': ['recipient name'],
    'value_of_supplies': ['value of supplies', 'value'],
}


class SheetMapper:
    SUPPORTED_SHEETS = (
        'b2b',
        'b2ba',
        'b2cl',
        'b2cla',
        'b2cs',
        'b2csa',
        'at',
        'ata',
        'atadj',
        'atadja',
        'exemp',
        'hsn_b2b',
        'hsn_b2c',
        'docs',
        'eco',
        'ecoa',
        'ecob2b',
        'ecourp2b',
        'ecob2c',
        'ecourp2c',
        'ecoab2b',
        'ecoaurp2b',
        'ecoab2c',
        'ecoaurp2c',
        'cdnr',
        'cdnra',
        'cdnur',
        'cdnura',
        'export',
        'expa',
    )
    
    def __init__(self, template_service: Optional[TemplateService] = None):
        self.template_service = template_service or TemplateService()
        self.validation_service = ValidationService()
        self.template_structure = self.template_service.load_template_structure()
        self.column_map: Dict[str, Optional[str]] = {}
        self.rule_book: GSTR1RuleBook = get_rule_book()
        
        self.sheet_mapping = self._build_sheet_mapping()
        self.template_field_headers = self._build_template_field_headers()
        
        logger.info("Template sheet mapping: %s", self.sheet_mapping)
        logger.info("GSTR rule sections available: %s", len(self.rule_book.all()))
    
    def prepare_data_for_template(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        if df.empty:
            return {}
        
        working_df = self._augment_dataframe(df)
        populated: Dict[str, pd.DataFrame] = {}
        
        for builder in (
            self._build_b2b,
            self._build_b2ba,
            self._build_b2cl,
            self._build_b2cla,
            self._build_b2cs,
            self._build_b2csa,
            self._build_at,
            self._build_ata,
            self._build_atadj,
            self._build_atadja,
            self._build_exemp,
            self._build_hsn_b2b,
            self._build_hsn_b2c,
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
            self._build_cdnr,
            self._build_cdnra,
            self._build_cdnur,
            self._build_cdnura,
            self._build_export,
            self._build_expa,
        ):
            sheet_name, sheet_df = builder(working_df)
            if sheet_name and not sheet_df.empty:
                populated[sheet_name] = sheet_df
        
        logger.info(
            "Prepared sheets: %s",
            {sheet: len(df_sheet) for sheet, df_sheet in populated.items()}
        )
        return populated
    
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
            lambda row: self._format_invoice_number(self._get_value(row, 'invoice_number')),
            axis=1
        )
        enriched['_invoice_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'invoice_date')), axis=1
        )
        
        enriched['_tax_total'] = enriched.apply(self._extract_tax_total, axis=1)
        enriched['_invoice_value'] = enriched.apply(self._resolve_invoice_value, axis=1)
        enriched['_taxable_value'] = enriched.apply(
            lambda row: self._resolve_taxable_value(row, row['_invoice_value']), axis=1
        )
        enriched['_total_value'] = enriched.apply(self._resolve_total_value, axis=1)
        enriched['_rate'] = enriched.apply(self._resolve_rate, axis=1)
        enriched['_cess_amount'] = enriched.apply(
            lambda row: self._resolve_cess_amount(row), axis=1
        )
        enriched['_applicable_percentage'] = enriched.apply(
            self._determine_applicable_percentage,
            axis=1
        )
        
        enriched['_receiver_name'] = enriched.apply(
            lambda row: self._truncate(self._safe_string(self._get_value(row, 'customer_name')), 100),
            axis=1
        )
        enriched['_ecommerce_gstin'] = enriched.apply(self._resolve_ecommerce_gstin, axis=1)
        enriched['_type_flag'] = enriched['_ecommerce_gstin'].apply(lambda val: 'E' if val else 'OE')
        enriched['_supply_text'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'supply_type') or self._get_value(row, 'unique_type')),
            axis=1
        )
        enriched['_document_type'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'document_type') or row.get('_doc_type')),
            axis=1
        )
        enriched['_document_number'] = enriched.apply(
            lambda row: self._format_invoice_number(self._get_value(row, 'document_number')) or row['_invoice_number'],
            axis=1
        )
        enriched['_document_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'document_date')) or row['_invoice_date'],
            axis=1
        )
        enriched['_document_cancelled'] = enriched.apply(
            lambda row: self._normalize_yes_no(self._get_value(row, 'document_cancelled')) == 'Y',
            axis=1
        )
        enriched['_document_category'] = enriched.apply(
            lambda row: self._document_bucket(row),
            axis=1
        )
        enriched['_eco_operator_gstin'] = enriched.apply(
            lambda row: self._clean_gstin_value(self._get_value(row, 'ecommerce_operator_gstin')),
            axis=1
        )
        enriched['_eco_operator_name'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'ecommerce_operator_name')),
            axis=1
        )
        enriched['_eco_nature'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'nature_of_supply') or row.get('_supply_text')),
            axis=1
        )
        enriched['_eco_net_value'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'net_value')),
            axis=1
        )
        enriched['_supplier_gstin'] = enriched.apply(
            lambda row: self._clean_gstin_value(self._get_value(row, 'supplier_gstin')),
            axis=1
        )
        enriched['_supplier_name'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'supplier_name')),
            axis=1
        )
        enriched['_recipient_gstin'] = enriched.apply(
            lambda row: self._clean_gstin_value(self._get_value(row, 'recipient_gstin')),
            axis=1
        )
        enriched['_recipient_name'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'recipient_name') or row['_receiver_name']),
            axis=1
        )
        enriched['_value_of_supplies'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'value_of_supplies')),
            axis=1
        )
        enriched['_is_adjusted_flag'] = enriched.apply(
            lambda row: self._normalize_yes_no(self._get_value(row, 'is_adjusted')) == 'Y',
            axis=1
        )
        enriched['_advance_amount'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'advance_amount')),
            axis=1
        )
        enriched['_advance_adjusted_amount'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'advance_adjusted_amount')),
            axis=1
        )
        enriched['_advance_cess_amount'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'advance_cess_amount')),
            axis=1
        )
        enriched['_supply_category'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'supply_category')),
            axis=1
        )
        enriched['_supply_value'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'supply_value')) or row.get('_taxable_value'),
            axis=1
        )
        enriched['_hsn_code'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'hsn_code')),
            axis=1
        )
        enriched['_hsn_description'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'hsn_description')),
            axis=1
        )
        enriched['_hsn_uqc'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'uom') or self._get_value(row, 'uqc')),
            axis=1
        )
        enriched['_hsn_quantity'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'item_quantity')),
            axis=1
        )
        enriched['_igst_amount_val'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'igst_amount')),
            axis=1
        )
        enriched['_cgst_amount_val'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'cgst_amount')),
            axis=1
        )
        enriched['_sgst_amount_val'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'sgst_amount')),
            axis=1
        )
        enriched['_is_advance'] = enriched.apply(self._detect_advance, axis=1)
        enriched['_is_advance_adjustment'] = enriched.apply(self._detect_advance_adjustment, axis=1)
        enriched['_amend_label'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'amendment_type')).lower(),
            axis=1
        )
        enriched['_original_invoice_number'] = enriched.apply(
            lambda row: self._format_invoice_number(self._get_value(row, 'original_invoice_number')),
            axis=1
        )
        enriched['_revised_invoice_number'] = enriched.apply(
            lambda row: self._format_invoice_number(self._get_value(row, 'revised_invoice_number')),
            axis=1
        )
        enriched['_original_invoice_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'original_invoice_date')),
            axis=1
        )
        enriched['_revised_invoice_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'revised_invoice_date')),
            axis=1
        )
        enriched['_original_taxable_value'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'original_taxable_value')),
            axis=1
        )
        enriched['_revised_taxable_value'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'revised_taxable_value')),
            axis=1
        )
        enriched['_original_rate'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'original_rate')),
            axis=1
        )
        enriched['_revised_rate'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'revised_rate')),
            axis=1
        )
        enriched['_original_cess_amount'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'original_cess_amount')),
            axis=1
        )
        enriched['_revised_cess_amount'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'revised_cess_amount')),
            axis=1
        )
        enriched['_revised_invoice_value'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'revised_invoice_value')),
            axis=1
        )
        enriched['_original_igst_amount'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'original_igst_amount')),
            axis=1
        )
        enriched['_revised_igst_amount'] = enriched.apply(
            lambda row: self._to_float(self._get_value(row, 'revised_igst_amount')),
            axis=1
        )
        enriched['_financial_year'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'financial_year')),
            axis=1
        )
        enriched['_original_month'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'original_month')),
            axis=1
        )
        enriched['_original_note_number'] = enriched.apply(
            lambda row: self._format_invoice_number(self._get_value(row, 'original_note_number')),
            axis=1
        )
        enriched['_revised_note_number'] = enriched.apply(
            lambda row: self._format_invoice_number(self._get_value(row, 'revised_note_number')) or row.get('_note_number'),
            axis=1
        )
        enriched['_original_note_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'original_note_date')),
            axis=1
        )
        enriched['_revised_note_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'revised_note_date')) or row.get('_note_date'),
            axis=1
        )
        enriched['_port_code'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'port_code')),
            axis=1
        )
        enriched['_shipping_bill_number'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'shipping_bill_number')),
            axis=1
        )
        enriched['_shipping_bill_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'shipping_bill_date')),
            axis=1
        )
        enriched['_original_pos_code'] = enriched.apply(
            lambda row: self._state_code_from_value(self._get_value(row, 'original_place_of_supply')) or row.get('_pos_code'),
            axis=1
        )
        enriched['_revised_pos_code'] = enriched.apply(
            lambda row: self._state_code_from_value(self._get_value(row, 'revised_place_of_supply')) or row.get('_pos_code'),
            axis=1
        )
        enriched['_amend_category'] = enriched.apply(self._determine_amendment_category, axis=1)
        enriched['_exempt_bucket'] = enriched.apply(self._determine_exempt_bucket, axis=1)
        enriched['_is_sez'] = enriched['_supply_text'].apply(self._detect_sez)
        enriched['_invoice_type'] = enriched.apply(
            lambda row: self._determine_invoice_type(row['_is_sez'], row['_supply_text']),
            axis=1
        )
        enriched['_reverse_charge_flag'] = enriched.apply(
            lambda row: self._normalize_yes_no(self._get_value(row, 'reverse_charge')) or 'N',
            axis=1
        )
        enriched['_note_supply_type'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'note_supply_type')) or row['_invoice_type'],
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
        enriched['_is_large_b2cl'] = enriched.apply(self._is_large_b2cl, axis=1)
        enriched['_ur_type'] = enriched.apply(self._determine_ur_type, axis=1)
        
        enriched['_doc_type'] = enriched.apply(
            lambda row: self._safe_string(self._get_value(row, 'doc_type') or self._get_value(row, 'unique_type')),
            axis=1
        )
        enriched['_note_number'] = enriched.apply(
            lambda row: self._format_invoice_number(self._get_value(row, 'note_number')) or row['_invoice_number'],
            axis=1
        )
        enriched['_note_date'] = enriched.apply(
            lambda row: self._parse_date(self._get_value(row, 'note_date')) or row['_invoice_date'],
            axis=1
        )
        enriched['_note_value'] = enriched.apply(self._resolve_note_value, axis=1)
        enriched['_note_type'] = enriched.apply(self._resolve_note_type, axis=1)
        enriched['_is_credit_or_debit'] = enriched.apply(
            lambda row: self._is_credit_or_debit(row['_doc_type'], row['_supply_text']) or bool(row['_note_type']),
            axis=1
        )
        enriched['_is_invoice_doc'] = enriched.apply(self._is_invoice_document, axis=1)
        
        enriched['_is_export'] = enriched.apply(self._detect_export, axis=1)
        enriched['_export_type'] = enriched.apply(self._resolve_export_type, axis=1)
        
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
            used_headers = set()
            for header in headers:
                normalized_header = normalize_label(header)
                for field_key, keywords in FIELD_KEYWORDS.items():
                    if field_key in header_map:
                        continue
                    if header in used_headers:
                        continue
                    if self._header_matches(header, normalized_header, field_key, keywords):
                        header_map[field_key] = header
                        used_headers.add(header)
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
            & df['_is_invoice_doc']
        )
        subset = df[mask]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            if not (row.get('_gstin') and row.get('_invoice_number') and row.get('_invoice_date') and row.get('_pos_code')):
                continue
            if row.get('_invoice_value') is None or row['_invoice_value'] <= 0:
                continue
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2b', 'gstin', row['_gstin'])
            self._set_field(payload, 'b2b', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'b2b', 'invoice_number', row['_invoice_number'])
            self._set_field(payload, 'b2b', 'invoice_date', row['_invoice_date'])
            self._set_field(payload, 'b2b', 'invoice_value', self._round_money(row['_invoice_value']))
            self._set_field(payload, 'b2b', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'b2b', 'reverse_charge', row['_reverse_charge_flag'])
            self._set_field(payload, 'b2b', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'b2b', 'invoice_type', row['_invoice_type'])
            self._set_field(payload, 'b2b', 'ecommerce_gstin', self._select_ecommerce_gstin(row))
            self._set_field(payload, 'b2b', 'rate', row['_rate'])
            self._set_field(payload, 'b2b', 'taxable_value', self._round_money(row['_taxable_value']))
            self._set_field(payload, 'b2b', 'cess_amount', self._round_money(abs(row['_cess_amount']) if row['_cess_amount'] is not None else None))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_b2ba(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('b2ba')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = (df['_amend_category'] == 'b2ba') & df['_has_valid_gstin']
        subset = df[mask]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            if not row.get('_original_invoice_number'):
                continue
            if self._is_invalid_amendment_sequence(row.get('_original_invoice_date'), row.get('_revised_invoice_date')):
                continue
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2ba', 'gstin', row['_gstin'])
            self._set_field(payload, 'b2ba', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'b2ba', 'original_invoice_number', row['_original_invoice_number'])
            self._set_field(payload, 'b2ba', 'original_invoice_date', row['_original_invoice_date'])
            self._set_field(payload, 'b2ba', 'invoice_number', row['_revised_invoice_number'] or row['_invoice_number'])
            self._set_field(payload, 'b2ba', 'invoice_date', row['_revised_invoice_date'] or row['_invoice_date'])
            self._set_field(payload, 'b2ba', 'invoice_value', self._round_money(row['_invoice_value']))
            self._set_field(payload, 'b2ba', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'b2ba', 'reverse_charge', row['_reverse_charge_flag'])
            self._set_field(payload, 'b2ba', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'b2ba', 'invoice_type', row['_invoice_type'])
            self._set_field(payload, 'b2ba', 'ecommerce_gstin', self._select_ecommerce_gstin(row))
            rate_value = row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate']
            taxable_value = row['_revised_taxable_value'] if row['_revised_taxable_value'] is not None else row['_taxable_value']
            cess_value = row['_revised_cess_amount'] if row['_revised_cess_amount'] is not None else row['_cess_amount']
            self._set_field(payload, 'b2ba', 'rate', rate_value)
            self._set_field(payload, 'b2ba', 'taxable_value', self._round_money(taxable_value))
            self._set_field(payload, 'b2ba', 'cess_amount', self._round_money(cess_value))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_b2cl(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('b2cl')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = (
            (~df['_has_valid_gstin'])
            & df['_is_large_b2cl']
            & (~df['_is_credit_or_debit'])
            & (~df['_is_export'])
            & df['_is_invoice_doc']
        )
        subset = df[mask]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            if not (row.get('_invoice_number') and row.get('_invoice_date') and row.get('_pos_code')):
                continue
            if row.get('_invoice_value') is None or row['_invoice_value'] <= 0:
                continue
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2cl', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'b2cl', 'invoice_number', row['_invoice_number'])
            self._set_field(payload, 'b2cl', 'invoice_date', row['_invoice_date'])
            self._set_field(payload, 'b2cl', 'invoice_value', self._round_money(abs(row['_invoice_value']) if row['_invoice_value'] is not None else None))
            self._set_field(payload, 'b2cl', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'b2cl', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'b2cl', 'rate', row['_rate'])
            self._set_field(payload, 'b2cl', 'taxable_value', self._round_money(abs(row['_taxable_value']) if row['_taxable_value'] is not None else None))
            self._set_field(payload, 'b2cl', 'ecommerce_gstin', self._select_ecommerce_gstin(row))
            self._set_field(payload, 'b2cl', 'cess_amount', self._round_money(abs(row['_cess_amount']) if row['_cess_amount'] is not None else None))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_b2cla(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('b2cla')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_amend_category'] == 'b2cla'
        subset = df[mask]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            if not row.get('_original_invoice_number'):
                continue
            if self._is_invalid_amendment_sequence(row.get('_original_invoice_date'), row.get('_revised_invoice_date')):
                continue
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2cla', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'b2cla', 'original_invoice_number', row['_original_invoice_number'])
            self._set_field(payload, 'b2cla', 'original_invoice_date', row['_original_invoice_date'])
            self._set_field(payload, 'b2cla', 'invoice_number', row['_revised_invoice_number'] or row['_invoice_number'])
            self._set_field(payload, 'b2cla', 'invoice_date', row['_revised_invoice_date'] or row['_invoice_date'])
            self._set_field(payload, 'b2cla', 'invoice_value', self._round_money(row['_invoice_value']))
            self._set_field(payload, 'b2cla', 'place_of_supply', self._format_place_of_supply(row['_original_pos_code'] or row['_pos_code']))
            self._set_field(payload, 'b2cla', 'revised_place_of_supply', self._format_place_of_supply(row['_revised_pos_code'] or row['_pos_code']))
            self._set_field(payload, 'b2cla', 'applicable_percentage', row['_applicable_percentage'])
            original_rate = row['_original_rate'] if row['_original_rate'] is not None else row['_rate']
            revised_rate = row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate']
            original_taxable = row['_original_taxable_value'] if row['_original_taxable_value'] is not None else row['_taxable_value']
            revised_taxable = row['_revised_taxable_value'] if row['_revised_taxable_value'] is not None else row['_taxable_value']
            original_cess = row['_original_cess_amount'] if row['_original_cess_amount'] is not None else row['_cess_amount']
            revised_cess = row['_revised_cess_amount'] if row['_revised_cess_amount'] is not None else row['_cess_amount']
            self._set_field(payload, 'b2cla', 'rate', revised_rate)
            self._set_field(payload, 'b2cla', 'original_rate', original_rate)
            self._set_field(payload, 'b2cla', 'taxable_value', self._round_money(revised_taxable))
            self._set_field(payload, 'b2cla', 'original_taxable_value', self._round_money(original_taxable))
            self._set_field(payload, 'b2cla', 'cess_amount', self._round_money(revised_cess))
            self._set_field(payload, 'b2cla', 'original_cess_amount', self._round_money(original_cess))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_b2cs(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('b2cs')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = (
            (~df['_has_valid_gstin'])
            & (~df['_is_large_b2cl'])
            & (~df['_is_credit_or_debit'])
            & (~df['_is_export'])
            & df['_is_invoice_doc']
        )
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        subset['_pos_display'] = subset['_pos_code'].apply(self._format_place_of_supply)
        subset['_taxable_amt'] = subset['_taxable_value'].apply(
            lambda val: self._to_float(val) or 0.0
        )
        subset['_cess_amt'] = subset['_cess_amount'].apply(
            lambda val: self._to_float(val) or 0.0
        )
        subset['_rate_value'] = subset['_rate']
        subset['_hsn_type_flag'] = subset['_ecommerce_gstin'].apply(lambda gstin: 'E' if gstin else 'OE')
        subset['_applicable_display'] = subset['_applicable_percentage'].fillna('')
        subset = subset[
            subset['_pos_display'].notna()
            & subset['_pos_display'].astype(str).str.strip().ne('')
            & subset['_rate_value'].notna()
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        grouped = (
            subset.groupby(
                ['_type_flag', '_pos_display', '_applicable_display', '_rate_value', '_ecommerce_gstin'],
                dropna=False
            )[['_taxable_amt', '_cess_amt']]
            .sum()
            .reset_index()
        )
        
        rows: List[Dict[str, object]] = []
        for _, row in grouped.iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2cs', 'type', row['_type_flag'] or 'OE')
            self._set_field(payload, 'b2cs', 'place_of_supply', row['_pos_display'])
            self._set_field(payload, 'b2cs', 'applicable_percentage', row['_applicable_display'] or None)
            self._set_field(payload, 'b2cs', 'rate', row['_rate_value'])
            self._set_field(payload, 'b2cs', 'taxable_value', self._round_money(row['_taxable_amt']))
            ecommerce_value = row['_ecommerce_gstin'] if row['_type_flag'] == 'E' else None
            self._set_field(payload, 'b2cs', 'ecommerce_gstin', ecommerce_value)
            self._set_field(payload, 'b2cs', 'cess_amount', self._round_money(row['_cess_amt']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_b2csa(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('b2csa')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_amend_category'] == 'b2csa'
        subset = df[mask]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2csa', 'financial_year', row['_financial_year'])
            self._set_field(payload, 'b2csa', 'original_month', row['_original_month'])
            self._set_field(payload, 'b2csa', 'type', row['_type_flag'] or 'OE')
            self._set_field(payload, 'b2csa', 'place_of_supply', self._format_place_of_supply(row['_original_pos_code'] or row['_pos_code']))
            rate_value = row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate']
            taxable_value = row['_revised_taxable_value'] if row['_revised_taxable_value'] is not None else row['_taxable_value']
            cess_value = row['_revised_cess_amount'] if row['_revised_cess_amount'] is not None else row['_cess_amount']
            self._set_field(payload, 'b2csa', 'rate', rate_value)
            self._set_field(payload, 'b2csa', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'b2csa', 'taxable_value', self._round_money(taxable_value))
            self._set_field(payload, 'b2csa', 'cess_amount', self._round_money(cess_value))
            ecommerce_value = row['_ecommerce_gstin'] if (row['_type_flag'] == 'E') else None
            self._set_field(payload, 'b2csa', 'ecommerce_gstin', ecommerce_value)
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_at(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('at')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = (
            df['_is_advance']
            & (~df['_is_advance_adjustment'])
            & (~df['_is_adjusted_flag'])
            & df['_advance_amount'].notna()
        )
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_pos_code'].notna()
            & subset['_rate'].notna()
            & (subset['_advance_amount'] > 0)
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        subset['_pos_display'] = subset['_pos_code'].apply(self._format_place_of_supply)
        subset['_advance_amt'] = subset['_advance_amount'].apply(lambda val: val if val is not None else 0.0)
        subset['_advance_cess'] = subset['_advance_cess_amount'].apply(lambda val: val if val is not None else 0.0)
        subset['_rate_value'] = subset['_rate']
        subset['_interstate_flag'] = subset['_is_interstate']
        
        grouped = (
            subset.groupby(['_pos_display', '_rate_value', '_interstate_flag'], dropna=False)[['_advance_amt', '_advance_cess']]
            .sum()
            .reset_index()
        )
        
        rows: List[Dict[str, object]] = []
        for _, row in grouped.iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'at', 'place_of_supply', row['_pos_display'])
            self._set_field(payload, 'at', 'rate', row['_rate_value'])
            self._set_field(payload, 'at', 'gross_advance', self._round_money(row['_advance_amt']))
            self._set_field(payload, 'at', 'cess_amount', self._round_money(row['_advance_cess']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ata(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ata')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = (df['_amend_category'] == 'ata') & df['_advance_amount'].notna()
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_financial_year'].astype(str).str.strip().ne('')
            & subset['_original_month'].astype(str).str.strip().ne('')
            & subset['_pos_code'].notna()
            & subset['_rate'].notna()
            & (subset['_advance_amount'] > 0)
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ata', 'financial_year', row['_financial_year'])
            self._set_field(payload, 'ata', 'original_month', row['_original_month'])
            self._set_field(payload, 'ata', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'ata', 'rate', row['_rate'])
            self._set_field(payload, 'ata', 'gross_advance', self._round_money(row['_advance_amount']))
            self._set_field(payload, 'ata', 'cess_amount', self._round_money(row['_advance_cess_amount'] or row['_cess_amount']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_atadj(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('atadj')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_is_advance_adjustment'] & (~df['_is_adjusted_flag']) & df['_advance_adjusted_amount'].notna()
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_pos_code'].notna()
            & subset['_rate'].notna()
            & (subset['_advance_adjusted_amount'] > 0)
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        subset['_pos_display'] = subset['_pos_code'].apply(self._format_place_of_supply)
        subset['_advance_adj_amt'] = subset['_advance_adjusted_amount'].apply(lambda val: val if val is not None else 0.0)
        subset['_advance_adj_cess'] = subset['_advance_cess_amount'].apply(lambda val: val if val is not None else 0.0)
        subset['_rate_value'] = subset['_rate']
        
        grouped = (
            subset.groupby(['_pos_display', '_rate_value'], dropna=False)[['_advance_adj_amt', '_advance_adj_cess']]
            .sum()
            .reset_index()
        )
        
        rows: List[Dict[str, object]] = []
        for _, row in grouped.iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'atadj', 'place_of_supply', row['_pos_display'])
            self._set_field(payload, 'atadj', 'rate', row['_rate_value'])
            self._set_field(payload, 'atadj', 'gross_advance_adjusted', self._round_money(row['_advance_adj_amt']))
            self._set_field(payload, 'atadj', 'cess_amount', self._round_money(row['_advance_adj_cess']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_atadja(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('atadja')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = (df['_amend_category'] == 'atadja') & df['_advance_adjusted_amount'].notna()
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_financial_year'].astype(str).str.strip().ne('')
            & subset['_original_month'].astype(str).str.strip().ne('')
            & subset['_pos_code'].notna()
            & subset['_rate'].notna()
            & (subset['_advance_adjusted_amount'] > 0)
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'atadja', 'financial_year', row['_financial_year'])
            self._set_field(payload, 'atadja', 'original_month', row['_original_month'])
            self._set_field(payload, 'atadja', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'atadja', 'rate', row['_rate'])
            self._set_field(payload, 'atadja', 'gross_advance_adjusted', self._round_money(row['_advance_adjusted_amount']))
            self._set_field(payload, 'atadja', 'cess_amount', self._round_money(row['_advance_cess_amount'] or row['_cess_amount']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_exemp(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('exemp')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df[df['_exempt_bucket'].notnull()].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        totals = {
            'nil_rated': 0.0,
            'exempted': 0.0,
            'non_gst': 0.0,
        }
        for _, row in subset.iterrows():
            bucket = row['_exempt_bucket']
            value = row['_supply_value'] if row['_supply_value'] is not None else row['_taxable_value']
            if bucket and value is not None:
                numeric = self._to_float(value)
                if numeric and numeric > 0:
                    totals[bucket] += numeric
        
        rows: List[Dict[str, object]] = []
        for bucket, total in totals.items():
            if total is None or total == 0:
                continue
            payload: Dict[str, object] = {}
            description = {
                'nil_rated': 'Nil Rated Supplies',
                'exempted': 'Exempted (Other than Nil rated/Non-GST supply)',
                'non_gst': 'Non GST Supplies',
            }[bucket]
            self._set_field(payload, 'exemp', 'description', description)
            if bucket == 'nil_rated':
                self._set_field(payload, 'exemp', 'nil_rated_value', self._round_money(total))
            elif bucket == 'exempted':
                self._set_field(payload, 'exemp', 'exempted_value', self._round_money(total))
            else:
                self._set_field(payload, 'exemp', 'non_gst_value', self._round_money(total))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_hsn_b2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('hsn_b2b')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df[(~df['_is_credit_or_debit']) & df['_has_valid_gstin']].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        subset['_hsn_code_clean'] = subset['_hsn_code'].apply(self._sanitize_hsn_code)
        subset['_hsn_description_clean'] = subset['_hsn_description'].apply(self._safe_string)
        subset['_hsn_uqc_clean'] = subset['_hsn_uqc'].apply(self._normalize_uqc)
        subset['_quantity_val'] = subset['_hsn_quantity'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_taxable_amt'] = subset['_taxable_value'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_igst_amt'] = subset['_igst_amount_val'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_cgst_amt'] = subset['_cgst_amount_val'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_sgst_amt'] = subset['_sgst_amount_val'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_cess_amt'] = subset['_cess_amount'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_total_value_amt'] = subset['_total_value'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_hsn_type_flag'] = subset['_ecommerce_gstin'].apply(lambda gstin: 'E' if gstin else 'OE')
        subset['_rate_value'] = subset['_rate']
        subset = subset[
            (subset['_taxable_amt'] > 0)
            & (
                subset['_hsn_code_clean'].notna()
                | subset['_hsn_description_clean'].astype(str).str.strip().ne('')
            )
            & subset['_rate_value'].notna()
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset['_is_service_hsn'] = subset['_hsn_code_clean'].apply(self._is_service_hsn)
        goods_mask = ~subset['_is_service_hsn']
        subset = subset[
            (~goods_mask)
            | (
                subset['_hsn_uqc_clean'].isin(VALID_UQC_CODES)
                & (subset['_quantity_val'] > 0)
            )
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        grouped = (
            subset.groupby(
                ['_hsn_code_clean', '_hsn_description_clean', '_hsn_uqc_clean', '_hsn_type_flag', '_rate_value'],
                dropna=False
            )[['_quantity_val', '_taxable_amt', '_igst_amt', '_cgst_amt', '_sgst_amt', '_cess_amt', '_total_value_amt']]
            .sum()
            .reset_index()
        )
        
        rows: List[Dict[str, object]] = []
        for _, row in grouped.iterrows():
            quantity_value = row['_quantity_val'] if row['_hsn_uqc_clean'] else None
            payload: Dict[str, object] = {}
            self._set_field(payload, 'hsn_b2b', 'hsn_code', row['_hsn_code_clean'])
            self._set_field(payload, 'hsn_b2b', 'description', row['_hsn_description_clean'])
            self._set_field(payload, 'hsn_b2b', 'uqc', row['_hsn_uqc_clean'] or None)
            self._set_field(payload, 'hsn_b2b', 'total_quantity', self._round_money(quantity_value) if quantity_value is not None else None)
            self._set_field(payload, 'hsn_b2b', 'rate', row['_rate_value'])
            self._set_field(payload, 'hsn_b2b', 'total_taxable_value', self._round_money(row['_taxable_amt']))
            self._set_field(payload, 'hsn_b2b', 'integrated_tax_amount', self._round_money(row['_igst_amt']))
            self._set_field(payload, 'hsn_b2b', 'central_tax_amount', self._round_money(row['_cgst_amt']))
            self._set_field(payload, 'hsn_b2b', 'state_tax_amount', self._round_money(row['_sgst_amt']))
            self._set_field(payload, 'hsn_b2b', 'cess_amount', self._round_money(row['_cess_amt']))
            self._set_field(payload, 'hsn_b2b', 'total_value', None)
            self._set_field(payload, 'hsn_b2b', 'type', row['_hsn_type_flag'])
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_hsn_b2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('hsn_b2c')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df[(~df['_is_credit_or_debit']) & (~df['_has_valid_gstin'])].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        subset['_hsn_code_clean'] = subset['_hsn_code'].apply(self._sanitize_hsn_code)
        subset['_hsn_description_clean'] = subset['_hsn_description'].apply(self._safe_string)
        subset['_hsn_uqc_clean'] = subset['_hsn_uqc'].apply(self._normalize_uqc)
        subset['_quantity_val'] = subset['_hsn_quantity'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_taxable_amt'] = subset['_taxable_value'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_igst_amt'] = subset['_igst_amount_val'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_cess_amt'] = subset['_cess_amount'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_rate_value'] = subset['_rate']
        subset['_hsn_type_flag'] = subset['_ecommerce_gstin'].apply(lambda gstin: 'E' if gstin else 'OE')
        subset = subset[
            (subset['_taxable_amt'] > 0)
            & subset['_rate_value'].notna()
            & (
                subset['_hsn_code_clean'].notna()
                | subset['_hsn_description_clean'].astype(str).str.strip().ne('')
            )
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset['_is_service_hsn'] = subset['_hsn_code_clean'].apply(self._is_service_hsn)
        goods_mask = ~subset['_is_service_hsn']
        subset = subset[
            (~goods_mask)
            | (
                subset['_hsn_uqc_clean'].isin(VALID_UQC_CODES)
                & (subset['_quantity_val'] > 0)
            )
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        grouped = (
            subset.groupby(
                ['_hsn_code_clean', '_hsn_description_clean', '_hsn_uqc_clean', '_rate_value', '_hsn_type_flag'],
                dropna=False
            )[['_quantity_val', '_taxable_amt', '_igst_amt', '_cess_amt']]
            .sum()
            .reset_index()
        )
        
        rows: List[Dict[str, object]] = []
        for _, row in grouped.iterrows():
            quantity_value = row['_quantity_val'] if row['_hsn_uqc_clean'] else None
            payload: Dict[str, object] = {}
            self._set_field(payload, 'hsn_b2c', 'hsn_code', row['_hsn_code_clean'])
            self._set_field(payload, 'hsn_b2c', 'description', row['_hsn_description_clean'])
            self._set_field(payload, 'hsn_b2c', 'uqc', row['_hsn_uqc_clean'] or None)
            self._set_field(payload, 'hsn_b2c', 'total_quantity', self._round_money(quantity_value) if quantity_value is not None else None)
            self._set_field(payload, 'hsn_b2c', 'rate', row['_rate_value'])
            self._set_field(payload, 'hsn_b2c', 'total_value', None)
            self._set_field(payload, 'hsn_b2c', 'type', row['_hsn_type_flag'])
            self._set_field(payload, 'hsn_b2c', 'total_taxable_value', self._round_money(row['_taxable_amt']))
            self._set_field(payload, 'hsn_b2c', 'integrated_tax_amount', self._round_money(row['_igst_amt']))
            self._set_field(payload, 'hsn_b2c', 'central_tax_amount', 0.0)
            self._set_field(payload, 'hsn_b2c', 'state_tax_amount', 0.0)
            self._set_field(payload, 'hsn_b2c', 'cess_amount', self._round_money(row['_cess_amt']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_docs(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('docs')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df[df['_document_category'].notnull() & df['_document_number'].fillna('').astype(str).str.strip().ne('')].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for category, group in subset.groupby('_document_category'):
            doc_numbers = sorted({self._safe_string(num) for num in group['_document_number'] if self._safe_string(num)})
            if not doc_numbers:
                continue
            document_code = self._normalize_doc_category(category)
            cancelled_count = int(group['_document_cancelled'].sum())
            total_count = len(doc_numbers)
            if cancelled_count > total_count:
                cancelled_count = total_count
            start_doc = doc_numbers[0]
            end_doc = doc_numbers[-1]
            start_numeric = self._extract_numeric_sequence(start_doc)
            end_numeric = self._extract_numeric_sequence(end_doc)
            if start_numeric is not None and end_numeric is not None:
                expected_span = abs(end_numeric - start_numeric) + 1
                if expected_span != total_count:
                    logger.warning(
                        "Document series gap detected for %s: expected %s, found %s",
                        category,
                        expected_span,
                        total_count,
                    )
            payload: Dict[str, object] = {}
            self._set_field(payload, 'docs', 'document_type', document_code)
            self._set_field(payload, 'docs', 'document_start_number', start_doc)
            self._set_field(payload, 'docs', 'document_end_number', end_doc)
            self._set_field(payload, 'docs', 'document_total', total_count)
            self._set_field(payload, 'docs', 'document_cancelled', cancelled_count)
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_eco(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('eco')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = (
            df['_eco_operator_gstin'].fillna('').astype(str).str.strip().ne('')
            & df['_eco_nature'].fillna('').astype(str).str.strip().ne('')
            & (df['_amend_category'] != 'ecoa')
        )
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset['_eco_nature_clean'] = subset['_eco_nature'].apply(self._normalize_eco_nature)
        subset = subset[
            subset['_eco_nature_clean'].notna()
            & subset['_eco_operator_gstin'].fillna('').astype(str).str.len().eq(15)
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        subset['_net_value_amt'] = subset['_eco_net_value'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_igst_amt'] = subset['_igst_amount_val'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_cgst_amt'] = subset['_cgst_amount_val'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_sgst_amt'] = subset['_sgst_amount_val'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_cess_amt'] = subset['_cess_amount'].apply(lambda val: self._to_float(val) or 0.0)
        subset = subset[subset['_net_value_amt'] > 0]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        grouped = (
            subset.groupby(
                ['_eco_operator_gstin', '_eco_operator_name', '_eco_nature_clean'],
                dropna=False
            )[['_net_value_amt', '_igst_amt', '_cgst_amt', '_sgst_amt', '_cess_amt']]
            .sum()
            .reset_index()
        )
        
        rows: List[Dict[str, object]] = []
        for _, row in grouped.iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'eco', 'nature_of_supply', row['_eco_nature_clean'])
            self._set_field(payload, 'eco', 'ecommerce_operator_gstin', row['_eco_operator_gstin'])
            self._set_field(payload, 'eco', 'ecommerce_operator_name', row['_eco_operator_name'])
            self._set_field(payload, 'eco', 'net_value', self._round_money(row['_net_value_amt']))
            self._set_field(payload, 'eco', 'integrated_tax_amount', self._round_money(row['_igst_amt']))
            self._set_field(payload, 'eco', 'central_tax_amount', self._round_money(row['_cgst_amt']))
            self._set_field(payload, 'eco', 'state_tax_amount', self._round_money(row['_sgst_amt']))
            self._set_field(payload, 'eco', 'cess_amount', self._round_money(row['_cess_amt']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoa(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ecoa')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df[df['_amend_category'] == 'ecoa'].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_financial_year'].astype(str).str.strip().ne('')
            & subset['_original_month'].astype(str).str.strip().ne('')
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset['_eco_nature_clean'] = subset['_eco_nature'].apply(self._normalize_eco_nature)
        subset = subset[
            subset['_eco_nature_clean'].notna()
            & subset['_eco_operator_gstin'].fillna('').astype(str).str.len().eq(15)
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset['_net_value_amt'] = subset['_eco_net_value'].apply(lambda val: self._to_float(val) or 0.0)
        subset = subset[subset['_net_value_amt'] > 0]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecoa', 'nature_of_supply', row['_eco_nature_clean'])
            self._set_field(payload, 'ecoa', 'financial_year', row['_financial_year'])
            self._set_field(payload, 'ecoa', 'original_month', row['_original_month'])
            self._set_field(payload, 'ecoa', 'ecommerce_operator_gstin', row['_eco_operator_gstin'])
            self._set_field(payload, 'ecoa', 'ecommerce_operator_name', row['_eco_operator_name'])
            self._set_field(payload, 'ecoa', 'net_value', self._round_money(row['_net_value_amt']))
            self._set_field(payload, 'ecoa', 'integrated_tax_amount', self._round_money(row['_igst_amount_val']))
            self._set_field(payload, 'ecoa', 'central_tax_amount', self._round_money(row['_cgst_amount_val']))
            self._set_field(payload, 'ecoa', 'state_tax_amount', self._round_money(row['_sgst_amount_val']))
            self._set_field(payload, 'ecoa', 'cess_amount', self._round_money(row['_cess_amount']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecob2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ecob2b')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df.copy()
        subset['_eco_nature_clean'] = subset['_eco_nature'].apply(self._normalize_eco_nature)
        mask = (
            subset['_eco_nature_clean'].eq('9(5)')
            & subset['_supplier_gstin'].fillna('').astype(str).str.len().eq(15)
            & subset['_recipient_gstin'].fillna('').astype(str).str.len().eq(15)
            & subset['_document_number'].fillna('').astype(str).str.strip().ne('')
            & subset['_document_date'].notna()
            & subset['_taxable_value'].notna()
            & (subset['_taxable_value'].apply(lambda val: self._to_float(val) or 0.0) > 0)
            & subset['_rate'].notna()
        )
        subset = subset[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            ~subset['_document_type'].fillna('').astype(str).str.lower().isin(TCS_KEYWORDS)
            & subset['_pos_code'].notna()
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            taxable_value = self._to_float(row['_taxable_value'])
            rate_value = self._to_float(row['_rate'])
            if taxable_value is None or taxable_value <= 0 or rate_value is None:
                continue
            supplier_state = self._gstin_state_code(row['_supplier_gstin'])
            recipient_state = self._gstin_state_code(row['_recipient_gstin'])
            pos_code = row['_pos_code']
            if not pos_code:
                continue
            is_interstate = bool(pos_code and recipient_state and pos_code != recipient_state)
            igst_amount, cgst_amount, sgst_amount = self._compute_tax_split(taxable_value, rate_value, is_interstate)
            cess_amount = self._round_money(self._to_float(row['_cess_amount']) or 0.0)
            value_of_supplies = self._resolve_value_of_supplies(row, taxable_value, igst_amount, cgst_amount, sgst_amount, cess_amount)
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecob2b', 'supplier_gstin', row['_supplier_gstin'])
            self._set_field(payload, 'ecob2b', 'supplier_name', row['_supplier_name'])
            self._set_field(payload, 'ecob2b', 'recipient_gstin', row['_recipient_gstin'])
            self._set_field(payload, 'ecob2b', 'recipient_name', row['_recipient_name'])
            self._set_field(payload, 'ecob2b', 'document_number', row['_document_number'])
            self._set_field(payload, 'ecob2b', 'document_date', row['_document_date'])
            self._set_field(payload, 'ecob2b', 'value_of_supplies', value_of_supplies)
            self._set_field(payload, 'ecob2b', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'ecob2b', 'document_type', row['_document_type'])
            self._set_field(payload, 'ecob2b', 'rate', rate_value)
            self._set_field(payload, 'ecob2b', 'taxable_value', self._round_money(taxable_value))
            self._set_field(payload, 'ecob2b', 'cess_amount', cess_amount)
            self._set_field(payload, 'ecob2b', 'integrated_tax_amount', igst_amount)
            self._set_field(payload, 'ecob2b', 'central_tax_amount', cgst_amount)
            self._set_field(payload, 'ecob2b', 'state_tax_amount', sgst_amount)
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecourp2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ecourp2b')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df.copy()
        subset['_eco_nature_clean'] = subset['_eco_nature'].apply(self._normalize_eco_nature)
        mask = (
            subset['_eco_nature_clean'].eq('9(5)')
            & subset['_supplier_gstin'].fillna('').astype(str).str.strip().eq('')
            & subset['_recipient_gstin'].fillna('').astype(str).str.len().eq(15)
            & subset['_document_number'].fillna('').astype(str).str.strip().ne('')
            & subset['_document_date'].notna()
            & subset['_taxable_value'].notna()
            & (subset['_taxable_value'].apply(lambda val: self._to_float(val) or 0.0) > 0)
            & subset['_rate'].notna()
        )
        subset = subset[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            ~subset['_document_type'].fillna('').astype(str).str.lower().isin(TCS_KEYWORDS)
            & subset['_pos_code'].notna()
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            taxable_value = self._to_float(row['_taxable_value'])
            rate_value = self._to_float(row['_rate'])
            if taxable_value is None or taxable_value <= 0 or rate_value is None:
                continue
            recipient_state = self._gstin_state_code(row['_recipient_gstin'])
            pos_code = row['_pos_code']
            if not pos_code:
                continue
            is_interstate = bool(pos_code and recipient_state and pos_code != recipient_state)
            igst_amount, cgst_amount, sgst_amount = self._compute_tax_split(taxable_value, rate_value, is_interstate)
            cess_amount = self._round_money(self._to_float(row['_cess_amount']) or 0.0)
            value_of_supplies = self._resolve_value_of_supplies(row, taxable_value, igst_amount, cgst_amount, sgst_amount, cess_amount)
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecourp2b', 'recipient_gstin', row['_recipient_gstin'])
            self._set_field(payload, 'ecourp2b', 'recipient_name', row['_recipient_name'])
            self._set_field(payload, 'ecourp2b', 'document_number', row['_document_number'])
            self._set_field(payload, 'ecourp2b', 'document_date', row['_document_date'])
            self._set_field(payload, 'ecourp2b', 'value_of_supplies', value_of_supplies)
            self._set_field(payload, 'ecourp2b', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'ecourp2b', 'document_type', row['_document_type'])
            self._set_field(payload, 'ecourp2b', 'rate', rate_value)
            self._set_field(payload, 'ecourp2b', 'taxable_value', self._round_money(taxable_value))
            self._set_field(payload, 'ecourp2b', 'cess_amount', cess_amount)
            self._set_field(payload, 'ecourp2b', 'integrated_tax_amount', igst_amount)
            self._set_field(payload, 'ecourp2b', 'central_tax_amount', cgst_amount)
            self._set_field(payload, 'ecourp2b', 'state_tax_amount', sgst_amount)
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecob2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ecob2c')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = (
            df['_eco_nature'].fillna('').astype(str).str.lower().str.contains('9(5)')
            & df['_supplier_gstin'].fillna('').astype(str).str.strip().ne('')
            & (~df['_amend_category'].isin(['ecoab2b', 'ecoab2c']))
        )
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_supplier_gstin'].astype(str).str.len().eq(15)
            & subset['_pos_code'].notna()
            & subset['_rate'].notna()
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        subset['_pos_display'] = subset['_pos_code'].apply(self._format_place_of_supply)
        subset['_taxable_amt'] = subset['_taxable_value'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_cess_amt'] = subset['_cess_amount'].apply(lambda val: self._to_float(val) or 0.0)
        subset = subset[subset['_taxable_amt'] > 0]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        grouped = (
            subset.groupby(['_supplier_gstin', '_supplier_name', '_pos_display', '_rate'], dropna=False)[['_taxable_amt', '_cess_amt']]
            .sum()
            .reset_index()
        )
        
        rows: List[Dict[str, object]] = []
        for _, row in grouped.iterrows():
            taxable_value = row['_taxable_amt']
            rate_value = row['_rate']
            cgst_amount = self._round_money(taxable_value * ((rate_value / 2) / 100.0))
            sgst_amount = cgst_amount
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecob2c', 'supplier_gstin', row['_supplier_gstin'])
            self._set_field(payload, 'ecob2c', 'supplier_name', row['_supplier_name'])
            self._set_field(payload, 'ecob2c', 'place_of_supply', row['_pos_display'])
            self._set_field(payload, 'ecob2c', 'rate', row['_rate'])
            self._set_field(payload, 'ecob2c', 'taxable_value', self._round_money(taxable_value))
            self._set_field(payload, 'ecob2c', 'central_tax_amount', cgst_amount)
            self._set_field(payload, 'ecob2c', 'state_tax_amount', sgst_amount)
            self._set_field(payload, 'ecob2c', 'cess_amount', self._round_money(row['_cess_amt']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecourp2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ecourp2c')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = (
            df['_eco_nature'].fillna('').astype(str).str.lower().str.contains('9(5)')
            & df['_supplier_gstin'].fillna('').astype(str).str.strip().eq('')
            & (~df['_amend_category'].isin(['ecoab2c', 'ecoaurp2c']))
        )
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_pos_code'].notna()
            & subset['_rate'].notna()
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        subset['_pos_display'] = subset['_pos_code'].apply(self._format_place_of_supply)
        subset['_taxable_amt'] = subset['_taxable_value'].apply(lambda val: self._to_float(val) or 0.0)
        subset['_cess_amt'] = subset['_cess_amount'].apply(lambda val: self._to_float(val) or 0.0)
        subset = subset[subset['_taxable_amt'] > 0]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        grouped = (
            subset.groupby(['_pos_display', '_rate'], dropna=False)[['_taxable_amt', '_cess_amt']]
            .sum()
            .reset_index()
        )
        
        rows: List[Dict[str, object]] = []
        for _, row in grouped.iterrows():
            taxable_value = row['_taxable_amt']
            rate_value = row['_rate']
            cgst_amount = self._round_money(taxable_value * ((rate_value / 2) / 100.0))
            sgst_amount = cgst_amount
            payload: Dict[str, object] = {}
            self._set_field(payload, 'ecourp2c', 'place_of_supply', row['_pos_display'])
            self._set_field(payload, 'ecourp2c', 'rate', row['_rate'])
            self._set_field(payload, 'ecourp2c', 'taxable_value', self._round_money(taxable_value))
            self._set_field(payload, 'ecourp2c', 'central_tax_amount', cgst_amount)
            self._set_field(payload, 'ecourp2c', 'state_tax_amount', sgst_amount)
            self._set_field(payload, 'ecourp2c', 'cess_amount', self._round_money(row['_cess_amt']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoab2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ecoab2b')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df[df['_amend_category'] == 'ecoab2b']
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            if self._is_invalid_amendment_sequence(row.get('_original_invoice_date'), row.get('_revised_invoice_date')):
                continue
            self._set_field(payload, 'ecoab2b', 'supplier_gstin', row['_supplier_gstin'])
            self._set_field(payload, 'ecoab2b', 'supplier_name', row['_supplier_name'])
            self._set_field(payload, 'ecoab2b', 'recipient_gstin', row['_recipient_gstin'])
            self._set_field(payload, 'ecoab2b', 'recipient_name', row['_recipient_name'])
            self._set_field(payload, 'ecoab2b', 'original_document_number', row['_original_invoice_number'] or row['_document_number'])
            self._set_field(payload, 'ecoab2b', 'original_document_date', row['_original_invoice_date'] or row['_document_date'])
            self._set_field(payload, 'ecoab2b', 'revised_document_number', row['_revised_invoice_number'] or row['_document_number'])
            self._set_field(payload, 'ecoab2b', 'revised_document_date', row['_revised_invoice_date'] or row['_document_date'])
            self._set_field(payload, 'ecoab2b', 'value_of_supplies', self._round_money(row['_value_of_supplies'] or row['_invoice_value']))
            self._set_field(payload, 'ecoab2b', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'ecoab2b', 'document_type', row['_document_type'])
            self._set_field(payload, 'ecoab2b', 'rate', row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate'])
            self._set_field(payload, 'ecoab2b', 'taxable_value', self._round_money(row['_revised_taxable_value'] if row['_revised_taxable_value'] is not None else row['_taxable_value']))
            self._set_field(payload, 'ecoab2b', 'cess_amount', self._round_money(row['_revised_cess_amount'] if row['_revised_cess_amount'] is not None else row['_cess_amount']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoaurp2b(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ecoaurp2b')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df[df['_amend_category'] == 'ecoaurp2b']
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            if self._is_invalid_amendment_sequence(row.get('_original_invoice_date'), row.get('_revised_invoice_date')):
                continue
            self._set_field(payload, 'ecoaurp2b', 'original_document_number', row['_original_invoice_number'] or row['_document_number'])
            self._set_field(payload, 'ecoaurp2b', 'original_document_date', row['_original_invoice_date'] or row['_document_date'])
            self._set_field(payload, 'ecoaurp2b', 'revised_document_number', row['_revised_invoice_number'] or row['_document_number'])
            self._set_field(payload, 'ecoaurp2b', 'revised_document_date', row['_revised_invoice_date'] or row['_document_date'])
            self._set_field(payload, 'ecoaurp2b', 'recipient_name', row['_recipient_name'])
            self._set_field(payload, 'ecoaurp2b', 'value_of_supplies', self._round_money(row['_value_of_supplies'] or row['_invoice_value']))
            self._set_field(payload, 'ecoaurp2b', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'ecoaurp2b', 'document_type', row['_document_type'])
            self._set_field(payload, 'ecoaurp2b', 'rate', row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate'])
            self._set_field(payload, 'ecoaurp2b', 'taxable_value', self._round_money(row['_revised_taxable_value'] if row['_revised_taxable_value'] is not None else row['_taxable_value']))
            self._set_field(payload, 'ecoaurp2b', 'cess_amount', self._round_money(row['_revised_cess_amount'] if row['_revised_cess_amount'] is not None else row['_cess_amount']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoab2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ecoab2c')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df[df['_amend_category'] == 'ecoab2c']
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            if self._is_invalid_amendment_sequence(row.get('_original_invoice_date'), row.get('_revised_invoice_date')):
                continue
            self._set_field(payload, 'ecoab2c', 'financial_year', row['_financial_year'])
            self._set_field(payload, 'ecoab2c', 'original_month', row['_original_month'])
            self._set_field(payload, 'ecoab2c', 'supplier_gstin', row['_supplier_gstin'])
            self._set_field(payload, 'ecoab2c', 'supplier_name', row['_supplier_name'])
            self._set_field(payload, 'ecoab2c', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'ecoab2c', 'rate', row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate'])
            self._set_field(payload, 'ecoab2c', 'taxable_value', self._round_money(row['_revised_taxable_value'] if row['_revised_taxable_value'] is not None else row['_taxable_value']))
            self._set_field(payload, 'ecoab2c', 'cess_amount', self._round_money(row['_revised_cess_amount'] if row['_revised_cess_amount'] is not None else row['_cess_amount']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_ecoaurp2c(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('ecoaurp2c')
        if not sheet_name:
            return None, pd.DataFrame()
        subset = df[df['_amend_category'] == 'ecoaurp2c']
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            if self._is_invalid_amendment_sequence(row.get('_original_invoice_date'), row.get('_revised_invoice_date')):
                continue
            self._set_field(payload, 'ecoaurp2c', 'financial_year', row['_financial_year'])
            self._set_field(payload, 'ecoaurp2c', 'original_month', row['_original_month'])
            self._set_field(payload, 'ecoaurp2c', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'ecoaurp2c', 'rate', row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate'])
            self._set_field(payload, 'ecoaurp2c', 'taxable_value', self._round_money(row['_revised_taxable_value'] if row['_revised_taxable_value'] is not None else row['_taxable_value']))
            self._set_field(payload, 'ecoaurp2c', 'cess_amount', self._round_money(row['_revised_cess_amount'] if row['_revised_cess_amount'] is not None else row['_cess_amount']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnr(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('cdnr')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_is_credit_or_debit'] & df['_has_valid_gstin']
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_note_number'].astype(str).str.strip().ne('')
            & subset['_note_date'].notna()
            & subset['_note_type'].astype(str).str.strip().ne('')
            & subset['_pos_code'].notna()
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            raw_note_value = abs(row['_note_value']) if row['_note_value'] is not None else None
            if raw_note_value is None or raw_note_value == 0:
                continue
            if row.get('_original_invoice_date') and row['_note_date'] and row['_note_date'] < row['_original_invoice_date']:
                continue
            taxable_value_raw = abs(row['_taxable_value']) if row['_taxable_value'] is not None else None
            if taxable_value_raw is not None and raw_note_value < taxable_value_raw:
                continue
            note_value = self._round_money(raw_note_value)
            taxable_value = self._round_money(taxable_value_raw)
            self._set_field(payload, 'cdnr', 'gstin', row['_gstin'])
            self._set_field(payload, 'cdnr', 'receiver_name', row['_receiver_name'])
            self._set_field(payload, 'cdnr', 'note_number', row['_note_number'])
            self._set_field(payload, 'cdnr', 'note_date', row['_note_date'])
            self._set_field(payload, 'cdnr', 'note_type', row['_note_type'])
            self._set_field(payload, 'cdnr', 'original_invoice_number', row['_original_invoice_number'])
            self._set_field(payload, 'cdnr', 'original_invoice_date', row['_original_invoice_date'])
            self._set_field(payload, 'cdnr', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'cdnr', 'reverse_charge', row['_reverse_charge_flag'])
            self._set_field(payload, 'cdnr', 'note_supply_type', row['_note_supply_type'])
            self._set_field(payload, 'cdnr', 'note_value', note_value)
            self._set_field(payload, 'cdnr', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'cdnr', 'rate', row['_rate'])
            self._set_field(payload, 'cdnr', 'taxable_value', taxable_value)
            self._set_field(payload, 'cdnr', 'cess_amount', self._round_money(abs(row['_cess_amount']) if row['_cess_amount'] is not None else None))
            self._set_field(payload, 'cdnr', 'ecommerce_gstin', self._select_ecommerce_gstin(row))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnra(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('cdnra')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_amend_category'] == 'cdnra'
        subset = df[mask]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            if not row.get('_original_note_number'):
                continue
            if self._is_invalid_amendment_sequence(row.get('_original_note_date'), row.get('_revised_note_date')):
                continue
            if not row.get('_note_type'):
                continue
            raw_note_value = abs(row['_note_value']) if row['_note_value'] is not None else None
            if raw_note_value is None or raw_note_value == 0:
                continue
            payload: Dict[str, object] = {}
            note_value = self._round_money(raw_note_value)
            taxable_value = self._round_money(
                abs(row['_revised_taxable_value']) if row['_revised_taxable_value'] is not None else (abs(row['_taxable_value']) if row['_taxable_value'] is not None else None)
            )
            cess_value = self._round_money(
                abs(row['_revised_cess_amount']) if row['_revised_cess_amount'] is not None else (abs(row['_cess_amount']) if row['_cess_amount'] is not None else None)
            )
            rate_value = row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate']
            self._set_field(payload, 'cdnra', 'gstin', row['_gstin'])
            self._set_field(payload, 'cdnra', 'receiver_name', row['_receiver_name'])
            self._set_field(payload, 'cdnra', 'original_note_number', row['_original_note_number'])
            self._set_field(payload, 'cdnra', 'original_note_date', row['_original_note_date'])
            self._set_field(payload, 'cdnra', 'note_number', row['_revised_note_number'] or row['_note_number'])
            self._set_field(payload, 'cdnra', 'note_date', row['_revised_note_date'] or row['_note_date'])
            self._set_field(payload, 'cdnra', 'note_type', row['_note_type'])
            self._set_field(payload, 'cdnra', 'place_of_supply', self._format_place_of_supply(row['_revised_pos_code'] or row['_pos_code']))
            self._set_field(payload, 'cdnra', 'reverse_charge', row['_reverse_charge_flag'])
            self._set_field(payload, 'cdnra', 'note_supply_type', row['_note_supply_type'])
            self._set_field(payload, 'cdnra', 'note_value', note_value)
            self._set_field(payload, 'cdnra', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'cdnra', 'rate', rate_value)
            self._set_field(payload, 'cdnra', 'taxable_value', taxable_value)
            self._set_field(payload, 'cdnra', 'cess_amount', cess_value)
            self._set_field(payload, 'cdnra', 'ecommerce_gstin', self._select_ecommerce_gstin(row))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnur(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('cdnur')
        if not sheet_name:
            return None, pd.DataFrame()
        allowed_ur_types = {'B2CL', 'EXPWP', 'EXPWOP'}
        mask = df['_is_credit_or_debit'] & (~df['_has_valid_gstin'])
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_note_number'].astype(str).str.strip().ne('')
            & subset['_note_date'].notna()
            & subset['_note_type'].astype(str).str.strip().ne('')
        ]
        subset = subset[subset['_ur_type'].isin(allowed_ur_types)]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset.assign(
            _note_value_abs=subset['_note_value'].apply(lambda val: abs(val) if val is not None else 0.0)
        )
        subset = subset[
            (subset['_note_value_abs'] > 0)
            & subset.apply(lambda row: row['_note_value_abs'] >= self._cdnur_threshold(row['_note_date']), axis=1)
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            if row.get('_original_invoice_date') and row['_note_date'] and row['_note_date'] < row['_original_invoice_date']:
                continue
            payload: Dict[str, object] = {}
            note_value = self._round_money(row['_note_value_abs'])
            taxable_value = self._round_money(abs(row['_taxable_value']) if row['_taxable_value'] is not None else None)
            rate_value = row['_rate']
            if row['_ur_type'] in ('EXPWP', 'EXPWOP'):
                rate_value = 0.0
            self._set_field(payload, 'cdnur', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'cdnur', 'ur_type', row['_ur_type'])
            self._set_field(payload, 'cdnur', 'note_number', row['_note_number'])
            self._set_field(payload, 'cdnur', 'note_date', row['_note_date'])
            self._set_field(payload, 'cdnur', 'note_type', row['_note_type'])
            self._set_field(payload, 'cdnur', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'cdnur', 'original_invoice_number', row['_original_invoice_number'])
            self._set_field(payload, 'cdnur', 'original_invoice_date', row['_original_invoice_date'])
            self._set_field(payload, 'cdnur', 'note_value', note_value)
            self._set_field(payload, 'cdnur', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'cdnur', 'note_supply_type', row['_note_supply_type'])
            self._set_field(payload, 'cdnur', 'rate', rate_value)
            self._set_field(payload, 'cdnur', 'taxable_value', taxable_value)
            self._set_field(payload, 'cdnur', 'cess_amount', self._round_money(abs(row['_cess_amount']) if row['_cess_amount'] is not None else None))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnura(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('cdnura')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_amend_category'] == 'cdnura'
        allowed_ur_types = {'B2CL', 'EXPWP', 'EXPWOP'}
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_ur_type'].isin(allowed_ur_types)
            & subset['_original_note_number'].astype(str).str.strip().ne('')
            & subset['_note_type'].astype(str).str.strip().ne('')
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset.assign(
            _note_value_abs=subset['_note_value'].apply(lambda val: abs(val) if val is not None else 0.0)
        )
        subset = subset[subset['_note_value_abs'] > 0]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in subset.iterrows():
            if not row.get('_original_note_number'):
                continue
            if self._is_invalid_amendment_sequence(row.get('_original_note_date'), row.get('_revised_note_date')):
                continue
            if not row.get('_note_type'):
                continue
            payload: Dict[str, object] = {}
            note_value = self._round_money(row['_note_value_abs'])
            taxable_value = self._round_money(
                abs(row['_revised_taxable_value']) if row['_revised_taxable_value'] is not None else (abs(row['_taxable_value']) if row['_taxable_value'] is not None else None)
            )
            cess_value = self._round_money(
                abs(row['_revised_cess_amount']) if row['_revised_cess_amount'] is not None else (abs(row['_cess_amount']) if row['_cess_amount'] is not None else None)
            )
            rate_value = row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate']
            if row['_ur_type'] in ('EXPWP', 'EXPWOP'):
                rate_value = 0.0
            self._set_field(payload, 'cdnura', 'ur_type', row['_ur_type'])
            self._set_field(payload, 'cdnura', 'note_type', row['_note_type'])
            self._set_field(payload, 'cdnura', 'original_note_number', row['_original_note_number'])
            self._set_field(payload, 'cdnura', 'original_note_date', row['_original_note_date'])
            self._set_field(payload, 'cdnura', 'note_number', row['_revised_note_number'] or row['_note_number'])
            self._set_field(payload, 'cdnura', 'note_date', row['_revised_note_date'] or row['_note_date'])
            self._set_field(payload, 'cdnura', 'place_of_supply', self._format_place_of_supply(row['_revised_pos_code'] or row['_pos_code']))
            self._set_field(payload, 'cdnura', 'note_value', note_value)
            self._set_field(payload, 'cdnura', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'cdnura', 'rate', rate_value)
            self._set_field(payload, 'cdnura', 'taxable_value', taxable_value)
            self._set_field(payload, 'cdnura', 'cess_amount', cess_value)
            self._set_field(payload, 'cdnura', 'note_supply_type', row['_note_supply_type'])
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_export(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('export')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_is_export'] & (~df['_is_credit_or_debit'])
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_invoice_number'].astype(str).str.strip().ne('')
            & subset['_invoice_date'].notna()
            & subset['_taxable_value'].notna()
            & subset['_export_type'].isin({'WPAY', 'WOPAY'})
            & subset['_invoice_value'].notna()
        ]
        subset = subset[
            subset['_invoice_value'] > 0
        ]
        subset = subset[
            subset['_taxable_value'] > 0
        ]
        subset = subset[
            ~(subset['_export_type'].eq('WOPAY') & subset['_rate'].fillna(0).abs().gt(1e-6))
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        today = date.today()
        for _, row in subset.iterrows():
            port_code = self._normalize_port_code(row['_port_code'])
            shipping_number = self._normalize_shipping_bill_number(row['_shipping_bill_number'])
            shipping_date = self._validate_shipping_bill_date(row['_shipping_bill_date'], row['_invoice_date'], today)
            if shipping_date is False:
                continue
            payload: Dict[str, object] = {}
            invoice_value = self._resolve_export_invoice_value(row, use_revised=False)
            if invoice_value is None or invoice_value <= 0:
                continue
            self._set_field(payload, 'export', 'export_type', row['_export_type'])
            self._set_field(payload, 'export', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'export', 'invoice_number', row['_invoice_number'])
            self._set_field(payload, 'export', 'invoice_date', row['_invoice_date'])
            self._set_field(payload, 'export', 'invoice_value', invoice_value)
            rate_value = row['_rate'] if row['_export_type'] == 'WPAY' else 0.0
            self._set_field(payload, 'export', 'rate', rate_value)
            self._set_field(payload, 'export', 'taxable_value', self._round_money(row['_taxable_value']))
            self._set_field(payload, 'export', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'export', 'port_code', port_code)
            self._set_field(payload, 'export', 'shipping_bill_number', shipping_number)
            self._set_field(payload, 'export', 'shipping_bill_date', shipping_date)
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_expa(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('expa')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_amend_category'] == 'expa'
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        subset = subset[
            subset['_original_invoice_number'].astype(str).str.strip().ne('')
            & subset['_original_invoice_date'].notna()
            & subset['_revised_invoice_date'].notna()
            & subset['_invoice_number'].astype(str).str.strip().ne('')
            & subset['_export_type'].isin({'WPAY', 'WOPAY'})
        ]
        subset = subset[
            subset['_invoice_value'].notna()
        ]
        subset = subset[
            subset['_invoice_value'] > 0
        ]
        subset = subset[
            ~(subset['_export_type'].eq('WOPAY') & subset['_rate'].fillna(0).abs().gt(1e-6))
        ]
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        today = date.today()
        for _, row in subset.iterrows():
            if self._is_invalid_amendment_sequence(row.get('_original_invoice_date'), row.get('_revised_invoice_date')):
                continue
            port_code = self._normalize_port_code(row['_port_code'])
            shipping_number = self._normalize_shipping_bill_number(row['_shipping_bill_number'])
            shipping_date = self._validate_shipping_bill_date(row['_shipping_bill_date'], row['_invoice_date'], today)
            if shipping_date is False:
                continue
            payload: Dict[str, object] = {}
            invoice_value = self._resolve_export_invoice_value(row, use_revised=True)
            if invoice_value is None or invoice_value <= 0:
                continue
            revised_taxable = row['_revised_taxable_value'] if row['_revised_taxable_value'] is not None else row['_taxable_value']
            self._set_field(payload, 'expa', 'export_type', row['_export_type'])
            self._set_field(payload, 'expa', 'original_invoice_number', row['_original_invoice_number'])
            self._set_field(payload, 'expa', 'original_invoice_date', row['_original_invoice_date'])
            self._set_field(payload, 'expa', 'invoice_number', row['_revised_invoice_number'] or row['_invoice_number'])
            self._set_field(payload, 'expa', 'invoice_date', row['_revised_invoice_date'] or row['_invoice_date'])
            self._set_field(payload, 'expa', 'invoice_value', invoice_value)
            rate_value = row['_revised_rate'] if row['_revised_rate'] is not None else row['_rate']
            if row['_export_type'] == 'WOPAY':
                rate_value = 0.0
            self._set_field(payload, 'expa', 'port_code', port_code)
            self._set_field(payload, 'expa', 'shipping_bill_number', shipping_number)
            self._set_field(payload, 'expa', 'shipping_bill_date', shipping_date)
            self._set_field(payload, 'expa', 'applicable_percentage', row['_applicable_percentage'])
            self._set_field(payload, 'expa', 'rate', rate_value)
            self._set_field(payload, 'expa', 'taxable_value', self._round_money(revised_taxable))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------
    def _set_field(self, payload: Dict[str, object], sheet_key: str, field_key: str, value):
        header = self.template_field_headers.get(sheet_key, {}).get(field_key)
        if not header:
            return
        value = self._prepare_output_value(value)
        if value is None:
            return
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return
        payload[header] = value

    def _prepare_output_value(self, value):
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        if isinstance(value, pd.Timestamp):
            value = value.to_pydatetime()
        if isinstance(value, datetime):
            value = value.date()
        if isinstance(value, date):
            return value.strftime('%d-%b-%Y')
        return value
    
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
    
    def _header_matches(self, header_value: str, normalized_header: str, field_key: str, keywords: List[str]) -> bool:
        header_lower = header_value.lower()
        for keyword in keywords:
            normalized_keyword = normalize_label(keyword)
            if not normalized_keyword:
                continue
            if normalized_keyword in normalized_header:
                if field_key == 'type':
                    if 'note' in normalized_header or 'export' in normalized_header:
                        continue
                if field_key == 'note_type' and 'note' not in normalized_header:
                    continue
                if field_key == 'note_value' and 'note' not in normalized_header:
                    continue
                if field_key == 'rate':
                    if 'gstin' in header_lower:
                        continue
                    if '%' not in header_value and 'rate' not in header_lower and 'tax' not in header_lower:
                        continue
                return True
        if field_key == 'rate':
            if 'gstin' in header_lower:
                return False
            if '%' in header_value and 'gst' in header_lower:
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
    
    def _format_invoice_number(self, value) -> str:
        text = self._safe_string(value)
        if not text:
            return ''
        cleaned = ''.join(ch for ch in text.upper() if ch.isalnum() or ch in {'/', '-'})
        return cleaned[:16]
    
    def _determine_applicable_percentage(self, row: pd.Series) -> Optional[str]:
        explicit = self._safe_string(self._get_value(row, 'applicable_percentage'))
        if explicit:
            normalized = explicit.replace('%', '').replace('percent', '').strip()
            if normalized in ('65', '65.0', '0.65'):
                return '65%'
            if normalized in ('0', '0.0'):
                return None
        text = (row.get('_supply_text') or '').lower()
        if '65%' in text or '65 percent' in text or 'gold' in text:
            return '65%'
        return None
    
    @staticmethod
    def _normalize_yes_no(value) -> Optional[str]:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return None
        text = str(value).strip().lower()
        if not text:
            return None
        if text in ('y', 'yes', 'true', '1'):
            return 'Y'
        if text in ('n', 'no', 'false', '0'):
            return 'N'
        return text.upper()
    
    def _determine_amendment_category(self, row: pd.Series) -> Optional[str]:
        label = (row.get('_amend_label') or '').lower()
        doc_text = (row.get('_doc_type') or '').lower()
        combined = f"{label} {doc_text}"
        
        if row.get('_original_note_number'):
            return 'cdnra' if row.get('_has_valid_gstin') else 'cdnura'
        
        if 'ecoab2b' in combined or ('eco' in combined and 'b2b' in combined and 'amend' in combined):
            return 'ecoab2b'
        if 'ecoaurp2b' in combined or ('eco' in combined and 'urp2b' in combined and 'amend' in combined):
            return 'ecoaurp2b'
        if 'ecoab2c' in combined or ('eco' in combined and 'b2c' in combined and 'amend' in combined):
            return 'ecoab2c'
        if 'ecoaurp2c' in combined or ('eco' in combined and 'urp2c' in combined and 'amend' in combined):
            return 'ecoaurp2c'
        if 'eco' in combined and 'amend' in combined:
            return 'ecoa'
        if 'advance adjustment' in combined and 'amend' in combined:
            return 'atadja'
        if 'advance' in combined and 'amend' in combined:
            return 'ata'
        if 'cdnr' in combined and 'amend' in combined:
            return 'cdnra'
        if 'cdnur' in combined and 'amend' in combined:
            return 'cdnura'
        if 'expa' in combined or ('export' in combined and 'amend' in combined):
            return 'expa'
        if 'b2ba' in combined or 'b2b amend' in combined:
            return 'b2ba'
        if 'b2cla' in combined or 'b2cl amend' in combined:
            return 'b2cla'
        if 'b2csa' in combined or 'b2cs amend' in combined:
            return 'b2csa'
        
        if row.get('_eco_operator_gstin') and row.get('_eco_nature'):
            if row.get('_financial_year') or row.get('_original_month'):
                if row.get('_supplier_gstin'):
                    return 'ecoab2c'
                if not row.get('_supplier_gstin'):
                    return 'ecoaurp2c'
                return 'ecoa'
            if row.get('_amend_label') and 'b2b' in row['_amend_label'].lower():
                return 'ecoab2b'
        
        if row.get('_is_advance_adjustment') and (row.get('_financial_year') or row.get('_original_month')):
            return 'atadja'
        if row.get('_is_advance') and (row.get('_financial_year') or row.get('_original_month')):
            return 'ata'
        if row.get('_is_advance_adjustment'):
            return 'atadj'
        
        if row.get('_original_invoice_number') and row.get('_is_export'):
            return 'expa'
        if row.get('_original_invoice_number'):
            if row.get('_has_valid_gstin'):
                return 'b2ba'
            if row.get('_is_large_b2cl'):
                return 'b2cla'
        if row.get('_financial_year') or row.get('_original_month'):
            return 'b2csa'
        return None
    
    def _resolve_ecommerce_gstin(self, row: pd.Series) -> str:
        gstin = self._clean_gstin_value(self._get_value(row, 'ecommerce_gstin'))
        if gstin and gstin != row.get('_gstin'):
            return gstin
        return ''
    
    def _select_ecommerce_gstin(self, row: pd.Series) -> Optional[str]:
        gstin = row.get('_ecommerce_gstin')
        if not gstin:
            return None
        if row.get('_type_flag') == 'E':
            return gstin
        return None
    
    def _normalize_port_code(self, value) -> Optional[str]:
        text = self._safe_string(value)
        if not text:
            return None
        digits = ''.join(ch for ch in text if ch.isdigit())
        if len(digits) == 6:
            return digits
        return None
    
    def _normalize_shipping_bill_number(self, value) -> Optional[str]:
        text = self._safe_string(value)
        if not text:
            return None
        if re.fullmatch(r'[A-Za-z0-9/\-]{1,20}', text):
            return text.upper()
        return None
    
    def _validate_shipping_bill_date(
        self,
        shipping_date: Optional[date],
        invoice_date: Optional[date],
        today: Optional[date] = None
    ):
        if shipping_date is None:
            return None
        if isinstance(shipping_date, pd.Timestamp):
            shipping_date = shipping_date.to_pydatetime().date()
        if today is None:
            today = date.today()
        if shipping_date > today:
            return None
        if invoice_date and shipping_date < invoice_date:
            return False
        return shipping_date
    
    def _sanitize_hsn_code(self, value) -> Optional[str]:
        text = ''.join(ch for ch in self._safe_string(value) if ch.isdigit())
        if 4 <= len(text) <= 8:
            return text
        return None
    
    @staticmethod
    def _is_service_hsn(hsn_code: Optional[str]) -> bool:
        return bool(hsn_code and hsn_code.startswith('99'))
    
    def _normalize_uqc(self, value) -> str:
        text = self._safe_string(value).upper()
        if text in VALID_UQC_CODES:
            return text
        return ''
    
    def _extract_numeric_sequence(self, value: str) -> Optional[int]:
        text = self._safe_string(value)
        digits = ''.join(ch for ch in text if ch.isdigit())
        if digits:
            try:
                return int(digits)
            except ValueError:
                return None
        return None
    
    @staticmethod
    def _normalize_doc_category(category: str) -> str:
        mapping = {
            'Invoices': 'INV',
            'Credit Notes': 'CDN',
            'Debit Notes': 'DBN',
        }
        return mapping.get(category, 'OTH')
    
    def _normalize_eco_nature(self, value: str) -> Optional[str]:
        text = self._safe_string(value).lower()
        if not text:
            return None
        if 'tcs' in text or '52' in text:
            return 'TCS'
        if '9(5)' in text or 'sec 9' in text or '9( 5' in text:
            return '9(5)'
        return None
    
    @staticmethod
    def _gstin_state_code(gstin: Optional[str]) -> Optional[str]:
        if not gstin or len(gstin) < 2:
            return None
        digits = ''.join(ch for ch in gstin[:2] if ch.isdigit())
        if len(digits) != 2:
            return None
        return STATE_NUMERIC_TO_CODE.get(digits)
    
    def _compute_tax_split(self, taxable_value: Optional[float], rate: Optional[float], is_interstate: bool) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        if taxable_value is None or rate is None:
            return None, None, None
        igst = cgst = sgst = 0.0
        if is_interstate:
            igst = self._round_money(taxable_value * (rate / 100.0))
        else:
            half_rate = rate / 2.0
            cgst = self._round_money(taxable_value * (half_rate / 100.0))
            sgst = cgst
        return igst, cgst, sgst
    
    def _resolve_value_of_supplies(self, row: pd.Series, taxable_value: Optional[float], igst: Optional[float], cgst: Optional[float], sgst: Optional[float], cess: Optional[float]) -> Optional[float]:
        explicit = row.get('_value_of_supplies')
        if explicit is not None:
            return self._round_money(explicit)
        components = [taxable_value, igst, cgst, sgst, cess]
        numeric = [val for val in components if val is not None]
        if not numeric:
            return None
        return self._round_money(sum(numeric))
    
    @staticmethod
    def _is_post_threshold(invoice_date: Optional[date]) -> bool:
        if not isinstance(invoice_date, date):
            return False
        return invoice_date >= date(2024, 8, 1)
    
    def _cdnur_threshold(self, note_date: Optional[date]) -> float:
        if isinstance(note_date, date) and note_date < date(2024, 8, 1):
            return 250000.0
        return 100000.0
    
    @staticmethod
    def _truncate(value: str, max_length: int) -> str:
        if not value:
            return ''
        if len(value) <= max_length:
            return value
        return value[:max_length]
    
    def _clean_gstin_value(self, value) -> str:
        clean_value = self._safe_string(value).upper()
        if not clean_value:
            return ''
        clean_value = re.sub(r'[^0-9A-Z]', '', clean_value)
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
        # Excel serial date handling
        if isinstance(value, (int, float)) and not pd.isna(value):
            serial = float(value)
            # XLS/XLSB date serials are typically > 20000 for year >= 1955
            if serial > 20000:
                try:
                    excel_base = date(1899, 12, 30)
                    return (excel_base + timedelta(days=serial))
                except Exception:
                    pass
        parsed = pd.to_datetime(value, errors='coerce')
        if pd.isna(parsed):
            return None
        return parsed.date()
    
    def _resolve_invoice_value(self, row: pd.Series) -> Optional[float]:
        invoice_value = self._to_float(self._get_value(row, 'invoice_value'))
        taxable = self._to_float(self._get_value(row, 'taxable_value'))
        tax_total = row.get('_tax_total')
        if tax_total is None:
            tax_total = self._extract_tax_total(row)
        calculated = None
        if taxable is not None:
            calculated = taxable + tax_total if tax_total is not None else taxable
        if invoice_value is None:
            candidates = [
                self._get_value(row, 'gross_amount'),
                self._get_value(row, 'mrp_value'),
            ]
            for value in candidates:
                numeric = self._to_float(value)
                if numeric is not None:
                    invoice_value = numeric
                    break
        if invoice_value is None and calculated is not None:
            invoice_value = calculated
        if invoice_value is not None and calculated is not None and abs(invoice_value - calculated) > 0.05:
            logger.warning(
                "Invoice value mismatch detected (provided=%s, calculated=%s). Using calculated total.",
                invoice_value,
                calculated,
            )
            invoice_value = calculated
        if taxable is not None and invoice_value is not None and invoice_value < taxable:
            invoice_value = taxable
        return invoice_value
    
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
    
    def _resolve_total_value(self, row: pd.Series) -> Optional[float]:
        total_value = self._to_float(self._get_value(row, 'total_value'))
        if total_value is not None:
            return total_value
        invoice_value = row.get('_invoice_value')
        if invoice_value is not None:
            return invoice_value
        taxable = row.get('_taxable_value')
        tax_total = row.get('_tax_total')
        if taxable is not None and tax_total is not None:
            return taxable + tax_total
        return taxable
    
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
    
    def _resolve_export_invoice_value(self, row: pd.Series, use_revised: bool = False) -> Optional[float]:
        taxable = row['_revised_taxable_value'] if use_revised and row['_revised_taxable_value'] is not None else row['_taxable_value']
        igst_candidates = []
        if use_revised:
            igst_candidates.append(row.get('_revised_igst_amount'))
        igst_candidates.append(self._to_float(self._get_value(row, 'igst_amount')))
        igst_candidates.append(row.get('_tax_total'))
        igst = next((val for val in igst_candidates if val is not None), None)
        invoice_override = row['_revised_invoice_value'] if use_revised and row['_revised_invoice_value'] is not None else row['_invoice_value']
        if taxable is not None and igst is not None:
            return self._round_money(taxable + igst)
        if invoice_override is not None:
            return self._round_money(invoice_override)
        if taxable is not None:
            return self._round_money(taxable)
        if igst is not None:
            return self._round_money(igst)
        return None
    
    def _resolve_note_type(self, row: pd.Series) -> Optional[str]:
        provided = self._safe_string(self._get_value(row, 'note_type')).upper()
        if provided in ('C', 'D', 'R'):
            return provided
        return self._determine_note_type(row.get('_doc_type'), row.get('_supply_text'), row.get('_note_value'))
    
    def _determine_note_type(self, doc_type: str, supply_text: str, note_value: Optional[float]) -> Optional[str]:
        doc_type_lower = f"{doc_type or ''} {supply_text or ''}".lower()
        if 'refund' in doc_type_lower:
            return 'R'
        if 'credit' in doc_type_lower or 'cn' in doc_type_lower:
            return 'C'
        if 'debit' in doc_type_lower or 'dn' in doc_type_lower:
            return 'D'
        return None
    
    def _determine_ur_type(self, row: pd.Series) -> str:
        if row.get('_is_export'):
            return 'EXPWP' if row.get('_export_type') == 'WPAY' else 'EXPWOP'
        return 'B2CL' if row.get('_is_large_b2cl') else 'B2CS'
    
    @staticmethod
    def _is_invalid_amendment_sequence(original_date: Optional[date], revised_date: Optional[date]) -> bool:
        if isinstance(original_date, date) and isinstance(revised_date, date):
            return revised_date < original_date
        return False
    
    def _is_credit_or_debit(self, doc_type: str, supply_text: str) -> bool:
        lowered = f"{doc_type or ''} {supply_text or ''}".lower()
        return any(keyword in lowered for keyword in ('credit', 'debit', 'cn', 'dn', 'refund'))
    
    def _is_invoice_document(self, row: pd.Series) -> bool:
        doc_value = (row.get('_doc_type') or '').lower()
        supply_value = (row.get('_supply_text') or '').lower()
        if 'invoice' in doc_value or 'invoice' in supply_value:
            return True
        if not doc_value:
            return not (row.get('_is_credit_or_debit') or row.get('_is_advance') or row.get('_is_advance_adjustment'))
        return False
    
    def _detect_advance(self, row: pd.Series) -> bool:
        text = f"{row.get('_doc_type', '')} {row.get('_supply_text', '')}".lower()
        return 'advance' in text and 'adjust' not in text
    
    def _detect_advance_adjustment(self, row: pd.Series) -> bool:
        text = f"{row.get('_doc_type', '')} {row.get('_supply_text', '')}".lower()
        return 'advance adjustment' in text or 'advance adj' in text
    
    def _determine_exempt_bucket(self, row: pd.Series) -> Optional[str]:
        category = (row.get('_supply_category') or row.get('_supply_text') or '').lower()
        if not category:
            return None
        if 'non' in category and 'gst' in category:
            return 'non_gst'
        if 'nil' in category:
            return 'nil_rated'
        if 'exempt' in category:
            return 'exempted'
        return None
    
    def _document_bucket(self, row: pd.Series) -> Optional[str]:
        doc_type = (row.get('_document_type') or row.get('_doc_type') or '').lower()
        if not doc_type:
            return None
        if 'invoice' in doc_type:
            return 'Invoices'
        if 'credit' in doc_type or 'cn' in doc_type:
            return 'Credit Notes'
        if 'debit' in doc_type or 'dn' in doc_type:
            return 'Debit Notes'
        return None
    
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
    
    def _resolve_export_type(self, row: pd.Series) -> str:
        supply_text = (row.get('_supply_text') or '').lower()
        if 'wpay' in supply_text or 'with payment' in supply_text:
            return 'WPAY'
        igst_amount = self._to_float(self._get_value(row, 'igst_amount'))
        if igst_amount and igst_amount > 0:
            return 'WPAY'
        return 'WOPAY'
    
    @staticmethod
    def _detect_sez(supply_text: str) -> bool:
        lowered = (supply_text or '').lower()
        return any(keyword in lowered for keyword in ('sez', 'special economic zone', 'deemed export'))
    
    def _determine_invoice_type(self, is_sez: bool, supply_text: str) -> str:
        lowered = (supply_text or '').lower()
        if 'deemed export' in lowered:
            return 'Deemed Export'
        if is_sez:
            if 'without' in lowered and 'payment' in lowered:
                return 'SEZ Without Payment'
            return 'SEZ With Payment'
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
        if 'adv_tax' in simplified:
            if 'adjust' in simplified:
                if 'amend' in simplified:
                    return 'atadja'
                return 'atadj'
            if 'amend' in simplified:
                return 'ata'
            return 'at'
        if 'nil' in simplified and 'exempt' in simplified:
            return 'exemp'
        if simplified.startswith('hsn'):
            if 'b2c' in simplified or 'c2' in simplified:
                return 'hsn_b2c'
            return 'hsn_b2b'
        if simplified.startswith('docs'):
            return 'docs'
        if simplified.startswith('eco'):
            if 'ab2b' in simplified:
                return 'ecoab2b'
            if 'aurp2b' in simplified:
                return 'ecoaurp2b'
            if 'ab2c' in simplified:
                return 'ecoab2c'
            if 'aurp2c' in simplified:
                return 'ecoaurp2c'
            if 'b2c' in simplified:
                return 'ecob2c'
            if 'urp2c' in simplified:
                return 'ecourp2c'
            if 'b2b' in simplified:
                return 'ecob2b'
            if 'urp2b' in simplified:
                return 'ecourp2b'
            if 'amend' in simplified:
                return 'ecoa'
            return 'eco'
        if simplified.startswith('b2bamend') or simplified.startswith('b2ba'):
            return 'b2ba'
        if simplified.startswith('b2cl') and 'amend' in simplified:
            return 'b2cla'
        if simplified.startswith('b2csa') or simplified.startswith('b2csamend'):
            return 'b2csa'
        if simplified.startswith('cdnr'):
            if 'amend' in simplified:
                return 'cdnra'
            return 'cdnr'
        if simplified.startswith('cdnur'):
            if 'amend' in simplified:
                return 'cdnura'
            return 'cdnur'
        if simplified.startswith('export'):
            if 'amend' in simplified:
                return 'expa'
            return 'export'
        if simplified.startswith('exp'):
            if 'amend' in simplified:
                return 'expa'
            return 'export'
        if simplified.startswith('b2b'):
            return 'b2b'
        if simplified.startswith('b2cl'):
            return 'b2cl'
        if simplified.startswith('b2cs'):
            return 'b2cs'
        return None
    
    @staticmethod
    def _round_money(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return round(float(value), 2)
    
    def _is_large_b2cl(self, row: pd.Series) -> bool:
        invoice_value = row.get('_invoice_value')
        if invoice_value is None or not row.get('_is_interstate'):
            return False
        threshold = 100000 if self._is_post_threshold(row.get('_invoice_date')) else 250000
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
