import re
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from app.services.template_service import TemplateService
from app.services.validation_service import ValidationService
from app.utils.logger import setup_logger

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


FIELD_KEYWORDS: Dict[str, List[str]] = {
    'gstin': ['customer gstin', 'customer gstn', 'gstin/uin', 'gstin', 'gstn'],
    'customer_name': ['customer name', 'receiver name', 'trade name'],
    'receiver_name': ['receiver name', 'trade name', 'customer name'],
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
    'rate': ['total tax%','tax rate', 'tax percent', 'rate'],
    'igst_amount': ['igst amount'],
    'cgst_amount': ['cgst amount'],
    'sgst_amount': ['sgst amount'],
    'cess_amount': ['cess amount', 'cess'],
    'ecommerce_gstin': ['e-commerce gstin', 'ecommerce gstin', 'eco gstin'],
    'unique_type': ['unique', 'transaction type'],
    'export_flag': ['export'],
}


class SheetMapper:
    SUPPORTED_SHEETS = ('b2b', 'b2cl', 'b2cs', 'cdnr', 'cdnur', 'export')
    
    def __init__(self, template_service: Optional[TemplateService] = None):
        self.template_service = template_service or TemplateService()
        self.validation_service = ValidationService()
        self.template_structure = self.template_service.load_template_structure()
        self.column_map: Dict[str, Optional[str]] = {}
        
        self.sheet_mapping = self._build_sheet_mapping()
        self.template_field_headers = self._build_template_field_headers()
        
        logger.info("Template sheet mapping: %s", self.sheet_mapping)
    
    def prepare_data_for_template(self, df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        if df.empty:
            return {}
        
        working_df = self._augment_dataframe(df)
        populated: Dict[str, pd.DataFrame] = {}
        
        for builder in (
            self._build_b2b,
            self._build_b2cl,
            self._build_b2cs,
            self._build_cdnr,
            self._build_cdnur,
            self._build_export,
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
            lambda row: self._truncate(self._safe_string(self._get_value(row, 'customer_name')), 100),
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
            lambda row: self._is_large_b2cl(row['_invoice_value'], row['_is_interstate']),
            axis=1
        )
        enriched['_ur_type'] = enriched['_is_large_b2cl'].apply(lambda flag: 'B2CL' if flag else 'B2CS')
        
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
        )
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in df[mask].iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2b', 'gstin', row['_gstin'])
            self._set_field(payload, 'b2b', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'b2b', 'invoice_number', row['_invoice_number'])
            self._set_field(payload, 'b2b', 'invoice_date', row['_invoice_date'])
            self._set_field(payload, 'b2b', 'invoice_value', self._round_money(row['_invoice_value']))
            self._set_field(payload, 'b2b', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'b2b', 'reverse_charge', 'N')
            self._set_field(payload, 'b2b', 'invoice_type', row['_invoice_type'])
            self._set_field(payload, 'b2b', 'ecommerce_gstin', row['_ecommerce_gstin'])
            self._set_field(payload, 'b2b', 'rate', '18')
            self._set_field(payload, 'b2b', 'taxable_value', self._round_money(row['_taxable_value']))
            self._set_field(payload, 'b2b', 'cess_amount', self._round_money(abs(row['_cess_amount']) if row['_cess_amount'] is not None else None))
            if payload:
                rows.append(payload)

        logger.info("B2B Rows", extra={"rows": rows})

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
        )
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        subset = df[mask]
        for _, row in subset.iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'b2cl', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'b2cl', 'invoice_number', row['_invoice_number'])
            self._set_field(payload, 'b2cl', 'invoice_date', row['_invoice_date'])
            self._set_field(payload, 'b2cl', 'invoice_value', self._round_money(abs(row['_invoice_value']) if row['_invoice_value'] is not None else None))
            self._set_field(payload, 'b2cl', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'b2cl', 'rate', row['_rate'])
            self._set_field(payload, 'b2cl', 'taxable_value', self._round_money(row['_taxable_value']))
            self._set_field(payload, 'b2cl', 'ecommerce_gstin', row['_ecommerce_gstin'])
            self._set_field(payload, 'b2cl', 'cess_amount', self._round_money(abs(row['_cess_amount']) if row['_cess_amount'] is not None else None))
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
        )
        subset = df[mask].copy()
        if subset.empty:
            return sheet_name, pd.DataFrame()
        
        subset['_pos_display'] = subset['_pos_code'].apply(self._format_place_of_supply)
        subset['_taxable_amt'] = subset['_taxable_value'].fillna(0)
        subset['_cess_amt'] = subset['_cess_amount'].fillna(0)
        subset['_rate_value'] = subset['_rate']
        
        grouped = (
            subset.groupby(
                ['_type_flag', '_pos_display', '_rate_value', '_ecommerce_gstin'],
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
            self._set_field(payload, 'b2cs', 'rate', row['_rate_value'])
            self._set_field(payload, 'b2cs', 'taxable_value', self._round_money(row['_taxable_amt']))
            self._set_field(payload, 'b2cs', 'ecommerce_gstin', row['_ecommerce_gstin'])
            self._set_field(payload, 'b2cs', 'cess_amount', self._round_money(row['_cess_amt']))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnr(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('cdnr')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_is_credit_or_debit'] & df['_has_valid_gstin']
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in df[mask].iterrows():
            payload: Dict[str, object] = {}
            note_value = self._round_money(abs(row['_note_value']) if row['_note_value'] is not None else None)
            taxable_value = self._round_money(abs(row['_taxable_value']) if row['_taxable_value'] is not None else None)
            self._set_field(payload, 'cdnr', 'gstin', row['_gstin'])
            self._set_field(payload, 'cdnr', 'receiver_name', row['_receiver_name'])
            self._set_field(payload, 'cdnr', 'note_number', row['_note_number'])
            self._set_field(payload, 'cdnr', 'note_date', row['_note_date'])
            self._set_field(payload, 'cdnr', 'note_type', row['_note_type'])
            self._set_field(payload, 'cdnr', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'cdnr', 'reverse_charge', 'N')
            self._set_field(payload, 'cdnr', 'note_value', note_value)
            self._set_field(payload, 'cdnr', 'rate', row['_rate'])
            self._set_field(payload, 'cdnr', 'taxable_value', taxable_value)
            self._set_field(payload, 'cdnr', 'cess_amount', self._round_money(abs(row['_cess_amount']) if row['_cess_amount'] is not None else None))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_cdnur(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('cdnur')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_is_credit_or_debit'] & (~df['_has_valid_gstin'])
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in df[mask].iterrows():
            payload: Dict[str, object] = {}
            note_value = self._round_money(abs(row['_note_value']) if row['_note_value'] is not None else None)
            taxable_value = self._round_money(abs(row['_taxable_value']) if row['_taxable_value'] is not None else None)
            self._set_field(payload, 'cdnur', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'cdnur', 'ur_type', row['_ur_type'])
            self._set_field(payload, 'cdnur', 'note_number', row['_note_number'])
            self._set_field(payload, 'cdnur', 'note_date', row['_note_date'])
            self._set_field(payload, 'cdnur', 'note_type', row['_note_type'])
            self._set_field(payload, 'cdnur', 'place_of_supply', self._format_place_of_supply(row['_pos_code']))
            self._set_field(payload, 'cdnur', 'note_value', note_value)
            self._set_field(payload, 'cdnur', 'rate', row['_rate'])
            self._set_field(payload, 'cdnur', 'taxable_value', taxable_value)
            self._set_field(payload, 'cdnur', 'cess_amount', self._round_money(abs(row['_cess_amount']) if row['_cess_amount'] is not None else None))
            if payload:
                rows.append(payload)
        return sheet_name, self._build_sheet_dataframe(rows, sheet_name)
    
    def _build_export(self, df: pd.DataFrame) -> Tuple[Optional[str], pd.DataFrame]:
        sheet_name = self.sheet_mapping.get('export')
        if not sheet_name:
            return None, pd.DataFrame()
        mask = df['_is_export'] & (~df['_is_credit_or_debit'])
        if not mask.any():
            return sheet_name, pd.DataFrame()
        
        rows: List[Dict[str, object]] = []
        for _, row in df[mask].iterrows():
            payload: Dict[str, object] = {}
            self._set_field(payload, 'export', 'export_type', row['_export_type'])
            self._set_field(payload, 'export', 'customer_name', row['_receiver_name'])
            self._set_field(payload, 'export', 'invoice_number', row['_invoice_number'])
            self._set_field(payload, 'export', 'invoice_date', row['_invoice_date'])
            self._set_field(payload, 'export', 'invoice_value', self._round_money(row['_invoice_value']))
            self._set_field(payload, 'export', 'rate', row['_rate'])
            self._set_field(payload, 'export', 'taxable_value', self._round_money(row['_taxable_value']))
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
        if simplified.startswith('b2b'):
            return 'b2b'
        if simplified.startswith('b2cl'):
            return 'b2cl'
        if simplified.startswith('b2cs'):
            return 'b2cs'
        if simplified.startswith('cdnr'):
            return 'cdnr'
        if simplified.startswith('cdnur'):
            return 'cdnur'
        if simplified.startswith('exp'):
            return 'export'
        if simplified.startswith('export'):
            return 'export'
        return None
    
    @staticmethod
    def _round_money(value: Optional[float]) -> Optional[float]:
        if value is None:
            return None
        return round(float(value), 2)
    
    @staticmethod
    def _is_large_b2cl(invoice_value: Optional[float], is_interstate: bool) -> bool:
        if invoice_value is None or not is_interstate:
            return False
        return abs(invoice_value) > 250000
    
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
