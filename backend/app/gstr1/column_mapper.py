from typing import Dict, List, Optional, Tuple

import pandas as pd
import re
import datetime

from app.gstr1.utils.gst_utils import safe_string, GSTIN_REGEX


def _normalize(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value).lower())


DATA_COLUMN_KEYWORDS: Dict[str, List[str]] = {
    "gstin": ["customer gstin", "customer gstn", "recipient gstin", "gstin", "gstn"],
    "customer_name": ["customer name", "receiver name", "trade name", "buyer name"],
    "invoice_number": ["invoice number", "invoice no", "invoice id", "order id", "document number"],
    "invoice_date": ["invoice date", "date of invoice", "invoice dt", "document date"],
    "invoice_value": ["invoice value", "invoice amount", "value of invoice"],
    "tax_total": ["tax total", "total tax", "tax amount", "tax total amount"],
    "gross_amount": ["gross sales", "gross sales after discount", "gross value"],
    "mrp_value": ["mrp total", "mrp value"],
    "taxable_value": ["taxable value", "net sales", "net sales amount", "taxable amount"],
    "place_of_supply": ["place of supply", "pos", "customer state"],
    "source_of_supply": ["source of supply", "source state", "state of supply"],
    "sales_channel": ["sales channel", "channel"],
    "doc_type": ["doc type", "document type"],
    "supply_type": ["supply type", "transaction type", "unique", "unique type"],
    "note_number": ["cn number", "dn number", "credit note number", "debit note number", "note number"],
    "note_date": ["note date", "cn date", "dn date", "credit note date", "debit note date"],
    "note_value": [
        "note value",
        "credit amount",
        "debit amount",
        "dr./ cr. value",
        "dr./ cr. note value",
        "gross sales after discount",
    ],
    "igst_rate": ["igst tax%", "igst%", "igst rate"],
    "cgst_rate": ["cgst tax%", "cgst%", "cgst rate"],
    "sgst_rate": ["sgst tax%", "sgst%", "sgst rate"],
    "rate": ["total tax%", "tax rate", "tax percent", "rate"],
    "igst_amount": ["igst amount"],
    "cgst_amount": ["cgst amount"],
    "sgst_amount": ["sgst amount"],
    "cess_amount": ["cess amount", "cess"],
    "ecommerce_gstin": ["e-commerce gstin", "ecommerce gstin", "eco gstin"],
    "unique_type": ["unique", "transaction type"],
    "export_flag": ["export"],
    "export_type": ["export type", "exp type", "type of export", "wpay", "wopay"],
    "port_code": ["port code", "shipping port", "port"],
    "shipping_bill_number": ["shipping bill number", "sb number", "shipping bill no"],
    "shipping_bill_date": ["shipping bill date", "sb date"],
    "hsn": ["hsn", "hsn code"],
    "description": ["description", "product name", "item name"],
    "uqc": ["uqc", "unit", "unit quantity", "quantity unit"],
    "quantity": ["quantity", "item quantity", "qty"],
    "total_value": ["total value", "value", "invoice line total"],
    "source_of_supply": ["source of supply", "source state", "state of supply"],

}


class ColumnMapper:
    """
    Maps input DataFrame columns to semantic field keys based on
    header keywords and content-based heuristics.
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.column_map: Dict[str, Optional[str]] = {}

    @staticmethod
    def _match_column(columns, keywords: List[str]) -> Optional[str]:
        normalized_columns = [(col, _normalize(col)) for col in columns]
        best_match: Optional[str] = None
        best_score: Optional[Tuple[int, int, int]] = None

        for priority, keyword in enumerate(keywords):
            normalized_keyword = _normalize(keyword)
            if not normalized_keyword:
                continue
            for idx, (original, label) in enumerate(normalized_columns):
                match_level = None
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

    def _detect_gstin_column(self) -> Optional[str]:
        for col in self.df.columns:
            series = self.df[col].dropna().astype(str)
            if series.empty:
                continue
            sample = series.head(30)
            matches = sample.str.match(GSTIN_REGEX)
            if matches.sum() >= max(3, int(0.6 * len(sample))):
                return col
        return None



    def _detect_date_column(self) -> Optional[str]:
        """
        Detect which column is a date column.
        Avoid pandas format inference (causes warnings).
        Parse each value individually using safe patterns.
        """
        DATE_FORMATS = [
            "%d-%m-%Y",
            "%d/%m/%Y",
            "%Y-%m-%d",
            "%d-%b-%Y",
            "%d-%B-%Y",
            "%d.%m.%Y",
        ]

        def looks_like_date(val):
            """Return True if val looks like a valid date."""
            if val is None:
                return False

            val = str(val).strip()
            if not val:
                return False

            # Excel serial number date (ex: 45123 → valid date)
            if val.isdigit() and len(val) <= 5:
                try:
                    base = datetime.datetime(1899, 12, 30)
                    serial = int(val)
                    if serial > 20000:  # serials below this are bogus
                        _ = base + datetime.timedelta(days=serial)
                        return True
                except:
                    pass

            # Try all known formats
            for fmt in DATE_FORMATS:
                try:
                    datetime.datetime.strptime(val, fmt)
                    return True
                except:
                    continue

            return False

        # ---------------- detect column ------------------
        for col in self.df.columns:
            series = self.df[col].dropna()
            if series.empty:
                continue

            sample = series.astype(str).head(20)
            count = sum(looks_like_date(v) for v in sample)
            ratio = count / len(sample)

            if ratio >= 0.70:
                return col

        return None


    def build_map(self) -> Dict[str, Optional[str]]:
        columns = list(self.df.columns)
        mapping: Dict[str, Optional[str]] = {}

        # Header-based mapping
        for field, keywords in DATA_COLUMN_KEYWORDS.items():
            mapping[field] = self._match_column(columns, keywords)

        # Content-based fallbacks
        if mapping.get("gstin") is None:
            mapping["gstin"] = self._detect_gstin_column()

        if mapping.get("invoice_date") is None:
            mapping["invoice_date"] = self._detect_date_column()

        self.column_map = mapping
        return mapping

    def get_value(self, row: pd.Series, field_key: str):
        col = self.column_map.get(field_key)
        if col and col in row:
            return row[col]
        return None

    def _map_doc_type(self, row):
        """
        Convert raw 'Doc Type' column → internal flags
        """

        doc = str(row.get("Doc Type", "")).strip().lower()

        # Base flags
        row["_is_credit_or_debit"] = False
        row["_is_ur_note"] = False
        row["_note_type"] = None

        # ---------------------------
        # CREDIT / DEBIT NOTES (REGISTERED → CDNR)
        # ---------------------------
        if doc in ["credit note", "debit note", "cn", "dn"]:
            row["_is_credit_or_debit"] = True
            row["_note_type"] = "C" if "credit" in doc else "D"
            return row

        # ---------------------------
        # CREDIT / DEBIT NOTES (UNREGISTERED → CDNUR)
        # ---------------------------
        if doc in ["ur credit note", "ur debit note", "ur cn", "ur dn"]:
            row["_is_credit_or_debit"] = True
            row["_is_ur_note"] = True
            row["_note_type"] = "C" if "credit" in doc else "D"
            return row

        # ---------------------------
        # INVOICES
        # ---------------------------
        row["_note_type"] = None
        return row
