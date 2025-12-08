from typing import Optional

import pandas as pd

from app.gstr1.column_mapper import ColumnMapper
from app.gstr1.utils.gst_utils import safe_string, to_float, round_money, is_valid_gstin
from app.gstr1.utils.date_utils import parse_excel_or_date
from app.gstr1.utils.state_codes import state_code_from_value


class GSTTransformer:
    """
    Enriches the raw user data with normalized fields used by all sheet builders.
    Fast mode: best-effort derivation, ambiguous things left blank.
    """

    def enrich(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        mapper = ColumnMapper(df)
        mapper.build_map()

        working = df.copy()

        # Basic identifiers
        working["_gstin"] = working.apply(
            lambda r: self._clean_gstin(mapper.get_value(r, "gstin")), axis=1
        )
        working["_has_valid_gstin"] = working["_gstin"].apply(is_valid_gstin)

        working["_invoice_number"] = working.apply(
            lambda r: safe_string(mapper.get_value(r, "invoice_number")), axis=1
        )
        working["_invoice_date"] = working.apply(
            lambda r: parse_excel_or_date(mapper.get_value(r, "invoice_date")), axis=1
        )

        # Tax amounts
        working["_tax_total"] = working.apply(
            lambda r: self._extract_tax_total(r, mapper), axis=1
        )
        working["_invoice_value"] = working.apply(
            lambda r: self._resolve_invoice_value(r, mapper), axis=1
        )
        working["_taxable_value"] = working.apply(
            lambda r: self._resolve_taxable_value(r, mapper, r["_invoice_value"]),
            axis=1,
        )

        working["_rate"] = working.apply(
            lambda r: self._resolve_rate(r, mapper), axis=1
        )
        working["_cess_amount"] = working.apply(
            lambda r: self._resolve_cess_amount(r, mapper), axis=1
        )

        # Names and e-commerce
        working["_receiver_name"] = working.apply(
            lambda r: self._truncate(
                safe_string(mapper.get_value(r, "customer_name")), 100
            ),
            axis=1,
        )
        working["_ecommerce_gstin"] = working.apply(
            lambda r: self._clean_gstin(mapper.get_value(r, "ecommerce_gstin")), axis=1
        )
        working["_type_flag"] = working["_ecommerce_gstin"].apply(
            lambda v: "E" if v else "OE"
        )

        # Supply text / SEZ / invoice type
        working["_supply_text"] = working.apply(
            lambda r: safe_string(
                mapper.get_value(r, "supply_type")
                or mapper.get_value(r, "unique_type")
            ),
            axis=1,
        )
        working["_is_sez"] = working["_supply_text"].apply(self._detect_sez)
        working["_invoice_type"] = working.apply(
            lambda r: self._determine_invoice_type(r["_is_sez"], r["_supply_text"]),
            axis=1,
        )

        # Place of supply / interstate
        working["_pos_code"] = working.apply(
            lambda r: state_code_from_value(mapper.get_value(r, "place_of_supply")),
            axis=1,
        )
        working["_source_state_code"] = working.apply(
            lambda r: state_code_from_value(mapper.get_value(r, "source_of_supply")),
            axis=1,
        )
        working["_is_interstate"] = working.apply(
            lambda r: bool(
                r["_pos_code"]
                and r["_source_state_code"]
                and r["_pos_code"] != r["_source_state_code"]
            ),
            axis=1,
        )
        working["_is_large_b2cl"] = working.apply(
            lambda r: self._is_large_b2cl(r["_invoice_value"], r["_is_interstate"]),
            axis=1,
        )

        # Notes / credit / debit
        working["_doc_type"] = working.apply(
            lambda r: safe_string(
                mapper.get_value(r, "doc_type")
                or mapper.get_value(r, "unique_type")
            ),
            axis=1,
        )
        working["_note_number"] = working.apply(
            lambda r: safe_string(mapper.get_value(r, "note_number"))
            or r["_invoice_number"],
            axis=1,
        )
        working["_note_date"] = working.apply(
            lambda r: parse_excel_or_date(mapper.get_value(r, "note_date"))
            or r["_invoice_date"],
            axis=1,
        )
        working["_note_value"] = working.apply(
            lambda r: self._resolve_note_value(r, mapper), axis=1
        )
        working["_note_type"] = working.apply(
            lambda r: self._determine_note_type(
                r["_doc_type"], r["_supply_text"], r["_note_value"]
            ),
            axis=1,
        )
        working["_is_credit_or_debit"] = working.apply(
            lambda r: self._is_credit_or_debit(r["_doc_type"], r["_supply_text"])
            or bool(r["_note_type"]),
            axis=1,
        )

        # Export
        working["_is_export"] = working.apply(
            lambda r: self._detect_export(r, mapper), axis=1
        )
        working["_export_type"] = working.apply(
            lambda r: self._resolve_export_type(r), axis=1
        )

        return working

    # ---------------- helpers ----------------

    @staticmethod
    def _clean_gstin(value) -> str:
        s = safe_string(value).upper()
        if len(s) != 15:
            return ""
        return s

    @staticmethod
    def _truncate(value: str, max_len: int) -> str:
        if not value:
            return ""
        if len(value) <= max_len:
            return value
        return value[:max_len]

    @staticmethod
    def _is_large_b2cl(invoice_value: Optional[float], is_interstate: bool) -> bool:
        if invoice_value is None or not is_interstate:
            return False
        return abs(invoice_value) > 250000

    # amounts

    def _extract_tax_total(self, row: pd.Series, mapper: ColumnMapper) -> Optional[float]:
        explicit = to_float(mapper.get_value(row, "tax_total"))
        if explicit is not None:
            return explicit
        igst = to_float(mapper.get_value(row, "igst_amount"))
        cgst = to_float(mapper.get_value(row, "cgst_amount"))
        sgst = to_float(mapper.get_value(row, "sgst_amount"))
        vals = [v for v in (igst, cgst, sgst) if v is not None]
        if not vals:
            return None
        return sum(vals)

    def _resolve_invoice_value(self, row: pd.Series, mapper: ColumnMapper) -> Optional[float]:
        invoice_value = to_float(mapper.get_value(row, "invoice_value"))
        if invoice_value is not None:
            return invoice_value

        for key in ("gross_amount", "mrp_value"):
            v = to_float(mapper.get_value(row, key))
            if v is not None:
                return v

        taxable = to_float(mapper.get_value(row, "taxable_value"))
        tax_total = row.get("_tax_total")
        if taxable is not None and tax_total is not None:
            return taxable + tax_total
        if taxable is not None:
            return taxable
        return None

    def _resolve_taxable_value(
        self, row: pd.Series, mapper: ColumnMapper, invoice_value: Optional[float]
    ) -> Optional[float]:
        taxable = to_float(mapper.get_value(row, "taxable_value"))
        if taxable is not None:
            return taxable
        if invoice_value is None:
            return None
        tax_total = row.get("_tax_total")
        if tax_total is None:
            return invoice_value
        return invoice_value - tax_total

    def _resolve_rate(self, row: pd.Series, mapper: ColumnMapper) -> Optional[float]:
        igst_rate = to_float(mapper.get_value(row, "igst_rate"))
        if igst_rate:
            return igst_rate
        cgst_rate = to_float(mapper.get_value(row, "cgst_rate")) or 0
        sgst_rate = to_float(mapper.get_value(row, "sgst_rate")) or 0
        if cgst_rate or sgst_rate:
            return cgst_rate + sgst_rate
        generic = to_float(mapper.get_value(row, "rate"))
        if generic:
            return generic
        taxable = to_float(mapper.get_value(row, "taxable_value"))
        tax_total = row.get("_tax_total")
        if taxable and tax_total:
            try:
                return round((tax_total / taxable) * 100, 2)
            except ZeroDivisionError:
                return None
        return None

    def _resolve_cess_amount(self, row: pd.Series, mapper: ColumnMapper) -> float:
        v = to_float(mapper.get_value(row, "cess_amount"))
        if v is not None:
            return v
        return 0.0

    def _resolve_note_value(self, row: pd.Series, mapper: ColumnMapper) -> Optional[float]:
        note_val = to_float(mapper.get_value(row, "note_value"))
        if note_val is not None:
            return note_val
        taxable = row.get("_taxable_value")
        tax_total = row.get("_tax_total")
        taxable_abs = abs(taxable) if taxable is not None else 0
        tax_total_abs = abs(tax_total) if tax_total is not None else 0
        if taxable_abs or tax_total_abs:
            return taxable_abs + tax_total_abs
        if row["_invoice_value"] is not None:
            return abs(row["_invoice_value"])
        return None

    @staticmethod
    def _determine_note_type(doc_type: str, supply_text: str, note_value: Optional[float]) -> Optional[str]:
        lowered = f"{doc_type or ''} {supply_text or ''}".lower()
        if "credit" in lowered or "cn" in lowered:
            return "C"
        if "debit" in lowered or "dn" in lowered:
            return "D"
        return None

    @staticmethod
    def _is_credit_or_debit(doc_type: str, supply_text: str) -> bool:
        lowered = f"{doc_type or ''} {supply_text or ''}".lower()
        return any(k in lowered for k in ("credit", "debit", "cn", "dn"))

    @staticmethod
    def _detect_sez(supply_text: str) -> bool:
        lowered = (supply_text or "").lower()
        return any(k in lowered for k in ("sez", "special economic zone", "deemed export"))

    @staticmethod
    def _determine_invoice_type(is_sez: bool, supply_text: str) -> str:
        if is_sez:
            lowered = (supply_text or "").lower()
            if "without" in lowered and "payment" in lowered:
                return "SEZ supplies without payment"
            return "SEZ supplies with payment"
        return "Regular"

    def _detect_export(self, row: pd.Series, mapper: ColumnMapper) -> bool:
        if row.get("_is_credit_or_debit"):
            return False
        candidates = [
            safe_string(mapper.get_value(row, "sales_channel")),
            row.get("_doc_type", ""),
            safe_string(mapper.get_value(row, "source_of_supply")),
            safe_string(mapper.get_value(row, "unique_type")),
            row.get("_supply_text", ""),
        ]
        for value in candidates:
            lowered = (value or "").lower()
            if lowered.startswith("exp "):
                return True
            if "export" in lowered:
                return True
        return False

    @staticmethod
    def _resolve_export_type(row: pd.Series) -> str:
        text = (row.get("_supply_text") or "").lower()
        if "with payment" in text or "wpay" in text:
            return "WPAY"
        return "WOPAY"
