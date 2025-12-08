import pandas as pd

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import format_place_of_supply


class B2CLBuilder:
    SHEET_NAME = "b2cl"

    def build(self, df: pd.DataFrame, headers):
        mask = (
            (~df["_has_valid_gstin"])
            & df["_is_large_b2cl"]
            & (~df["_is_credit_or_debit"])
            & (~df["_is_export"])
        )
        subset = df[mask]
        if subset.empty:
            return pd.DataFrame(columns=headers)

        h = {name: name for name in headers}
        rows = []
        for _, r in subset.iterrows():
            row = {}
            if h.get("Invoice Number"):
                row[h["Invoice Number"]] = r["_invoice_number"] or None
            if h.get("Invoice date"):
                row[h["Invoice date"]] = r["_invoice_date"] or None
            if h.get("Invoice Value"):
                row[h["Invoice Value"]] = round_money(r["_invoice_value"])
            if h.get("Place Of Supply"):
                row[h["Place Of Supply"]] = format_place_of_supply(r["_pos_code"])
            if h.get("Rate"):
                row[h["Rate"]] = round_money(r["_rate"]) if r["_rate"] is not None else None
            if h.get("Taxable Value"):
                row[h["Taxable Value"]] = round_money(r["_taxable_value"])
            if h.get("Cess Amount"):
                row[h["Cess Amount"]] = round_money(abs(r["_cess_amount"]))
            if h.get("E-Commerce GSTIN"):
                row[h["E-Commerce GSTIN"]] = r["_ecommerce_gstin"] or None
            rows.append(row)

        result = pd.DataFrame(rows)
        for col in headers:
            if col not in result.columns:
                result[col] = None
        return result[headers]
