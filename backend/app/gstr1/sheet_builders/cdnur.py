import pandas as pd

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import format_place_of_supply


class CDNURBuilder:
    SHEET_NAME = "cdnur"

    def build(self, df: pd.DataFrame, headers):
        mask = df["_is_credit_or_debit"] & (~df["_has_valid_gstin"])
        subset = df[mask]
        if subset.empty:
            return pd.DataFrame(columns=headers)

        h = {name: name for name in headers}
        rows = []

        for _, r in subset.iterrows():
            note_val = round_money(abs(r["_note_value"])) if r["_note_value"] is not None else None
            taxable_val = round_money(abs(r["_taxable_value"])) if r["_taxable_value"] is not None else None
            ur_type = "B2CL" if r["_is_large_b2cl"] else "B2CS"

            row = {}
            if h.get("UR Type"):
                row[h["UR Type"]] = ur_type
            if h.get("Note Number"):
                row[h["Note Number"]] = r["_note_number"] or None
            if h.get("Note Date"):
                row[h["Note Date"]] = r["_note_date"] or None
            if h.get("Note Type"):
                row[h["Note Type"]] = r["_note_type"] or None
            if h.get("Place Of Supply"):
                row[h["Place Of Supply"]] = format_place_of_supply(r["_pos_code"])
            if h.get("Note Value"):
                row[h["Note Value"]] = note_val
            if h.get("Rate"):
                row[h["Rate"]] = round_money(r["_rate"]) if r["_rate"] is not None else None
            if h.get("Taxable Value"):
                row[h["Taxable Value"]] = taxable_val
            if h.get("Cess Amount"):
                row[h["Cess Amount"]] = round_money(abs(r["_cess_amount"]))

            rows.append(row)

        result = pd.DataFrame(rows)
        for col in headers:
            if col not in result.columns:
                result[col] = None
        return result[headers]
