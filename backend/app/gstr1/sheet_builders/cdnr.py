import pandas as pd

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import format_place_of_supply


class CDNRBuilder:
    SHEET_NAME = "cdnr"

    def build(self, df: pd.DataFrame, headers):
        mask = df["_is_credit_or_debit"] & df["_has_valid_gstin"]
        subset = df[mask]
        if subset.empty:
            return pd.DataFrame(columns=headers)

        h = {name: name for name in headers}
        rows = []

        for _, r in subset.iterrows():
            note_val = round_money(abs(r["_note_value"])) if r["_note_value"] is not None else None
            taxable_val = round_money(abs(r["_taxable_value"])) if r["_taxable_value"] is not None else None

            row = {}
            if h.get("GSTIN/UIN of Recipient"):
                row[h["GSTIN/UIN of Recipient"]] = r["_gstin"] or None
            if h.get("Receiver Name"):
                row[h["Receiver Name"]] = r["_receiver_name"] or None
            if h.get("Note Number"):
                row[h["Note Number"]] = r["_note_number"] or None
            if h.get("Note Date"):
                row[h["Note Date"]] = r["_note_date"] or None
            if h.get("Note Type"):
                row[h["Note Type"]] = r["_note_type"] or None
            if h.get("Place Of Supply"):
                row[h["Place Of Supply"]] = format_place_of_supply(r["_pos_code"])
            if h.get("Reverse Charge"):
                row[h["Reverse Charge"]] = "N"
            if h.get("Note Supply Type"):
                # Fast mode: not inferring; leave blank
                row[h["Note Supply Type"]] = None
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
