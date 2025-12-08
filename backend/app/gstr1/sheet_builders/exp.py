import pandas as pd

from app.gstr1.utils.gst_utils import round_money


class EXPBuilder:
    SHEET_NAME = "exp"

    def build(self, df: pd.DataFrame, headers):
        mask = df["_is_export"] & (~df["_is_credit_or_debit"])
        subset = df[mask]
        if subset.empty:
            return pd.DataFrame(columns=headers)

        h = {name: name for name in headers}
        rows = []

        for _, r in subset.iterrows():
            row = {}
            if h.get("Export Type"):
                row[h["Export Type"]] = r["_export_type"]
            if h.get("Invoice Number"):
                row[h["Invoice Number"]] = r["_invoice_number"] or None
            if h.get("Invoice date"):
                row[h["Invoice date"]] = r["_invoice_date"] or None
            if h.get("Invoice Value"):
                row[h["Invoice Value"]] = round_money(r["_invoice_value"])
            if h.get("Port Code"):
                row[h["Port Code"]] = None
            if h.get("Shipping Bill Number"):
                row[h["Shipping Bill Number"]] = None
            if h.get("Shipping Bill Date"):
                row[h["Shipping Bill Date"]] = None
            if h.get("Rate"):
                row[h["Rate"]] = round_money(r["_rate"]) if r["_rate"] is not None else None
            if h.get("Taxable Value"):
                row[h["Taxable Value"]] = round_money(r["_taxable_value"])

            rows.append(row)

        result = pd.DataFrame(rows)
        for col in headers:
            if col not in result.columns:
                result[col] = None
        return result[headers]
