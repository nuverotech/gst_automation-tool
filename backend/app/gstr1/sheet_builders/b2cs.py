import pandas as pd

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import format_place_of_supply


class B2CSBuilder:
    SHEET_NAME = "b2cs"

    def build(self, df: pd.DataFrame, headers):
        mask = (
            (~df["_has_valid_gstin"])
            & (~df["_is_large_b2cl"])
            & (~df["_is_credit_or_debit"])
            & (~df["_is_export"])
        )
        subset = df[mask].copy()
        if subset.empty:
            return pd.DataFrame(columns=headers)

        subset["_pos_display"] = subset["_pos_code"].apply(format_place_of_supply)
        subset["_taxable_amt"] = subset["_taxable_value"].fillna(0)
        subset["_cess_amt"] = subset["_cess_amount"].fillna(0)
        subset["_rate_value"] = subset["_rate"]

        grouped = (
            subset.groupby(
                ["_type_flag", "_pos_display", "_rate_value", "_ecommerce_gstin"],
                dropna=False,
            )[["_taxable_amt", "_cess_amt"]]
            .sum()
            .reset_index()
        )

        h = {name: name for name in headers}
        rows = []
        for _, r in grouped.iterrows():
            row = {}
            if h.get("Type"):
                row[h["Type"]] = r["_type_flag"] or "OE"
            if h.get("Place Of Supply"):
                row[h["Place Of Supply"]] = r["_pos_display"]
            if h.get("Rate"):
                row[h["Rate"]] = round_money(r["_rate_value"]) if r["_rate_value"] is not None else None
            if h.get("Taxable Value"):
                row[h["Taxable Value"]] = round_money(r["_taxable_amt"])
            if h.get("Cess Amount"):
                row[h["Cess Amount"]] = round_money(r["_cess_amt"])
            if h.get("E-Commerce GSTIN"):
                row[h["E-Commerce GSTIN"]] = r["_ecommerce_gstin"] or None
            rows.append(row)

        result = pd.DataFrame(rows)
        for col in headers:
            if col not in result.columns:
                result[col] = None
        return result[headers]
