import pandas as pd
from app.gstr1.utils.gst_utils import round_money


class HSNB2CBuilder:
    SHEET_NAME = "hsn(b2c)"

    def build(self, df: pd.DataFrame, headers):
        """
        Build HSN Summary of B2C supplies.

        Rules:
        - Includes B2CL + B2CS (i.e. consumer supplies)
        - Includes only rows WITHOUT valid GSTIN
        - Excludes credit/debit notes
        - Excludes exports
        """

        mask = (
            (~df["_has_valid_gstin"])      # Consumer (unregistered)
            & (~df["_is_credit_or_debit"]) # Exclude CDNs
            & (~df["_is_export"])          # Exclude export supplies
        )

        subset = df[mask].copy()
        if subset.empty:
            return pd.DataFrame(columns=headers)

        # -----------------------------
        # Normalization
        # -----------------------------
        subset["_hsn"] = subset["_hsn"].fillna("").astype(str)
        subset["_description"] = subset["_description"].fillna("").astype(str)
        subset["_uqc"] = subset["_uqc"].fillna("").astype(str)

        subset["_quantity"] = subset["_quantity"].fillna(0).astype(float)
        subset["_total_value"] = subset["_total_value"].fillna(0).astype(float)
        subset["_taxable_value"] = subset["_taxable_value"].fillna(0).astype(float)

        subset["_rate"] = subset["_rate"].fillna(0).astype(float)

        subset["_igst_amount"] = subset["_igst_amount"].fillna(0).astype(float)
        subset["_cgst_amount"] = subset["_cgst_amount"].fillna(0).astype(float)
        subset["_sgst_amount"] = subset["_sgst_amount"].fillna(0).astype(float)
        subset["_cess_amount"] = subset["_cess_amount"].fillna(0).astype(float)

        # -----------------------------
        # Group by HSN SUMMARY RULE
        # -----------------------------
        grouped = subset.groupby(
            ["_hsn", "_description", "_uqc", "_rate"],
            dropna=False
        ).agg({
            "_quantity": "sum",
            "_total_value": "sum",
            "_taxable_value": "sum",
            "_igst_amount": "sum",
            "_cgst_amount": "sum",
            "_sgst_amount": "sum",
            "_cess_amount": "sum",
        }).reset_index()

        h = {name: name for name in headers}
        rows = []

        # -----------------------------
        # Build final rows
        # -----------------------------
        for _, r in grouped.iterrows():
            row = {}

            if "HSN" in h:
                hsn_val = r["_hsn"]
                if hsn_val not in ("", None):
                    row[h["HSN"]] = int(float(hsn_val))
                else:
                    row[h["HSN"]] = ""


            if "Description" in h:
                row[h["Description"]] = r["_description"]

            if "UQC" in h:
                row[h["UQC"]] = "PCS-PIECES"

            if "Total Quantity" in h:
                row[h["Total Quantity"]] = round_money(r["_quantity"])

            if "Total Value" in h:
                row[h["Total Value"]] = round_money(r["_total_value"])

            if "Rate" in h:
                row[h["Rate"]] = round_money(r["_rate"])

            if "Taxable Value" in h:
                row[h["Taxable Value"]] = round_money(r["_taxable_value"])

            if "Integrated Tax Amount" in h:
                row[h["Integrated Tax Amount"]] = round_money(r["_igst_amount"])

            if "Central Tax Amount" in h:
                row[h["Central Tax Amount"]] = round_money(r["_cgst_amount"])

            if "State/UT Tax Amount" in h:
                row[h["State/UT Tax Amount"]] = round_money(r["_sgst_amount"])

            if "Cess Amount" in h:
                row[h["Cess Amount"]] = round_money(r["_cess_amount"])

            rows.append(row)

        result = pd.DataFrame(rows)

        # Ensure all required headers exist
        for col in headers:
            if col not in result.columns:
                result[col] = None

        return result[headers]
