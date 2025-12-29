import pandas as pd


class DOCSBuilder:
    SHEET_NAME = "docs"

    def build(self, df: pd.DataFrame, headers):
        rows = []
        h = {c: c for c in headers}

        if "_is_cancelled" not in df.columns:
            df["_is_cancelled"] = False

        # --------------------------------------------------
        # 1. Invoices for outward supply
        # --------------------------------------------------
        inv_df = df[~df["_is_credit_or_debit"]]

        if not inv_df.empty:
            nums = inv_df["_invoice_number"].dropna().astype(str)

            rows.append({
                h["Nature of Document"]: "Invoices for outward supply",
                h["Sr. No. From"]: nums.min(),
                h["Sr. No. To"]: nums.max(),
                h["Total Number"]: nums.nunique(),  # ✅ UNIQUE
                h["Cancelled"]: inv_df.loc[
                    inv_df["_is_cancelled"], "_invoice_number"
                ].nunique(),  # ✅ UNIQUE
            })

        # --------------------------------------------------
        # 2. Credit Notes
        # --------------------------------------------------
        cn_df = df[df["_is_credit_or_debit"] & (df["_note_type"] == "C")]

        if not cn_df.empty:
            nums = cn_df["_note_number"].dropna().astype(str)

            rows.append({
                h["Nature of Document"]: "Credit Note",
                h["Sr. No. From"]: nums.min(),
                h["Sr. No. To"]: nums.max(),
                h["Total Number"]: nums.nunique(),  # ✅ UNIQUE
                h["Cancelled"]: cn_df.loc[
                    cn_df["_is_cancelled"], "_note_number"
                ].nunique(),  # ✅ UNIQUE
            })

        # --------------------------------------------------
        # 3. Debit Notes
        # --------------------------------------------------
        dn_df = df[df["_is_credit_or_debit"] & (df["_note_type"] == "D")]

        if not dn_df.empty:
            nums = dn_df["_note_number"].dropna().astype(str)

            rows.append({
                h["Nature of Document"]: "Debit Note",
                h["Sr. No. From"]: nums.min(),
                h["Sr. No. To"]: nums.max(),
                h["Total Number"]: nums.nunique(),  # ✅ UNIQUE
                h["Cancelled"]: dn_df.loc[
                    dn_df["_is_cancelled"], "_note_number"
                ].nunique(),  # ✅ UNIQUE
            })

        result = pd.DataFrame(rows)

        for col in headers:
            if col not in result.columns:
                result[col] = None

        return result[headers]
