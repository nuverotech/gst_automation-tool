import re
import pandas as pd
from app.gstr1.utils.gst_utils import round_money, is_valid_gstin


class ECOBuilder:
    SHEET_NAME = "eco"

    VALID_NATURES = {
        "Liable to collect tax u/s 52(TCS)",
        "Liable to pay tax u/s 9(5)",
    }

    GSTIN_REGEX = re.compile(
        r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z][0-9A-Z][0-9A-Z]$"
    )


    # -------------------------
    # VALIDATIONS
    # -------------------------

    def validate_nature(self, v):
        if not v:
            return False, "Nature of Supply required"
        if v not in self.VALID_NATURES:
            return False, "Invalid Nature of Supply"
        return True, None

    def validate_gstin(self, g):
        if not g:
            return False, "E-Commerce GSTIN required"
        if not self.GSTIN_REGEX.match(str(g)):
            return False, "Invalid E-Commerce GSTIN"
        return True, None

    def validate_amount(self, v, field):
        try:
            if float(v) < 0:
                return False, f"{field} < 0"
            return True, None
        except:
            return False, f"Invalid {field}"

    # -------------------------
    # MAIN BUILD
    # -------------------------

    def build(self, df: pd.DataFrame, headers):

        # --------------------------------------------------
        # ECO MASK (STRICT & CORRECT)
        # --------------------------------------------------
        mask = (
            df["_ecommerce_gstin"].notna()
            & (df["_ecommerce_gstin"] != "")
            & (~df["_is_credit_or_debit"])
            & (~df["_is_export"])
        )

        subset = df[mask].copy()

        if subset.empty:
            return pd.DataFrame(columns=headers)

        # --------------------------------------------------
        # Derive Nature of Supply
        # (Default = Section 52 TCS unless client gives flag)
        # --------------------------------------------------
        subset["_nature_of_supply"] = "Liable to collect tax u/s 52(TCS)"

        # --------------------------------------------------
        # AGGREGATION (MANDATORY FOR ECO)
        # --------------------------------------------------
        grouped = (
            subset.groupby(
                ["_nature_of_supply", "_ecommerce_gstin", "_receiver_name"],
                dropna=False,
            )
            .agg(
                {
                    "_invoice_value": "sum",
                    "_igst_amount": "sum",
                    "_cgst_amount": "sum",
                    "_sgst_amount": "sum",
                    "_cess_amount": "sum",
                }
            )
            .reset_index()
        )

        h = {c: c for c in headers}
        rows = []
        errors = []

        # --------------------------------------------------
        # BUILD FINAL ROWS
        # --------------------------------------------------
        for idx, r in grouped.iterrows():

            row = {}

            # Nature of Supply
            ok, err = self.validate_nature(r["_nature_of_supply"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Nature of Supply"]] = r["_nature_of_supply"]

            # E-Commerce GSTIN
            ok, err = self.validate_gstin(r["_ecommerce_gstin"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["GSTIN of E-Commerce Operator"]] = r["_ecommerce_gstin"]

            # Operator Name
            row[h["E-Commerce Operator Name"]] = r["_receiver_name"] or None

            # Net Value of Supplies
            ok, err = self.validate_amount(
                r["_invoice_value"], "Net value of supplies"
            )
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Net value of supplies"]] = round_money(r["_invoice_value"])

            # Integrated Tax
            ok, err = self.validate_amount(
                r["_igst_amount"], "Integrated tax"
            )
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Integrated tax"]] = round_money(r["_igst_amount"])

            # Central Tax
            ok, err = self.validate_amount(
                r["_cgst_amount"], "Central tax"
            )
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Central tax"]] = round_money(r["_cgst_amount"])

            # State / UT Tax
            ok, err = self.validate_amount(
                r["_sgst_amount"], "State/UT tax"
            )
            if not ok:
                errors.append((idx, err))
                continue
            row[h["State/UT tax"]] = round_money(r["_sgst_amount"])

            # Cess
            ok, err = self.validate_amount(
                r["_cess_amount"], "Cess"
            )
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Cess"]] = round_money(r["_cess_amount"])

            rows.append(row)

        # --------------------------------------------------
        # PRINT VALIDATION ERRORS (IF ANY)
        # --------------------------------------------------
        if errors:
            print("⚠️ ECO Validation Errors:")
            for e in errors:
                print(f"  Row {e[0]} → {e[1]}")

        result = pd.DataFrame(rows)

        # Ensure column order
        for col in headers:
            if col not in result.columns:
                result[col] = None

        return result[headers]
