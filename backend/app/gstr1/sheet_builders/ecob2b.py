import pandas as pd
from datetime import datetime

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import normalize_pos_code, state_code_from_value, format_place_of_supply


class ECOB2BBuilder:
    SHEET_NAME = "ecob2b"

    # 🔒 TEMP HARD-CODE (as per your instruction)
    SUPPLIER_GSTIN = "29ABCDE1234F1Z5"
    SUPPLIER_NAME = "Your Company Name"

    # -------------------------
    # VALIDATIONS
    # -------------------------
    def validate_gstin(self, g):
        if not g or len(str(g)) != 15:
            return False, "Invalid GSTIN"
        return True, None

    def validate_invoice_no(self, inv):
        if not inv or len(str(inv)) > 16:
            return False, "Invalid document number"
        return True, None

    def validate_invoice_date(self, d):
        if not d:
            return False, "Document date required"
        try:
            if isinstance(d, str):
                datetime.strptime(d[:11], "%d-%b-%Y")
            return True, None
        except:
            return False, "Invalid document date"

    def validate_number(self, v, name):
        try:
            if float(v) < 0:
                return False, f"{name} < 0"
            return True, None
        except:
            return False, f"Invalid {name}"

    # -------------------------
    # MAIN BUILD
    # -------------------------
    def build(self, df: pd.DataFrame, headers):

        # --------------------------------------------------
        # ECO B2B FILTER
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

        rows = []
        h = {c: c for c in headers}
        errors = []

        print("Length of subset for ECOB2B:", subset.shape[0])

        for idx, r in subset.iterrows():

            row = {}

            # ---------------- Supplier (FILER) ----------------
            row[h["Supplier GSTIN/UIN"]] = self.SUPPLIER_GSTIN
            row[h["Supplier Name"]] = self.SUPPLIER_NAME

            # ---------------- Recipient (ECO) ----------------
            ok, err = self.validate_gstin(r["_ecommerce_gstin"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Recipient GSTIN/UIN"]] = r["_ecommerce_gstin"]
            row[h["Recipient Name"]] = r.get("_receiver_name") or ""

            # ---------------- Document ----------------
            ok, err = self.validate_invoice_no(r["_invoice_number"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Document Number"]] = r["_invoice_number"]

            ok, err = self.validate_invoice_date(r["_invoice_date"])
            if not ok:
                errors.append((idx, err))
                continue
            if isinstance(r["_invoice_date"], str):
                row[h["Document Date"]] = datetime.strptime(
                    r["_invoice_date"][:11], "%d-%b-%Y"
                )
            else:
                row[h["Document Date"]] = r["_invoice_date"]


            # ---------------- Values ----------------
            ok, err = self.validate_number(r["_invoice_value"], "Invoice value")
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Value of supplies made"]] = round_money(r["_invoice_value"])

            # ---------------- POS ----------------
            normalized = normalize_pos_code(r["_pos_code"])
            pos_state = state_code_from_value(normalized)
            row[h["Place Of Supply"]] = format_place_of_supply(pos_state)

            # ---------------- Document Type ----------------
            row[h["Document type"]] = "Regular"

            # ---------------- Rate ----------------
            row[h["Rate"]] = round_money(r["_rate"])

            # ---------------- Taxable ----------------
            ok, err = self.validate_number(r["_taxable_value"], "Taxable value")
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Taxable Value"]] = round_money(r["_taxable_value"])

            # ---------------- Cess ----------------
            row[h["Cess Amount"]] = round_money(r["_cess_amount"])

            rows.append(row)

        if errors:
            print("⚠️ ECOB2B Validation Errors:")
            for e in errors:
                print(f"  Row {e[0]} → {e[1]}")

        result = pd.DataFrame(rows)

        for col in headers:
            if col not in result.columns:
                result[col] = None

        return result[headers]
