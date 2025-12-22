import re
import pandas as pd
from datetime import datetime

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import normalize_pos_code, format_place_of_supply


class EXPBuilder:
    SHEET_NAME = "exp"

    NOTE_NUM_REGEX = re.compile(r"^[A-Za-z0-9/-]+$")
    VALID_GST_RATES = {0, 0.1, 0.25, 1, 1.5, 3, 5, 12, 18, 28}
    PORT_CODE_REGEX = re.compile(r"^[A-Z0-9]{6}$")

    # ---------------------------
    # VALIDATIONS
    # ---------------------------

    def validate_invoice_no(self, inv):
        if not inv:
            return False, "Invoice number required"
        inv = str(inv)
        if len(inv) > 16:
            return False, "Invoice number > 16 characters"
        if not self.NOTE_NUM_REGEX.match(inv):
            return False, "Invalid characters in invoice number"
        return True, None

    def validate_invoice_date(self, dt):
        if not dt:
            return False, "Invoice date required"
        try:
            if isinstance(dt, str):
                datetime.strptime(dt[:11], "%d-%b-%Y")
            return True, None
        except:
            return False, "Invalid invoice date format"

    def validate_export_type(self, t):
        if not t:
            return False, "Export type required"
        t = str(t).strip().upper()
        if t not in ("WPAY", "WOPAY"):
            return False, "Export Type must be WPAY or WOPAY"
        return True, None

    def validate_rate(self, r):
        try:
            if float(r) in self.VALID_GST_RATES:
                return True, None
        except:
            pass
        return False, f"Invalid Export GST rate {r}"

    def validate_taxable_value(self, val):
        try:
            if float(val) < 0:
                return False, "Taxable value < 0"
            return True, None
        except:
            return False, "Invalid taxable value"

    def validate_cess(self, val):
        try:
            if float(val) < 0:
                return False, "Cess < 0"
            return True, None
        except:
            return False, "Invalid cess amount"

    def validate_port_code(self, code):
        if not code:
            return True, None  # Optional
        code = str(code).strip().upper()
        if not self.PORT_CODE_REGEX.match(code):
            return False, "Invalid Port Code (must be 6 alphanumeric chars)"
        return True, None

    # ---------------------------
    # MAIN BUILD
    # ---------------------------
    def build(self, df: pd.DataFrame, headers):

        mask = df["_is_export"] & (~df["_is_credit_or_debit"])
        subset = df[mask]

        if subset.empty:
            return pd.DataFrame(columns=headers)

        h = {name: name for name in headers}
        rows = []
        errors = []

        for idx, r in subset.iterrows():
            row = {}

            # ----------------------------
            # Export Type
            # ----------------------------
            ok, err = self.validate_export_type(r["_export_type"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Export Type"]] = r["_export_type"].upper()

            # ----------------------------
            # Invoice Number
            # ----------------------------
            ok, err = self.validate_invoice_no(r["_invoice_number"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Invoice Number"]] = r["_invoice_number"]

            # ----------------------------
            # Invoice Date
            # ----------------------------
            ok, err = self.validate_invoice_date(r["_invoice_date"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Invoice date"]] = r["_invoice_date"]

            # ----------------------------
            # Invoice Value
            # ----------------------------
            row[h["Invoice Value"]] = round_money(r["_invoice_value"])

            # ----------------------------
            # Port Code
            # ----------------------------
            ok, err = self.validate_port_code(r.get("_port_code"))
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Port Code"]] = r.get("_port_code") or None

            # ----------------------------
            # Shipping Bill fields (optional)
            # ----------------------------
            row[h["Shipping Bill Number"]] = r.get("_shipping_bill_number") or None
            row[h["Shipping Bill Date"]] = r.get("_shipping_bill_date") or None

            # ----------------------------
            # GST Rate
            # ----------------------------
            ok, err = self.validate_rate(r["_rate"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Rate"]] = round_money(r["_rate"])

            # ----------------------------
            # Taxable Value
            # ----------------------------
            ok, err = self.validate_taxable_value(r["_taxable_value"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Taxable Value"]] = round_money(r["_taxable_value"])

            # ----------------------------
            # POS must always be 96 - Other Territory
            # ----------------------------
            if "Place Of Supply" in h:
                row[h["Place Of Supply"]] = "96-Other Territory"

            row[h["Cess Amount"]] = round_money(abs(r["_cess_amount"]))

            rows.append(row)

        if errors:
            print("⚠️ EXP Validation Errors:")
            for e in errors:
                print(f"  Row {e[0]} → {e[1]}")

        df_final = pd.DataFrame(rows)

        # Ensure missing columns exist
        for col in headers:
            if col not in df_final.columns:
                df_final[col] = None

        return df_final[headers]
