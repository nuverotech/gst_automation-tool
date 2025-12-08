import re
import pandas as pd
from datetime import datetime

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import format_place_of_supply


class B2BBuilder:
    """
    B2B + SEZ + Deemed export (sheet: 'b2b,sez,de')
    """

    SHEET_NAME = "b2b,sez,de"

    # Allowed values as per GST template
    ALLOWED_INVOICE_TYPES = {
        "Regular B2B",
        "SEZ supplies with payment",
        "SEZ supplies without payment",
        "Deemed Exp",
        "Intra-State supplies attracting IGST",
    }

    GSTIN_REGEX = re.compile(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$")
    VALID_GST_RATES = {0, 0.1, 0.25, 1, 1.5, 3, 5, 12, 18, 28}

    def map_invoice_type(self, value):
        """Convert internal invoice type → allowed GST dropdown value."""
        if not value:
            return "Regular B2B"

        v = str(value).lower()

        if "sez" in v:
            if "without" in v:
                return "SEZ supplies without payment"
            return "SEZ supplies with payment"

        if "deemed" in v:
            return "Deemed Exp"

        if "intra" in v:
            return "Intra-State supplies attracting IGST"

        return "Regular B2B"

    # -------------------------------
    # VALIDATORS (ALL INLINE)
    # -------------------------------
    def validate_gstin(self, gstin):
        if not gstin:
            return False, "GSTIN required"
        if not self.GSTIN_REGEX.match(str(gstin).strip()):
            return False, "Invalid GSTIN"
        return True, None

    def validate_invoice_no(self, inv):
        if not inv:
            return False, "Invoice number required"
        inv = str(inv)
        if len(inv) > 16:
            return False, "Invoice number > 16 characters"
        if not re.match(r"^[A-Za-z0-9/-]+$", inv):
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
            return False, "Invalid invoice date"

    def validate_rate(self, rate):
        try:
            if float(rate) in self.VALID_GST_RATES:
                return True, None
            return False, "Invalid GST rate"
        except:
            return False, "Invalid GST rate"

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

    def validate_invoice_type(self, inv_type):
        if inv_type not in self.ALLOWED_INVOICE_TYPES:
            return False, "Invoice Type invalid"
        return True, None

    def validate_pos(self, pos):
        if not pos or "-" not in str(pos):
            return False, "Invalid POS"
        return True, None

    # -------------------------------
    # MAIN BUILD METHOD
    # -------------------------------
    def build(self, df: pd.DataFrame, headers):
        mask = (
            df["_has_valid_gstin"]
            & (~df["_is_credit_or_debit"])
            & (~df["_is_export"])
        )
        subset = df[mask]

        if subset.empty:
            return pd.DataFrame(columns=headers)

        h = {name: name for name in headers}
        rows = []
        errors = []

        for idx, r in subset.iterrows():

            row = {}

            # GSTIN
            ok, err = self.validate_gstin(r["_gstin"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["GSTIN/UIN of Recipient"]] = r["_gstin"]

            # Receiver Name
            row[h["Receiver Name"]] = r["_receiver_name"]

            # Invoice Number
            ok, err = self.validate_invoice_no(r["_invoice_number"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Invoice Number"]] = r["_invoice_number"]

            # Invoice Date
            ok, err = self.validate_invoice_date(r["_invoice_date"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Invoice date"]] = r["_invoice_date"]

            # Invoice Value
            row[h["Invoice Value"]] = round_money(r["_invoice_value"])

            # POS
            pos = format_place_of_supply(r["_pos_code"])
            ok, err = self.validate_pos(pos)
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Place Of Supply"]] = pos

            # Reverse Charge (always N)
            row[h["Reverse Charge"]] = "N"

            # Invoice Type
            mapped_type = self.map_invoice_type(r["_invoice_type"])
            ok, err = self.validate_invoice_type(mapped_type)
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Invoice Type"]] = mapped_type

            # E-Commerce GSTIN
            row[h["E-Commerce GSTIN"]] = r["_ecommerce_gstin"]

            # Rate
            ok, err = self.validate_rate(r["_rate"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Rate"]] = round_money(r["_rate"])

            # Taxable Value
            ok, err = self.validate_taxable_value(r["_taxable_value"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Taxable Value"]] = round_money(r["_taxable_value"])

            # Cess Amount
            ok, err = self.validate_cess(r["_cess_amount"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Cess Amount"]] = round_money(abs(r["_cess_amount"]))

            rows.append(row)

        # Debug output
        if errors:
            print("⚠️ B2B Validation Errors:")
            for e in errors:
                print(f"  Row {e[0]} → {e[1]}")

        # Build final DataFrame
        result = pd.DataFrame(rows)
        for col in headers:
            if col not in result.columns:
                result[col] = None

        return result[headers]
