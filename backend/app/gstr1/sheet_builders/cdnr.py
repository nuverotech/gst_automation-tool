import re
import pandas as pd
from datetime import datetime

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import (
    normalize_pos_code,
    state_code_from_value,
    format_place_of_supply
)


class CDNRBuilder:
    SHEET_NAME = "cdnr"

    GSTIN_REGEX = re.compile(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][1-9A-Z]Z[0-9A-Z]$")
    VALID_GST_RATES = {0, 0.1, 0.25, 1, 1.5, 3, 5, 12, 18, 28}

    # -------------------------------
    # VALIDATORS
    # -------------------------------
    def validate_gstin(self, g):
        if not g:
            return False, "GSTIN required"
        if not self.GSTIN_REGEX.match(str(g).strip()):
            return False, "Invalid GSTIN"
        return True, None

    def validate_note_no(self, n):
        if not n:
            return False, "Note number required"
        n = str(n)
        if len(n) > 16:
            return False, "Note number > 16 characters"
        if not re.match(r"^[A-Za-z0-9/-]+$", n):
            return False, "Invalid characters in note number"
        return True, None

    def validate_note_date(self, d):
        if not d:
            return False, "Note date required"
        try:
            if isinstance(d, str):
                datetime.strptime(d[:11], "%d-%b-%Y")
            return True, None
        except:
            return False, "Invalid note date"

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

    def validate_note_type(self, t):
        if t not in ["C", "D"]:
            return False, "Note Type must be C or D"
        return True, None

    def validate_pos(self, pos_display):
        """pos_display must be 'XX-State Name'."""
        if not pos_display or "-" not in pos_display:
            return False, "Invalid POS"
        return True, None

    # -------------------------------
    # MAIN BUILD
    # -------------------------------
    def build(self, df: pd.DataFrame, headers):

        mask = df["_is_credit_or_debit"] & df["_has_valid_gstin"] & (~df["_is_ur_note"])
        subset = df[mask]

        if subset.empty:
            return pd.DataFrame(columns=headers)

        # Prepare helper mapping
        h = {name: name for name in headers}
        rows = []
        errors = []

        print("Length of subset for CDNR:", subset.shape[0])

        for idx, r in subset.iterrows():

            row = {}

            # ---------------- GSTIN ----------------
            ok, err = self.validate_gstin(r["_gstin"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["GSTIN/UIN of Recipient"]] = r["_gstin"]

            # ---------------- Receiver Name ----------------
            row[h["Receiver Name"]] = r["_receiver_name"]

            # ---------------- Note Number ----------------
            ok, err = self.validate_note_no(r["_note_number"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Note Number"]] = r["_note_number"]

            # ---------------- Note Date ----------------
            ok, err = self.validate_note_date(r["_note_date"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Note Date"]] = r["_note_date"]

            # ---------------- Note Type ----------------
            ok, err = self.validate_note_type(r["_note_type"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Note Type"]] = r["_note_type"]

            # ---------------- POS (with new logic) ----------------
            normalized = normalize_pos_code(r["_pos_code"])
            state_code = state_code_from_value(normalized)
            pos_display = format_place_of_supply(state_code)

            ok, err = self.validate_pos(pos_display)
            if not ok:
                errors.append((idx, err))
                continue

            row[h["Place Of Supply"]] = pos_display

            # ---------------- Reverse Charge ----------------
            row[h["Reverse Charge"]] = "N"

            # ---------------- Note Supply Type ----------------
            inv_type_raw = str(r["_invoice_type"]).lower()

            if "sez" in inv_type_raw:
                row[h["Note Supply Type"]] = "SEZ"
            elif "deemed" in inv_type_raw:
                row[h["Note Supply Type"]] = "DE"
            else:
                row[h["Note Supply Type"]] = "B2B"

            # ---------------- Note Value ----------------
            note_val = abs(round_money(r["_note_value"] or 0))
            row[h["Note Value"]] = note_val

            # ---------------- Rate ----------------
            ok, err = self.validate_rate(r["_rate"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Rate"]] = round_money(r["_rate"])

            # ---------------- Taxable Value ----------------
            ok, err = self.validate_taxable_value(abs(r["_taxable_value"]))
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Taxable Value"]] = abs(round_money(r["_taxable_value"]))

            # ---------------- Cess Amount ----------------
            ok, err = self.validate_cess(r["_cess_amount"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Cess Amount"]] = abs(round_money(r["_cess_amount"]))

            rows.append(row)

        # ---------------- Print any validation errors ----------------
        if errors:
            print("⚠️ CDNR Validation Errors:")
            for e in errors:
                print(f"  Row {e[0]} → {e[1]}")

        df_final = pd.DataFrame(rows)

        # Add missing columns
        for col in headers:
            if col not in df_final.columns:
                df_final[col] = None

        return df_final[headers]
