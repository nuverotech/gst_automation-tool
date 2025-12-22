import re
import pandas as pd
from datetime import datetime

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import (
    normalize_pos_code,
    state_code_from_value,
    format_place_of_supply
)


class CDNURBuilder:
    SHEET_NAME = "cdnur"

    NOTE_NUM_REGEX = re.compile(r"^[A-Za-z0-9/-]+$")
    VALID_GST_RATES = {0, 0.1, 0.25, 1, 1.5, 3, 5, 12, 18, 28}

    # ------------------------------
    # HELPERS
    # ------------------------------
    @staticmethod
    def normalize_amount(v):
        try:
            return abs(float(v))
        except:
            return 0.0

    # ------------------------------
    # VALIDATORS
    # ------------------------------
    def validate_note_no(self, n):
        if not n:
            return False, "Note number required"
        n = str(n)
        if len(n) > 16:
            return False, "Note number > 16 characters"
        if not self.NOTE_NUM_REGEX.match(n):
            return False, "Invalid characters in Note Number"
        return True, None

    def validate_note_date(self, d):
        if not d:
            return False, "Note date required"
        try:
            if isinstance(d, str):
                datetime.strptime(d[:11], "%d-%b-%Y")
            return True, None
        except:
            return False, "Invalid note date format"

    def validate_note_type(self, t):
        if t not in ("C", "D"):
            return False, "Note Type must be C or D"
        return True, None

    def validate_rate(self, r):
        try:
            if float(r) in self.VALID_GST_RATES:
                return True, None
        except:
            pass
        return False, "Invalid GST rate"

    def validate_interstate_and_threshold(
        self, invoice_val, supplier_state, pos_state, period_month, period_year
    ):
        if not supplier_state or not pos_state:
            return False, "Invalid state codes"

        # MUST be interstate
        if supplier_state == pos_state:
            return False, "CDNUR allowed only for INTERSTATE notes"

        # Threshold
        is_after_aug_2024 = (
            period_year > 2024 or (period_year == 2024 and period_month >= 8)
        )
        threshold = 100000 if is_after_aug_2024 else 250000

        try:
            if abs(float(invoice_val)) <= threshold:
                return False, f"Invoice value must be > {threshold}"
        except:
            return False, "Invalid invoice value"

        return True, None

    # ------------------------------
    # BUILD
    # ------------------------------
    def build(self, df: pd.DataFrame, headers):

        # CDNUR → credit/debit notes + UNREGISTERED
        mask = df["_is_credit_or_debit"] & (~df["_has_valid_gstin"])
        subset = df[mask]

        if subset.empty:
            return pd.DataFrame(columns=headers)

        h = {c: c for c in headers}
        rows = []
        errors = []

        print("Length of subset for CDNUR:", subset.shape[0])

        for idx, r in subset.iterrows():

            row = {}

            # ------------------------------------
            # UR TYPE (BASED ON ORIGINAL INVOICE)
            # ------------------------------------
            if r["_is_export"]:
                row[h["UR Type"]] = "EXPWP" if r["_export_type"] == "WPAY" else "EXPWOP"
            else:
                row[h["UR Type"]] = "B2CL"

            # ------------------------------------
            # Note Number
            # ------------------------------------
            ok, err = self.validate_note_no(r["_note_number"])
            if not ok:
                errors.append((idx, err)); continue
            row[h["Note Number"]] = r["_note_number"]

            # ------------------------------------
            # Note Date
            # ------------------------------------
            ok, err = self.validate_note_date(r["_note_date"])
            if not ok:
                errors.append((idx, err)); continue
            row[h["Note Date"]] = r["_note_date"]

            # ------------------------------------
            # Note Type
            # ------------------------------------
            ok, err = self.validate_note_type(r["_note_type"])
            if not ok:
                errors.append((idx, err)); continue
            row[h["Note Type"]] = r["_note_type"]

            # ------------------------------------
            # POS
            # ------------------------------------
            normalized = normalize_pos_code(r["_pos_code"])
            pos_state = state_code_from_value(normalized)
            row[h["Place Of Supply"]] = format_place_of_supply(pos_state)

            supplier_state = state_code_from_value(r["_supplier_state"])

            # ------------------------------------
            # INTERSTATE + THRESHOLD
            # ------------------------------------
            ok, err = self.validate_interstate_and_threshold(
                r["_invoice_value"],
                supplier_state,
                pos_state,
                r["_period_month"],
                r["_period_year"],
            )
            if not ok:
                errors.append((idx, err)); continue

            # ------------------------------------
            # Note Value
            # ------------------------------------
            row[h["Note Value"]] = round_money(
                self.normalize_amount(r["_note_value"])
            )

            # ------------------------------------
            # Rate
            # ------------------------------------
            ok, err = self.validate_rate(r["_rate"])
            if not ok:
                errors.append((idx, err)); continue
            row[h["Rate"]] = round_money(r["_rate"])

            # ------------------------------------
            # Taxable Value
            # ------------------------------------
            row[h["Taxable Value"]] = round_money(
                self.normalize_amount(r["_taxable_value"])
            )

            # ------------------------------------
            # Cess
            # ------------------------------------
            row[h["Cess Amount"]] = round_money(
                self.normalize_amount(r["_cess_amount"])
            )

            rows.append(row)

        if errors:
            print("⚠️ CDNUR Validation Errors:")
            for e in errors:
                print(f"  Row {e[0]} → {e[1]}")

        df_final = pd.DataFrame(rows)

        for col in headers:
            if col not in df_final.columns:
                df_final[col] = None

        return df_final[headers]
