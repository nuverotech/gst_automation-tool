import pandas as pd
from datetime import datetime, date

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import format_place_of_supply, get_normalized_state_pos, normalize_pos_code, state_code_from_value   


class B2CLBuilder:
    SHEET_NAME = "b2cl"

    # -------------------------
    # VALIDATION HELPERS
    # -------------------------

    def validate_invoice_no(self, inv):
        if not inv:
            return False, "Invoice number required"
        inv = str(inv)
        if len(inv) > 16:
            return False, "Invoice number > 16 characters"
        return True, None

    def validate_invoice_date(self, dt):
        if not dt:
            return False, "Invoice date required"

        try:
            if isinstance(dt, str):
                datetime.strptime(dt[:11], "%d-%b-%Y")
                return True, None
            if isinstance(dt, (datetime, date)):
                return True, None
        except:
            return False, "Invalid invoice date"

        return False, "Invalid invoice date"

    def validate_invoice_value_limit(self, value, dt):
        """
        B2CL condition:
        a) Interstate (POS != Supplier State)
        b) Invoice value > 1,00,000 after Aug 2024
           > 2,50,000 before Aug 2024
        """

        if not value:
            return False, "Invoice value missing"

        try:
            v = float(value)
        except:
            return False, "Invalid invoice value"

        if isinstance(dt, str):
            try:
                dt = datetime.strptime(dt[:11], "%d-%b-%Y").date()
            except:
                return False, "Invalid invoice date"

        cutoff_date = date(2024, 8, 1)

        # Threshold logic
        if dt < cutoff_date:
            return (v > 250000), "Invoice < 250000 (pre-Aug 2024)"
        else:
            return (v > 100000), "Invoice < 100000 (post-Aug 2024)"

    def validate_interstate(self, pos_code, source_state_code):
        """
        Interstate means POS != Supplier/Source State.
        """

        if not pos_code or not source_state_code:
            return False

        return str(pos_code).upper() != str(source_state_code).upper()

    def validate_rate(self, rate):
        try:
            float(rate)
            return True, None
        except:
            return False, "Invalid GST rate"

    def validate_taxable_value(self, val):
        try:
            float(val)
            return True, None
        except:
            return False, "Invalid taxable value"

    # -------------------------
    # MAIN BUILD METHOD
    # -------------------------

    def build(self, df: pd.DataFrame, headers):

        # Identify B2CL rows
        mask = (
            (~df["_has_valid_gstin"])
            # & (df["_ecommerce_gstin"].isna() | (df["_ecommerce_gstin"] == ""))
            & (~df["_is_credit_or_debit"])
            & (~df["_is_export"])
            & (~df["_is_same_state"])
        )

        subset = df[mask]

        # Returns a tuple (number_of_rows, number_of_columns)
        # print("Length of subset for B2CL:", subset.shape[0])

        if subset.empty:
            return pd.DataFrame(columns=headers)

        rows = []
        h = {c: c for c in headers}
        errors = []

        for idx, r in subset.iterrows():

            # pos_raw = r["_pos_code"]
            # src_raw = r["_source_of_supply"]  # <-- user input

            # pos_code = get_normalized_state_pos(pos_raw)
            # src_code = get_normalized_state_pos(src_raw)


            # # -----------------------------
            # # Interstate check
            # # -----------------------------
            # if not self.validate_interstate(pos_code, src_code):
            #     continue  # Skip intrastate invoices

            # -----------------------------
            # Invoice value limit check
            # -----------------------------
            ok, err = self.validate_invoice_value_limit(r["_invoice_value"], r["_invoice_date"])
            if not ok:
                continue

            row = {}

            # Invoice number
            ok, err = self.validate_invoice_no(r["_invoice_number"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Invoice Number"]] = r["_invoice_number"]

            # Invoice date
            ok, err = self.validate_invoice_date(r["_invoice_date"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Invoice date"]] = r["_invoice_date"]

            # Invoice value
            row[h["Invoice Value"]] = round_money(r["_invoice_value"])

            raw_pos = r["_pos_code"]

            # Step 1: Accept OT / MH / 27 / Maharashtra → normalize
            pos_raw_normalized = normalize_pos_code(raw_pos)

            # Step 2: POS → proper GST state code
            pos_state_code = state_code_from_value(pos_raw_normalized)

            # Step 3: Convert to final required format → "27-Maharashtra"
            formatted_pos = format_place_of_supply(pos_state_code)

            # Place of supply formatted to template (e.g., "27-Maharashtra")
            row[h["Place Of Supply"]] = formatted_pos

            # Rate
            ok, err = self.validate_rate(r["_rate"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Rate"]] = round_money(r["_rate"])

            # Taxable value
            ok, err = self.validate_taxable_value(r["_taxable_value"])
            if not ok:
                errors.append((idx, err))
                continue
            row[h["Taxable Value"]] = round_money(r["_taxable_value"])

            # Cess
            row[h["Cess Amount"]] = round_money(abs(r["_cess_amount"]))

            # E-commerce GSTIN
            row[h["E-Commerce GSTIN"]] = r["_ecommerce_gstin"] or None

            rows.append(row)

        if errors:
            print("⚠️ B2CL Validation Errors:")
            for e in errors:
                print(f"  Row {e[0]} → {e[1]}")

        df_final = pd.DataFrame(rows)

        # Add missing columns
        for col in headers:
            if col not in df_final.columns:
                df_final[col] = None

        return df_final[headers]
