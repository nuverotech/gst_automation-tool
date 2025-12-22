import pandas as pd
from datetime import datetime, date

from app.gstr1.utils.gst_utils import round_money
from app.gstr1.utils.state_codes import (
    normalize_pos_code,
    state_code_from_value,
    format_place_of_supply,
)


class B2CSBuilder:
    SHEET_NAME = "b2cs"

    VALID_GST_RATES = {0, 0.1, 0.25, 1, 1.5, 3, 5, 12, 18, 28}

    # ------------------------------
    # VALIDATORS
    # ------------------------------
    def validate_type(self, t):
        if t not in ("E", "OE"):
            return False, "Invalid Type (must be E or OE)"
        return True, None

    def validate_rate(self, rate):
        try:
            float(rate)
            return True, None
        except:
            return False, "Invalid GST Rate"

    def validate_taxable_value(self, val):
        try:
            float(val)
            return True, None
        except:
            return False, "Invalid taxable value"

    def validate_cess(self, val):
        try:
            float(val)
            return True, None
        except:
            return False, "Invalid cess amount"

    def validate_interstate_limit(self, invoice_value, invoice_date):
        """
        For inter-state B2CS:
        Invoice value must be <= 1 lakh (Aug 2024 onwards)
        <= 2.5 lakh before Aug 2024
        """

        try:
            iv = float(invoice_value)
        except:
            return False, "Invalid invoice value"

        # Parse date
        if isinstance(invoice_date, str):
            try:
                d = datetime.strptime(invoice_date[:11], "%d-%b-%Y").date()
            except:
                return False, "Invalid invoice date"
        elif isinstance(invoice_date, datetime):
            d = invoice_date.date()
        elif isinstance(invoice_date, date):
            d = invoice_date
        else:
            return False, "Invalid invoice date"

        cutoff = date(2024, 8, 1)

        if d < cutoff:
            return (iv <= 250000), "Interstate value > 250000 (pre-Aug 2024)"
        else:
            return (iv <= 100000), "Interstate value > 100000 (post-Aug 2024)"

    # ------------------------------
    # BUILD
    # ------------------------------
    def build(self, df: pd.DataFrame, headers):

        # ---------------------------------------------------------
        # B2CS applies only to consumers:
        #   - No GSTIN
        #   - Not credit/debit note
        #   - Not export
        # ---------------------------------------------------------
        base_mask = (
            (~df["_has_valid_gstin"])
            & (~df["_is_credit_or_debit"])
            & (~df["_is_export"])
        )

        # ---------------------------------------------------------
        # MASK 1 → INTRA-STATE (Always allowed)
        # ---------------------------------------------------------
        mask_intra = base_mask & df["_is_same_state"]

        # ---------------------------------------------------------
        # MASK 2 → INTER-STATE but below threshold
        # ---------------------------------------------------------
        mask_inter_candidates = base_mask & (~df["_is_same_state"])

        inter_rows = []
        for idx, r in df[mask_inter_candidates].iterrows():
            ok, _ = self.validate_interstate_limit(r["_invoice_value"], r["_invoice_date"])
            if ok:
                inter_rows.append(idx)

        mask_inter = df.index.isin(inter_rows)

        # FINAL B2CS dataset = union of mask1 + mask2
        subset = df[mask_intra | mask_inter].copy()

        # print("Length of subset for B2CS:", subset.shape[0])

        if subset.empty:
            return pd.DataFrame(columns=headers)

        rows = []
        h = {c: c for c in headers}
        errors = []

        # ---------------------------------------------------------
        # FINAL ROW BUILD LOOP
        # ---------------------------------------------------------
        for idx, r in subset.iterrows():

            # ---- Normalise POS ----
            raw_pos = r["_pos_code"]
            pos_normalized = normalize_pos_code(raw_pos)
            pos_state_code = state_code_from_value(pos_normalized)
            pos_display = format_place_of_supply(pos_state_code)

            # ---- Skip OT rows ----
            if pos_state_code == "OT":
                continue

            typ = r["_type_flag"] or "OE"

            # ---- Validations ----
            ok, err = self.validate_type(typ)
            if not ok:
                errors.append((idx, err)); continue

            ok, err = self.validate_rate(r["_rate"])
            if not ok:
                errors.append((idx, err)); continue

            ok, err = self.validate_taxable_value(r["_taxable_value"])
            if not ok:
                errors.append((idx, err)); continue

            ok, err = self.validate_cess(r["_cess_amount"])
            if not ok:
                errors.append((idx, err)); continue

            # ---- BUILD ROW ----
            row = {
                h["Type"]: typ,
                h["Place Of Supply"]: pos_display,
                h["Rate"]: round_money(r["_rate"]),
                h["Taxable Value"]: round_money(r["_taxable_value"]),
                h["Cess Amount"]: round_money(abs(r["_cess_amount"])),
                h["E-Commerce GSTIN"]: r["_ecommerce_gstin"] or None,
            }

            rows.append(row)

        # Show validation errors
        if errors:
            print("⚠️ B2CS Validation Errors:")
            for e in errors:
                print(f"  Row {e[0]} → {e[1]}")

        df_final = pd.DataFrame(rows)

        # Add missing template columns
        for col in headers:
            if col not in df_final.columns:
                df_final[col] = None

        return df_final[headers]
