from openpyxl import load_workbook

from app.gstr1.utils.date_utils import parse_excel_or_date
from app.gstr1.utils.gst_utils import safe_string, to_float
from app.gstr2.reconciler import reconcile_b2b
from app.utils.logger import setup_logger

logger = setup_logger(
    name="gstr2.processor",
    log_file="logs/gstr2_processor.log"
)


# -------------------------------------------------
# Robust header detector (FIXED)
# -------------------------------------------------
def _detect_header_row(ws, required_headers, max_scan_rows=20):
    """
    Detect header row by fuzzy matching across header blocks.
    Handles multi-row GSTR-2B headers (Invoice Details → Invoice Number).
    """

    best_match = {"row": None, "score": 0, "col_map": {}}

    for row_idx in range(1, max_scan_rows + 1):
        row_values = [
            safe_string(c.value).replace("\n", " ").strip()
            for c in ws[row_idx]
        ]

        # Look ahead one row for split headers (critical for GSTR-2B)
        next_row_values = []
        if row_idx + 1 <= ws.max_row:
            next_row_values = [
                safe_string(c.value).replace("\n", " ").strip()
                for c in ws[row_idx + 1]
            ]

        col_map = {}
        score = 0

        for col_idx, cell in enumerate(row_values):
            combined_text = cell.lower().replace(" ", "")

            # Merge with next row header text if present
            if col_idx < len(next_row_values):
                combined_text += next_row_values[col_idx].lower().replace(" ", "")

            for field, aliases in required_headers.items():
                for alias in aliases:
                    alias_norm = alias.lower().replace(" ", "")
                    if alias_norm in combined_text and field not in col_map:
                        col_map[field] = col_idx
                        score += 1

        if score > best_match["score"]:
            best_match = {
                "row": row_idx,
                "score": score,
                "col_map": col_map,
            }

    if best_match["score"] < 2:
        raise ValueError("Could not reliably detect header row")

    missing = set(required_headers.keys()) - set(best_match["col_map"].keys())
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    return best_match["row"], best_match["col_map"]



# -------------------------------------------------
# Purchase Register Reader
# -------------------------------------------------
def _read_purchase_register(purchase_path: str) -> dict:
    wb = load_workbook(purchase_path, data_only=True)
    ws = wb.active

    REQUIRED_HEADERS = {
        "gstin": {
            "VENDOR GSTN",
            "VENDOR GSTIN",
            "SUPPLIER GSTIN",
            "GSTIN",
        },
        "invoice_no": {
            "BILL NUMBER",
            "INVOICE NO",
            "DOC NO",
            "DOCUMENT NO",
        },
        "invoice_date": {
            "DATE",
            "INVOICE DATE",
            "BILL DATE",
        },
        "taxable_value": {
            "TAXABLE VALUE",
            "TOTAL PRICE",
            "TOTAL PRICE AFTER DISCOUNT",
            "NET AMOUNT",
        },
    }

    header_row, col_map = _detect_header_row(ws, REQUIRED_HEADERS)
    logger.info(f"Purchase header detected at row {header_row}")

    rows = {}

    raw_row_count = 0

    for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
        raw_row_count += 1

        gstin = safe_string(row[col_map["gstin"]])
        invoice_no = safe_string(row[col_map["invoice_no"]])

        if not gstin or not invoice_no:
            continue

        key = (
            gstin,
            invoice_no,
            parse_excel_or_date(row[col_map["invoice_date"]]),
        )

        taxable_value = to_float(row[col_map["taxable_value"]])

        # ✅ ACCUMULATE instead of overwrite
        if key in rows:
            rows[key] += taxable_value
        else:
            rows[key] = taxable_value

    logger.info(f"Raw purchase rows read (line items): {raw_row_count}")
    logger.info(f"Unique purchase invoices formed: {len(rows)}")

    return rows



# -------------------------------------------------
# GSTR-2B B2B Reader (FIXED)
# -------------------------------------------------
def _read_gstr2b_b2b(gstr2b_path: str) -> list[dict]:
    wb = load_workbook(gstr2b_path, data_only=True)

    if "B2B" not in wb.sheetnames:
        raise ValueError("B2B sheet not found in GSTR-2B")

    ws = wb["B2B"]

    REQUIRED_HEADERS = {
        "gstin": {
            "GSTIN OF SUPPLIER",
            "GSTIN",
        },
        "invoice_no": {
            "INVOICE NUMBER",
            "INVOICE NO",
        },
        "invoice_date": {
            "INVOICE DATE",
            "DATE",
        },
        "taxable_value": {
            "TAXABLE VALUE",
        },
    }

    header_row, col_map = _detect_header_row(ws, REQUIRED_HEADERS, max_scan_rows=20)
    logger.info(f"GSTR2B B2B header detected at row {header_row}")

    rows = []

    for row in ws.iter_rows(min_row=header_row + 1, values_only=True):
        gstin = safe_string(row[col_map["gstin"]])
        invoice_no = safe_string(row[col_map["invoice_no"]])

        if not gstin or not invoice_no:
            continue

        rows.append({
            "supplier_gstin": gstin,
            "invoice_no": invoice_no,
            "invoice_date": parse_excel_or_date(row[col_map["invoice_date"]]),
            "taxable_value": to_float(row[col_map["taxable_value"]]),
        })

    logger.info(f"GSTR2B B2B rows read: {len(rows)}")
    return rows


# -------------------------------------------------
# Public Entry Point
# -------------------------------------------------
def process_b2b_single_state(purchase_path: str, gstr2b_path: str) -> dict:
    purchase_rows = _read_purchase_register(purchase_path)
    gstr2b_rows = _read_gstr2b_b2b(gstr2b_path)

    reconciled_rows = reconcile_b2b(
        gstr2b_rows=gstr2b_rows,
        purchase_rows=purchase_rows,
    )

    summary = {
        "total_rows": len(reconciled_rows),
        "matched": sum(r["comment"] == "Matched" for r in reconciled_rows),
        "mismatched": sum(r["comment"] == "Not Matched" for r in reconciled_rows),
        "not_in_books": sum(r["comment"] == "Not in Books" for r in reconciled_rows),
        "not_found": sum(r["comment"] == "Not Found in 2B" for r in reconciled_rows),
    }

    return summary
