"""
GSTR-2B B2B Reconciliation Logic
--------------------------------
Identity:
    Supplier GSTIN + Invoice No + Invoice Date

Comparison:
    Taxable Value

Outcomes:
    - Matched
    - Not Matched
    - Not in Books
    - Not Found in 2B
"""

from app.gstr1.utils.gst_utils import safe_string, to_float, round_money
from app.gstr1.utils.date_utils import parse_excel_or_date

from app.utils.logger import setup_logger

logger = setup_logger(
    name="gstr2.reconciler",
    log_file="logs/gstr2_reconciler.log"
)


def _make_key(gstin, invoice_no, invoice_date):
    return (
        safe_string(gstin),
        safe_string(invoice_no),
        parse_excel_or_date(invoice_date),
    )


def reconcile_b2b(gstr2b_rows, purchase_rows):
    output_rows = []
    matched_purchase_keys = set()

    logger.info(f"GSTR2B rows received: {len(gstr2b_rows)}")
    logger.info(f"Purchase rows received: {len(purchase_rows)}")

    # -----------------------------
    # PASS 1: GSTR2B → Purchase
    # -----------------------------
    for row in gstr2b_rows:
        key = _make_key(
            row["supplier_gstin"],
            row["invoice_no"],
            row["invoice_date"],
        )

        gstr_value = round_money(to_float(row["taxable_value"]))

        if key not in purchase_rows:
            comment = "Not in Books"
            book_value = None
            logger.info(f"[NOT IN BOOKS] {key}")
        else:
            book_value = round_money(to_float(purchase_rows[key]))
            matched_purchase_keys.add(key)

            if gstr_value == book_value:
                comment = "Matched"
                logger.info(f"[MATCHED] {key} VALUE={gstr_value}")
            else:
                comment = "Not Matched"
                logger.info(
                    f"[MISMATCH] {key} GSTR2B={gstr_value} BOOKS={book_value}"
                )

        output_rows.append({
            "sheet": "B2B",
            "supplier_gstin": key[0],
            "invoice_no": key[1],
            "invoice_date": key[2],
            "gstr2b_taxable_value": gstr_value,
            "book_taxable_value": book_value,
            "comment": comment,
        })

    # -----------------------------
    # PASS 2: Purchase → Missing in 2B
    # -----------------------------
    for key, book_value in purchase_rows.items():
        if key in matched_purchase_keys:
            continue

        logger.info(f"[NOT FOUND IN 2B] {key}")

        output_rows.append({
            "sheet": "B2B",
            "supplier_gstin": key[0],
            "invoice_no": key[1],
            "invoice_date": key[2],
            "gstr2b_taxable_value": None,
            "book_taxable_value": round_money(to_float(book_value)),
            "comment": "Not Found in 2B",
        })

    logger.info(f"Final reconciled rows: {len(output_rows)}")
    return output_rows
