from openpyxl import load_workbook
from app.gstr2.reconciler import reconcile_sheets
from pathlib import Path


def process_gstr2b_files(gstr2b_path: str, purchase_path: str) -> dict:
    """
    Main entry point for GSTR-2B Excel reconciliation.
    """

    gstr2b_wb = load_workbook(gstr2b_path)
    purchase_wb = load_workbook(purchase_path)

    summary = {
        "sheets_processed": [],
        "total_rows": 0,
        "matched": 0,
        "mismatched": 0,
    }

    for sheet_name in gstr2b_wb.sheetnames:
        ws = gstr2b_wb[sheet_name]

        result = reconcile_sheets(
            sheet_name=sheet_name,
            gstr2b_sheet=ws,
            purchase_wb=purchase_wb,
        )

        summary["sheets_processed"].append(sheet_name)
        summary["total_rows"] += result["rows"]
        summary["matched"] += result["matched"]
        summary["mismatched"] += result["mismatched"]

    return summary
