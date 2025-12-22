def reconcile_sheets(sheet_name, gstr2b_sheet, purchase_wb):
    """
    Reconcile one GSTR-2B sheet.
    For now: counts rows only.
    """

    rows = list(gstr2b_sheet.iter_rows(min_row=2, values_only=True))

    return {
        "sheet": sheet_name,
        "rows": len(rows),
        "matched": 0,      # TODO: implement logic
        "mismatched": 0,   # TODO: implement logic
    }
