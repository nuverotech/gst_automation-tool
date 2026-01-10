import json
from pathlib import Path

from app.gstr1.utils.gst_utils import safe_string
from app.gstr2.reconciler import reconcile_b2b
from app.utils.logger import setup_logger

logger = setup_logger(
    name="gstr2.processor",
    log_file="logs/gstr2_processor.log"
)

# -------------------------------------------------
# Header detection (UNCHANGED)
# -------------------------------------------------
def detect_header_row(ws, required_headers, max_scan_rows=20):
    best_match = {"row": None, "score": 0, "col_map": {}}

    for row_idx in range(1, max_scan_rows):
        row = [safe_string(c.value) for c in ws[row_idx]]
        next_row = [safe_string(c.value) for c in ws[row_idx + 1]]

        col_map = {}
        score = 0

        for col_idx in range(len(row)):
            texts = []

            if col_idx < len(next_row) and next_row[col_idx]:
                texts.append(next_row[col_idx].lower())

            if row[col_idx]:
                texts.append(row[col_idx].lower())

            combined = " ".join(texts)

            for field, aliases in required_headers.items():
                for alias in aliases:
                    if alias.lower() in combined and field not in col_map:
                        col_map[field] = col_idx
                        score += 1

        if score > best_match["score"]:
            best_match = {
                "row": row_idx,
                "score": score,
                "col_map": col_map,
            }

    missing = set(required_headers) - set(best_match["col_map"])
    if missing:
        raise ValueError(
            f"Header detection failed. Missing: {missing}. "
            f"Detected: {best_match['col_map']}"
        )

    return best_match["row"] + 1, best_match["col_map"]


# -------------------------------------------------
# MULTI-STATE PROCESSOR (POS-BASED)
# -------------------------------------------------
def process_b2b_multi_state(purchase_path: str, gstr2b_paths: list[str]) -> dict:
    from app.gstr2.sheet_reader.b2b import (
        read_purchase_register,
        read_gstr2b_b2b,
    )

    logger.info("START MULTI-STATE RECONCILIATION")

    all_purchase_rows = read_purchase_register(purchase_path)

    state_wise = {}
    overall = {
        "matched": 0,
        "not_matched": 0,
        "not_in_books": 0,
        "not_in_2b": 0,
    }

    for gstr2b_path in gstr2b_paths:
        gstr2b_rows = read_gstr2b_b2b(gstr2b_path)

        states = {r["pos_state"] for r in gstr2b_rows}
        if len(states) != 1:
            raise ValueError(f"Multiple POS states in {gstr2b_path}")

        state = states.pop()
        logger.info(f"PROCESSING STATE={state}")

        purchase_rows = [
            p for p in all_purchase_rows
            if p["pos_state"] == state
        ]

        reconciled = reconcile_b2b(
            gstr2b_rows=gstr2b_rows,
            purchase_rows=purchase_rows,
        )

        summary = {
            "total_rows": len(reconciled),
            "matched": sum(r["comment"] == "Matched" for r in reconciled),
            "not_matched": sum(r["comment"] == "Not Matched" for r in reconciled),
            "not_in_books": sum(r["comment"] == "Not in Books" for r in reconciled),
            "not_in_2b": sum(r["comment"] == "Not Found in 2B" for r in reconciled),
        }

        state_wise[state] = summary

        for k in overall:
            overall[k] += summary[k]

    final_result = {
        "state_wise": state_wise,
        "overall": overall,
    }

    # 🔵 NEW CODE: WRITE SUMMARY TO FILE
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)

    summary_file = logs_dir / "gstr2_state_wise_summary.json"
    with summary_file.open("w", encoding="utf-8") as f:
        json.dump(final_result, f, indent=2)

    logger.info(f"STATE-WISE SUMMARY WRITTEN TO {summary_file}")

    return final_result
