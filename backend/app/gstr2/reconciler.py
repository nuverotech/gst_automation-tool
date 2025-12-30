from app.utils.logger import setup_logger

logger = setup_logger(
    name="gstr2.reconciler",
    log_file="logs/gstr2_reconciler.log"
)


# def reconcile_b2b(gstr2b_rows, purchase_rows):
#     results = []
#     used_purchase_indexes = set()

#     logger.info(f"GSTR2B rows: {len(gstr2b_rows)}")
#     logger.info(f"Purchase rows: {len(purchase_rows)}")

#     # -------------------------------------------------
#     # PASS 1: GSTR-2B → PURCHASE BOOKS
#     # -------------------------------------------------
#     for g in gstr2b_rows:
#         exact_match_index = None
#         partial_match_found = False

#         for idx, p in enumerate(purchase_rows):
#             if idx in used_purchase_indexes:
#                 continue

#             if g["gstin"] == p["gstin"] and g["invoice_no"] == p["invoice_no"]:
#                 partial_match_found = True

#                 if (
#                     g["taxable_value"] == p["taxable_value"]
#                     # and g["cgst"] == p["cgst"]
#                     # and g["sgst"] == p["sgst"]
#                     # and g["igst"] == p["igst"]
#                     # and g["total"] == p["total"]
#                 ):
#                     exact_match_index = idx
#                     break

#         if exact_match_index is not None:
#             used_purchase_indexes.add(exact_match_index)
#             comment = "Matched"
#         elif partial_match_found:
#             comment = "Not Matched"
#         else:
#             comment = "Not in Books"

#         results.append({
#             "gstin": g["gstin"],
#             "invoice_no": g["invoice_no"],
#             "invoice_date": g["invoice_date"],
#             "taxable_value": g["taxable_value"],
#             # "cgst": g["cgst"],
#             # "sgst": g["sgst"],
#             # "igst": g["igst"],
#             # "total": g["total"],
#             "comment": comment,
#         })

#     # -------------------------------------------------
#     # PASS 2: PURCHASE BOOKS → GSTR-2B
#     # -------------------------------------------------
#     for idx, p in enumerate(purchase_rows):
#         if idx in used_purchase_indexes:
#             continue

#         results.append({
#             "gstin": p["gstin"],
#             "invoice_no": p["invoice_no"],
#             "invoice_date": p["invoice_date"],
#             "taxable_value": p["taxable_value"],
#             # "cgst": p["cgst"],
#             # "sgst": p["sgst"],
#             # "igst": p["igst"],
#             # "total": p["total"],
#             "comment": "Not Found in 2B",
#         })

#     logger.info(f"Final reconciled rows: {len(results)}")
#     return results

def reconcile_b2b(gstr2b_rows, purchase_rows):
    results = []

    # 🔑 track purchase rows seen in 2B (identity match)
    identity_matched_indexes = set()

    # 🔑 track exact matches (optional, for stats/debug)
    exact_matched_indexes = set()

    logger.info(
        f"START RECONCILIATION | "
        f"GSTR2B={len(gstr2b_rows)} PURCHASE={len(purchase_rows)}"
    )

    # -------------------------------------------------
    # PASS 1: GSTR-2B → PURCHASE BOOKS
    # -------------------------------------------------
    for g in gstr2b_rows:
        exact_match_index = None
        partial_match_index = None

        logger.debug(
            f"CHECKING 2B INVOICE | GSTIN={g['gstin']} | "
            f"INV={g['invoice_no']} | TAXABLE_2B={g['taxable_value']}"
        )

        for idx, p in enumerate(purchase_rows):

            # skip already exactly matched rows
            if idx in exact_matched_indexes:
                continue

            if g["gstin"] == p["gstin"] and g["invoice_no"] == p["invoice_no"]:
                identity_matched_indexes.add(idx)
                partial_match_index = idx

                logger.debug(
                    f"  FOUND BOOK ENTRY | GSTIN={p['gstin']} | "
                    f"INV={p['invoice_no']} | TAXABLE_BOOK={p['taxable_value']}"
                )

                if g["taxable_value"] == p["taxable_value"]:
                    exact_match_index = idx
                    exact_matched_indexes.add(idx)

                    logger.info(
                        f"MATCHED | GSTIN={g['gstin']} | INV={g['invoice_no']} | "
                        f"DATE={g['invoice_date']} | "
                        f"TAXABLE=2B({g['taxable_value']}) == BOOK({p['taxable_value']})"
                    )
                    break

        if exact_match_index is not None:
            comment = "Matched"

        elif partial_match_index is not None:
            p = purchase_rows[partial_match_index]
            comment = "Not Matched"

            logger.warning(
                f"NOT MATCHED | GSTIN={g['gstin']} | INV={g['invoice_no']} | "
                f"DATE={g['invoice_date']} | "
                f"TAXABLE=2B({g['taxable_value']}) != BOOK({p['taxable_value']})"
            )

        else:
            comment = "Not in Books"
            logger.error(
                f"NOT IN BOOKS | GSTIN={g['gstin']} | INV={g['invoice_no']} | "
                f"DATE={g['invoice_date']} | TAXABLE_2B={g['taxable_value']}"
            )

        results.append({
            "gstin": g["gstin"],
            "invoice_no": g["invoice_no"],
            "invoice_date": g["invoice_date"],
            "taxable_value": g["taxable_value"],
            "comment": comment,
        })

    logger.info(
        f"IDENTITY MATCHED PURCHASE INDEXES={identity_matched_indexes} "
        f"COUNT={len(identity_matched_indexes)}"
    )

    # -------------------------------------------------
    # PASS 2: PURCHASE BOOKS → GSTR-2B
    # -------------------------------------------------
    for idx, p in enumerate(purchase_rows):

        # ❗ exclude ALL purchase rows already seen in 2B
        if idx in identity_matched_indexes:
            continue

        logger.error(
            f"NOT FOUND IN 2B | GSTIN={p['gstin']} | "
            f"INV={p['invoice_no']} | DATE={p['invoice_date']} | "
            f"TAXABLE_BOOK={p['taxable_value']} | INDEX={idx}"
        )

        results.append({
            "gstin": p["gstin"],
            "invoice_no": p["invoice_no"],
            "invoice_date": p["invoice_date"],
            "taxable_value": p["taxable_value"],
            "comment": "Not Found in 2B",
        })

    logger.info(f"END RECONCILIATION | TOTAL_OUTPUT_ROWS={len(results)}")
    return results
