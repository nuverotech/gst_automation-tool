from datetime import date, timedelta
from typing import Optional

import pandas as pd


def parse_excel_or_date(value) -> Optional[date]:
    """
    Fast-mode date parser:
    - Handles Excel serial dates.
    - Handles strings / datetime-like values via pandas.
    - Returns date or None.
    """
    if value is None:
        return None

    # Excel serial dates
    if isinstance(value, (int, float)):
        try:
            if value > 20000:  # very rough cutoff, works for modern years
                base = date(1899, 12, 30)
                return base + timedelta(days=float(value))
        except Exception:
            pass

    parsed = pd.to_datetime(value, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.date()
