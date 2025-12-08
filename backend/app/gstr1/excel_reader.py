import os
from typing import Optional, List

import pandas as pd


class ExcelReader:
    """
    Simple Excel reader that tries sensible engines based on extension.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path

    def _get_engine_priority(self) -> List[str]:
        ext = os.path.splitext(self.file_path)[1].lower()
        if ext == ".xlsb":
            return ["pyxlsb"]
        if ext == ".xls":
            return ["xlrd"]
        return ["openpyxl"]

    def read(self, sheet_name: Optional[str] = None) -> pd.DataFrame:
        engines = self._get_engine_priority()
        last_error: Optional[Exception] = None

        for engine in engines:
            try:
                df = pd.read_excel(
                    self.file_path,
                    sheet_name=sheet_name if sheet_name is not None else 0,
                    engine=engine,
                )
                if isinstance(df, dict):
                    df = next(iter(df.values()))
                return df
            except Exception as exc:
                last_error = exc

        raise RuntimeError(f"Unable to read Excel file {self.file_path}: {last_error}")
