import shutil
from typing import List, Type

import pandas as pd
import xlwings as xw

from app.gstr1.sheet_builders.b2b import B2BBuilder
from app.gstr1.sheet_builders.b2cl import B2CLBuilder
from app.gstr1.sheet_builders.b2cs import B2CSBuilder
from app.gstr1.sheet_builders.cdnr import CDNRBuilder
from app.gstr1.sheet_builders.cdnur import CDNURBuilder
from app.gstr1.sheet_builders.exp import EXPBuilder
from app.gstr1.sheet_builders.hsnb2b import HSNB2BBuilder
from app.gstr1.sheet_builders.hsnb2c import HSNB2CBuilder  
from app.gstr1.sheet_builders.eco import ECOBuilder  
from app.gstr1.sheet_builders.ecob2b import ECOB2BBuilder 
from app.gstr1.sheet_builders.docs import DOCSBuilder

class SheetWriter:
    """
    Writes GST output using Excel's native engine via xlwings.
    Preserves dropdowns, data validation, protection, and GST compliance.
    Shows progress per sheet.
    """

    HEADER_ROW_INDEX = 3      # Excel row 4
    DATA_START_ROW_INDEX = 4  # Excel row 5

    def __init__(self, template_path: str, output_path: str):
        self.template_path = template_path
        self.output_path = output_path

        # Copy template first (VERY IMPORTANT)
        shutil.copyfile(self.template_path, self.output_path)

        self.builder_classes: List[Type] = [
            B2BBuilder,
            B2CLBuilder,
            B2CSBuilder,
            CDNRBuilder,
            CDNURBuilder,
            EXPBuilder,
            HSNB2BBuilder,
            HSNB2CBuilder,
            DOCSBuilder,
            ECOBuilder,
            ECOB2BBuilder,
        ]

    # --------------------------------------------------
    # HELPERS
    # --------------------------------------------------

    def _get_sheet_headers(self, ws) -> List[str]:
        """
        Reads header row (Excel row 4).
        """
        last_col = ws.used_range.last_cell.column
        return ws.range(
            (self.HEADER_ROW_INDEX + 1, 1),
            (self.HEADER_ROW_INDEX + 1, last_col)
        ).value

    def _write_sheet_data(
        self,
        ws,
        df: pd.DataFrame,
        headers: List[str],
        sheet_name: str,
        chunk_size: int = 1000,
    ):
        """
        Writes DataFrame to Excel in chunks and prints progress.
        """
        if df.empty:
            return

        # Clean + align DataFrame
        df = df.fillna("").reset_index(drop=True)
        df = df[headers]

        start_row = self.DATA_START_ROW_INDEX + 1  # Excel row 5
        start_col = 1
        total_rows = len(df)

        print(f"    → Rows to write: {total_rows}")

        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            chunk = df.iloc[start:end].values.tolist()

            ws.range(
                (start_row + start, start_col)
            ).options(expand=False).value = chunk

            percent = (end / total_rows) * 100
            print(
                f"    → Written: {end} / {total_rows} ({percent:.1f}%)"
            )

    # --------------------------------------------------
    # MAIN ENTRY
    # --------------------------------------------------

    def build_and_write_all(self, df: pd.DataFrame) -> None:
        app = xw.App(visible=False, add_book=False)

        try:
            # SAFE performance settings (NO calculation changes)
            app.screen_updating = False
            app.display_alerts = False

            wb = app.books.open(self.output_path)

            total_sheets = len(self.builder_classes)

            app.api.EnableEvents = False

            for idx, builder_cls in enumerate(self.builder_classes, start=1):
                builder = builder_cls()
                sheet_name = builder.SHEET_NAME

                if sheet_name not in [s.name for s in wb.sheets]:
                    print(f"[{idx}/{total_sheets}] Skipping missing sheet: {sheet_name}")
                    continue

                ws = wb.sheets[sheet_name]
                headers = self._get_sheet_headers(ws)

                sheet_df = builder.build(df, headers)
                if sheet_df is None or sheet_df.empty:
                    print(f"[{idx}/{total_sheets}] {sheet_name}: No data")
                    continue

                print(f"\n[{idx}/{total_sheets}] Writing sheet: {sheet_name}")

                # Ensure correct column order
                final_df = pd.DataFrame(columns=headers)
                for col in sheet_df.columns:
                    if col in final_df.columns:
                        final_df[col] = sheet_df[col]

                # ✅ Excel-native, GST-safe write
                self._write_sheet_data(
                    ws=ws,
                    df=final_df,
                    headers=headers,
                    sheet_name=sheet_name,
                    chunk_size=3000,   # safe for ECOB2B
                )

            app.api.EnableEvents = True

            wb.save(self.output_path)
            print("\n✔️ All sheets completed successfully")

        finally:
            app.quit()
