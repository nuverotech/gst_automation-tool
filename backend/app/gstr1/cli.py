import argparse
import os
import shutil

from app.gstr1.excel_reader import ExcelReader
from app.gstr1.transformer import GSTTransformer
from app.gstr1.sheet_writer import SheetWriter


def run_gstr1(input_path: str, template_path: str, output_path: str, progress_callback=None):
    """
    Main reusable GSTR-1 processor for Celery + CLI.
    """

    # --- 1. Reading input file ---
    if progress_callback: progress_callback(5, "Reading input file")
    reader = ExcelReader(input_path)
    df = reader.read()
    if progress_callback: progress_callback(20, "Input file loaded")

    # --- 2. Transform / normalize user data ---
    if progress_callback: progress_callback(40, "Transforming data")
    transformer = GSTTransformer()
    enriched_df = transformer.enrich(df)
    if progress_callback: progress_callback(60, "Data transformed")

    # --- 3. Build sheets into template ---
    if progress_callback: progress_callback(75, "Populating GST sheets")
    writer = SheetWriter(template_path=template_path, output_path=output_path)
    writer.build_and_write_all(enriched_df)

    # --- 4. Save final workbook ---
    if progress_callback: progress_callback(90, "Saving output file")
    try:
        writer.save()
    except:
        pass

    if progress_callback: progress_callback(100, "Completed")

    return output_path


def main():
    parser = argparse.ArgumentParser(
        description="Offline GSTR-1 Excel generator (CLI, fast mode)."
    )
    parser.add_argument(
        "--input", "-i", required=True, help="Path to input data Excel file.",
    )
    parser.add_argument(
        "--template",
        "-t",
        required=True,
        help="Path to GSTR-1 Excel workbook template (e.g. GSTR1_Excel_Workbook_Template_V2.2.xlsx).",
    )
    parser.add_argument(
        "--out",
        "-o",
        required=True,
        help="Output Excel file path (a copy of template populated with data).",
    )

    args = parser.parse_args()

    input_path = os.path.abspath(args.input)
    template_path = os.path.abspath(args.template)
    out_path = os.path.abspath(args.out)

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"Template file not found: {template_path}")

    # Copy template to output
    shutil.copyfile(template_path, out_path)

    # Read user data
    reader = ExcelReader(input_path)
    df = reader.read()

    # Enrich / normalize data
    transformer = GSTTransformer()
    enriched_df = transformer.enrich(df)

    # Build all sheets and write into the copied template
    writer = SheetWriter(template_path=template_path, output_path=out_path)
    writer.build_and_write_all(enriched_df)

    print(f"GSTR-1 workbook generated at: {out_path}")


if __name__ == "__main__":
    main()
