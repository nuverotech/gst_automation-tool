import json
import sys
from app.gstr2.processor import process_b2b_multi_state


def main():
    if len(sys.argv) < 3:
        print("Usage:")
        print("  python -m app.gstr2.cli <purchase.xlsx> <gstr2b1.xlsx> <gstr2b2.xlsx> ...")
        sys.exit(1)

    purchase_path = sys.argv[1]
    gstr2b_paths = sys.argv[2:]

    result = process_b2b_multi_state(
        purchase_path=purchase_path,
        gstr2b_paths=gstr2b_paths,
    )

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
