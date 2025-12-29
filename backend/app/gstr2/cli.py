import json
import sys
from app.gstr2.processor import process_b2b_single_state


def main():
    if len(sys.argv) != 3:
        print("Usage:")
        print("  python -m app.gstr2.cli <purchase.xlsx> <gstr2b.xlsx>")
        sys.exit(1)

    purchase_path = sys.argv[1]
    gstr2b_path = sys.argv[2]

    summary = process_b2b_single_state(
        purchase_path=purchase_path,
        gstr2b_path=gstr2b_path,
    )

    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
