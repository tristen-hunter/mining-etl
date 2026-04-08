"""
Mining Fraud ETL Pipeline  —  run_pipeline.py

Walks the folder tree:
    master/
      YEAR/
        Month/
          Day/
            loading_list.csv
            olf_bookings.csv
            tons_report.csv

For each Day folder it:
  1. Runs the full fraud audit  (load.process_day)
  2. Writes a dated PDF report  (pdf.create_pdf_report)
  3. Appends rows to the master CSV summary  (load.write_summary_csv)
"""

import os
import traceback

from load import process_day, write_summary_csv
from pdf import create_pdf_report

# ── Path configuration ─────────────────────────────────────────────────────────
BASE_DIR   = r"C:\Users\trist\dev\mining-etl\master\2026"
OUTPUT_DIR = r"C:\Users\trist\dev\mining-etl\reports"
CSV_PATH   = r"C:\Users\trist\dev\mining-etl\DB\reports_summary.csv"


# ══════════════════════════════════════════════════════════════════════════════
#  PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline(
    base_dir: str   = BASE_DIR,
    output_dir: str = OUTPUT_DIR,
    csv_path: str   = CSV_PATH,
) -> None:
    """
    Iterate over every Month/Day subfolder under base_dir.
    Skip anything that is not a directory.
    Log and continue on per-day errors so one bad day does not abort the run.
    """
    if not os.path.isdir(base_dir):
        raise FileNotFoundError(
            f"Base directory not found: {base_dir}\n"
            "Check that BASE_DIR is correct and the drive is mounted."
        )

    os.makedirs(output_dir, exist_ok=True)

    processed = 0
    failed    = 0

    for month in sorted(os.listdir(base_dir)):
        month_path = os.path.join(base_dir, month)
        if not os.path.isdir(month_path):
            continue

        for day in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day)
            if not os.path.isdir(day_path):
                continue

            label = f"{month}/{day}"
            print(f"\n{'─'*55}")
            print(f"  Processing: {label}")
            print(f"{'─'*55}")

            try:
                # 1. Load & audit
                result = process_day(day_path)

                # 2. PDF
                pdf_filename = f"{month}_{day}.pdf"
                pdf_path     = os.path.join(output_dir, pdf_filename)
                create_pdf_report(result, pdf_path)

                # 3. CSV append
                write_summary_csv(result, csv_path)

                processed += 1

            except ValueError as exc:
                # Missing mandatory files — log and skip, not a crash
                print(f"  [SKIP] {label}: {exc}")
                failed += 1

            except Exception:
                # Unexpected error — print full traceback but keep going
                print(f"  [ERROR] {label} — unexpected failure:")
                traceback.print_exc()
                failed += 1

    print(f"\n{'='*55}")
    print(f"  Pipeline complete.")
    print(f"  Days processed : {processed}")
    print(f"  Days skipped   : {failed}")
    print(f"{'='*55}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run_pipeline()