import os
from datetime import datetime
import pandas as pd
from queries.fetch_jobsteps import fetch_jobsteps_for_guid
from processing.transformer import (
    generate_report, _assign_rows_to_stages, _compute_timings,
)
from processing.html_report import generate_html_report
from db.connection import get_connection

INPUT_CSV = "input/job_guids.csv"
OUTPUT_DIR = "output"


def _create_run_folder():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    folder = os.path.join(OUTPUT_DIR, f"run_{timestamp}")
    os.makedirs(folder, exist_ok=True)
    return folder


def main():
    print("Agent started.")
    run_folder = _create_run_folder()
    print(f"Output folder: {run_folder}")

    input_df = pd.read_csv(INPUT_CSV)
    conn = get_connection()

    all_runs = []

    for _, row in input_df.iterrows():
        job_guid = row["job_guid"]
        case_count = int(row["case_count"])

        print(f"\nProcessing GUID: {job_guid} (cases: {case_count})")
        df = fetch_jobsteps_for_guid(conn, job_guid)

        if df.empty:
            print(f"  No data for GUID: {job_guid}")
            continue

        print(f"  Fetched {len(df)} rows.")

        # Generate individual Excel report
        xlsx_path = os.path.join(run_folder, f"report_{job_guid}.xlsx")
        generate_report(df, case_count, None, xlsx_path)

        # Collect data for comparative HTML report
        stages = _assign_rows_to_stages(df)
        timings, total_time, first_ts, final_ts = _compute_timings(df, stages)
        all_runs.append({
            "job_guid": job_guid,
            "case_count": case_count,
            "df": df,
            "timings": timings,
            "total_time": total_time,
            "first_ts": first_ts,
            "final_ts": final_ts,
        })

    conn.close()

    # Generate comparative HTML report
    if all_runs:
        html_path = os.path.join(run_folder, "comparison_report.html")
        generate_html_report(all_runs, html_path)

    print(f"\nAgent completed. Results saved to: {run_folder}")


if __name__ == "__main__":
    main()
