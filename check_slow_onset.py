import argparse
import tempfile
from pathlib import Path

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv

from src import plot, utils
from src.constants import ISO3S
from src.datasources import asap, ipc

load_dotenv()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force", action="store_true", help="Force update even if no changes detected"
    )
    args = parser.parse_args()

    print("Checking hotspots...")
    df_hs_raw = asap.get_hotspots(filter_countries=list(ISO3S.keys()))
    df_hs_classified = asap.classify_hotspots(df_hs_raw)
    df_hs_latest = asap.proccess_latest_hotspots(df_hs_classified)

    print("Checking IPC...")
    df_ipc_raw = ipc.get_reports(filter_iso3s=list(ISO3S.values()))
    df_ipc_classified = ipc.classify_reports(df_ipc_raw)
    df_ipc_latest = ipc.process_latest_ipc(df_ipc_classified)

    print("Merging all data")
    df_clean = utils.merge_ipc_hotspots(df_hs=df_hs_latest, df_ipc=df_ipc_latest)

    # Check the latest file
    df_latest = stratus.load_csv_from_blob(
        "ds-rosea-thresholds/monitoring/summary.csv",
        parse_dates=["ipc_end_date", "ipc_start_date", "hotspot_date"],
    )
    diff = df_clean.compare(df_latest)
    print(diff.to_string())

    if len(diff) != 0 or args.force:
        print("Alert! Writing new outputs...")
        if args.force:
            print("Forcing update...")
        # Create the table and save as an image...
        with tempfile.TemporaryDirectory() as temp_dir:
            df_table = df_clean.drop(df_clean.columns[-9:], axis=1)
            gt = plot.summary_table(df_table, diff if len(diff) != 0 else None)
            output_path = Path(temp_dir) / "tmp.png"
            gt.save(output_path, scale=4, window_size=[4000, 8000])
            # Save as the summary file
            with open(output_path, "rb") as data:
                stratus.upload_blob_data(
                    data, "ds-rosea-thresholds/monitoring/summary_table.png"
                )

        # Save the CSV in the date folder and as a summary file
        stratus.upload_csv_to_blob(
            df_clean, blob_name="ds-rosea-thresholds/monitoring/summary.csv"
        )
        stratus.upload_csv_to_blob(
            df_clean,
            blob_name=f"ds-rosea-thresholds/monitoring/{pd.Timestamp.now().strftime('%Y%m%d')}/summary.csv",
        )
        print("Updated files saved to blob!")
    else:
        print("No new changes detected! Keeping old summary file.")
