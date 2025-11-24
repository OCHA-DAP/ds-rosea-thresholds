import argparse

import ocha_stratus as stratus
import pandas as pd
from dotenv import load_dotenv
from great_tables import GT

from src import listmonk, plot, utils
from src.constants import ISO3S
from src.datasources import asap, ipc

load_dotenv()
BLOB_LATEST_CSV = "ds-rosea-thresholds/monitoring/summary.csv"
BLOB_LATEST_TABLE = "ds-rosea-thresholds/monitoring/summary_table.png"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--force", action="store_true", help="Force update even if no changes detected"
    )
    args = parser.parse_args()

    print("Checking hotspots...")
    df_hs_raw = asap.get_hotspots(filter_countries=list(ISO3S.keys()))
    df_hs_classified = asap.classify_hotspots(df_hs_raw)
    df_hs_latest = asap.process_latest_hotspots(df_hs_classified)

    print("Checking IPC...")
    df_ipc_raw = ipc.get_reports(filter_iso3s=list(ISO3S.values()))
    df_ipc_classified = ipc.classify_reports(df_ipc_raw)
    df_ipc_latest = ipc.process_latest_ipc(df_ipc_classified)

    print("Merging all data")
    df_clean = utils.merge_ipc_hotspots(df_hs=df_hs_latest, df_ipc=df_ipc_latest)

    # Check the latest file
    df_latest = stratus.load_csv_from_blob(
        BLOB_LATEST_CSV,
        parse_dates=["ipc_end_date", "ipc_start_date", "hotspot_date"],
    )
    diff = df_clean.compare(df_latest)
    print(diff.to_string())

    if len(diff) != 0 or args.force:
        print("Alert! Writing new outputs...")
        if args.force:
            print("Forcing update...")

        df_table = df_clean.drop(df_clean.columns[-9:], axis=1)
        gt = plot.summary_table(df_table, diff if len(diff) != 0 else None)

        # Save the CSV in the date folder and as a summary file
        stratus.upload_csv_to_blob(df_clean, blob_name=BLOB_LATEST_CSV)
        stratus.upload_csv_to_blob(
            df_clean,
            blob_name=f"ds-rosea-thresholds/monitoring/{pd.Timestamp.now().strftime('%Y%m%d')}/summary.csv",
        )
        print("Updated files saved to blob! Sending emails...")
        gt_html = GT.as_raw_html(gt)
        body_content = listmonk.generate_rosea_content(gt_html)
        listmonk.send_rosea_campaign(body_content)
    else:
        print("No new changes detected! Keeping old summary file. No emails sent.")
