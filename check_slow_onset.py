import argparse
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from great_tables import GT

from src import listmonk, plot, utils
from src.constants import ISO3S
from src.datasources import asap, ipc

load_dotenv()

# Local data paths
DATA_DIR = Path(__file__).parent / "data"
CURRENT_CSV = DATA_DIR / "current.csv"
PREVIOUS_CSV = DATA_DIR / "previous.csv"

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

    # Ensure data directory exists
    DATA_DIR.mkdir(exist_ok=True)

    # Check the current file for comparison
    if CURRENT_CSV.exists():
        df_latest = pd.read_csv(
            CURRENT_CSV,
            parse_dates=["ipc_end_date", "ipc_start_date", "hotspot_date"],
        )
        diff = df_clean.compare(df_latest)
        print(diff.to_string())
        has_changes = len(diff) != 0
    else:
        print("No existing current.csv found - treating as first run")
        diff = None
        has_changes = True

    if has_changes or args.force:
        print("Alert! Writing new outputs...")
        if args.force:
            print("Forcing update...")

        df_table = df_clean.drop(df_clean.columns[-9:], axis=1)
        gt = plot.summary_table(df_table, diff)

        # Rotate current.csv to previous.csv, then save new current.csv
        if CURRENT_CSV.exists():
            CURRENT_CSV.rename(PREVIOUS_CSV)
        df_clean.to_csv(CURRENT_CSV, index=False)
        print("Updated files saved locally! Sending emails...")
        gt_html = GT.as_raw_html(gt)
        body_content = listmonk.generate_rosea_content(gt_html)
        listmonk.send_rosea_campaign(body_content)
    else:
        print("No new changes detected! Keeping old summary file. No emails sent.")
