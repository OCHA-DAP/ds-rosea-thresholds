from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from great_tables import GT

from src import listmonk, plot

load_dotenv()

DATA_DIR = Path(__file__).parent / "data"
CURRENT_CSV = DATA_DIR / "current.csv"
PREVIOUS_CSV = DATA_DIR / "previous.csv"

if __name__ == "__main__":
    df_current = pd.read_csv(
        CURRENT_CSV,
        parse_dates=["ipc_end_date", "ipc_start_date", "hotspot_date"],
    )

    # Compare with previous to highlight changes
    if PREVIOUS_CSV.exists():
        df_previous = pd.read_csv(
            PREVIOUS_CSV,
            parse_dates=["ipc_end_date", "ipc_start_date", "hotspot_date"],
        )
        diff = df_current.compare(df_previous)
    else:
        diff = None

    df_table = df_current.drop(df_current.columns[-9:], axis=1)
    gt = plot.summary_table(df_table, diff)

    gt_html = GT.as_raw_html(gt)
    body_content = listmonk.generate_rosea_content(gt_html)
    listmonk.send_rosea_campaign(body_content)
