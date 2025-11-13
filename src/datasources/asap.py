import io
import zipfile

import pandas as pd
import requests

from src.constants import HIGH_CONSEC


def _classify_row(row):
    if row["hs_code"] == 0:
        return "low"
    elif (row["hs_code"] == 1) and (row["consecutive_count"] < HIGH_CONSEC):
        return "medium"
    elif (row["hs_code"] == 1) and (row["consecutive_count"] >= HIGH_CONSEC):
        return "high"
    elif row["hs_code"] == 2:
        return "very high"
    else:
        return None


def get_hotspots(filter_countries=None):
    url = "https://agricultural-production-hotspots.ec.europa.eu/files/hotspots_ts.zip"
    response = requests.get(url)
    df = pd.read_csv(
        zipfile.ZipFile(io.BytesIO(response.content)).open("hotspots_ts.csv"), sep=";"
    )
    if filter_countries:
        df = df[df["asap0_name"].isin(filter_countries)]
    df = df.sort_values(["asap0_name", "date"]).reset_index(drop=True)
    return df


def classify_hotspots(df):
    _df = df.copy()
    _df["consecutive_count"] = _df.groupby(
        ["asap0_id", (_df["hs_code"] != _df["hs_code"].shift()).cumsum()]
    )["hs_code"].cumcount()
    _df["alert_level"] = _df.apply(_classify_row, axis=1)
    _df = _df[["asap0_name", "date", "alert_level", "comment"]].rename(
        columns={"asap0_name": "country"}
    )
    return _df
