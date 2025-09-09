import pandas as pd
import logging
import coloredlogs
from src.config import LOG_LEVEL
import requests
from dotenv import load_dotenv
import os

load_dotenv()


logger = logging.getLogger(__name__)
coloredlogs.install(level=LOG_LEVEL, logger=logger)


def get_ipc_from_hapi(iso3=None):
    endpoint = (
        "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-security"
    )
    params = {
        "app_identifier": os.getenv("HAPI_APP_IDENTIFIER"),
        "admin_level": 0,
        "output_format": "json",
        "limit": 100000,
        "offset": 0,
    }
    if iso3:
        params["location_code"] = iso3
    # Check if the request was successful
    response = requests.get(endpoint, params=params)
    json_data = response.json()
    # Extract the data list from the JSON
    data_list = json_data.get("data", [])
    df_response = pd.DataFrame(data_list)

    if df_response.empty:
        raise Exception(f"No data available for {iso3}")
    df_response["From"] = pd.to_datetime(df_response["reference_period_start"])
    df_response["To"] = pd.to_datetime(df_response["reference_period_end"])
    df_response["year"] = df_response["To"].dt.year
    return df_response.sort_values("reference_period_start", ascending=False)[
        [
            "location_code",
            "ipc_phase",
            "ipc_type",
            "population_in_phase",
            "population_fraction_in_phase",
            "From",
            "To",
            "year",
        ]
    ]


def combine_4_plus(df_all):
    df = df_all.copy()
    mapping = {"4": "4+", "5": "4+"}
    df["ipc_phase"] = df["ipc_phase"].map(mapping).fillna(df["ipc_phase"])
    dff = df[df.ipc_phase == "4+"]
    dff = (
        dff.groupby(["From", "To", "location_code", "ipc_type", "year", "ipc_phase"])
        .agg({"population_in_phase": "sum", "population_fraction_in_phase": "sum"})
        .reset_index()
    )
    return pd.concat([df_all, dff])