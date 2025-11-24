import os

import numpy as np
import pandas as pd
import requests

from src.constants import (
    H_D_IN_3,
    H_D_PR_3,
    H_S_POP_4,
    M_POP_4,
    M_PR_3,
    POP_THRESH,
    VH_D_IN_4,
    VH_D_PR_3,
    VH_S_POP_4,
)


def get_reports(filter_iso3s=None):
    endpoint = (
        "https://hapi.humdata.org/api/v2/food-security-nutrition-poverty/food-security"
    )
    params = {
        "app_identifier": os.getenv("HAPI_APP_IDENTIFIER"),
        "admin_level": 0,
        "output_format": "json",
        "limit": 10000,
        "offset": 0,
    }
    all_data = []
    while True:
        response = requests.get(endpoint, params=params)
        data_list = response.json().get("data", [])
        if not data_list:
            break
        all_data.extend(data_list)
        params["offset"] += params["limit"]

    df_response = pd.DataFrame(all_data)

    df_response["From"] = pd.to_datetime(df_response["reference_period_start"])
    df_response["To"] = pd.to_datetime(df_response["reference_period_end"])
    df_response["year"] = df_response["To"].dt.year
    if filter_iso3s:
        df_response = df_response[df_response.location_code.isin(filter_iso3s)]
    return df_response.sort_values("reference_period_start", ascending=False)[
        [
            "location_code",
            "ipc_phase",
            "ipc_type",
            "population_fraction_in_phase",
            "population_in_phase",
            "From",
            "To",
            "year",
        ]
    ]


def classify_reports(df):
    _df = df.copy()
    _df = _combine_4_plus(_df)
    _df = _get_pop_analyzed(_df)
    _df = _transform_wide(_df)
    _df["cat"] = _df.apply(_classify_row, axis=1)
    split_categories = _df["cat"].str.split(" - ", expand=True)
    _df["alert_level"] = split_categories[0]
    _df["alert_level_detail"] = split_categories[1]
    _df = _df.drop("cat", axis="columns")
    return _df


def _get_pop_analyzed(df):
    _df = df.copy()
    pop_all = _df[_df["ipc_phase"] == "all"].set_index(
        ["location_code", "ipc_type", "From", "To"]
    )["population_in_phase"]
    _df["population_analyzed"] = (
        _df.set_index(["location_code", "ipc_type", "From", "To"])
        .index.map(pop_all)
        .values
    )
    return _df


def _combine_4_plus(df):
    _df = df.copy()
    mapping = {"4": "4+", "5": "4+"}
    _df["ipc_phase"] = df["ipc_phase"].map(mapping).fillna(_df["ipc_phase"])
    dff = _df[_df.ipc_phase == "4+"]
    dff = (
        dff.groupby(
            [
                "From",
                "To",
                "location_code",
                "ipc_type",
                "year",
                "ipc_phase",
            ]
        )
        .agg(
            {
                "population_fraction_in_phase": "sum",
                "population_in_phase": "sum",
            }
        )
        .reset_index()
    )
    return pd.concat([df, dff])


def _classify_row(row):
    P3 = row.get("proportion_3+", np.nan)
    PP4 = row.get("population_4+", np.nan)
    D3 = row.get("pt_change_3+", np.nan)
    D4 = row.get("pt_change_4+", np.nan)

    very_high_severe = PP4 >= VH_S_POP_4
    very_high_deteriorating = P3 >= VH_D_PR_3 and (
        pd.notna(D4) and D4 >= VH_D_IN_4 * 100
    )
    high_severe = PP4 >= H_S_POP_4
    high_deteriorating = P3 >= H_D_PR_3 and (pd.notna(D3) and D3 >= H_D_IN_3 * 100)
    medium = (P3 >= M_PR_3) or (PP4 >= M_POP_4)

    if very_high_severe and very_high_deteriorating:
        return "very high - all criteria"
    elif very_high_severe:
        return "very high - emergency"
    elif very_high_deteriorating:
        return "very high - deteriorating"
    elif high_severe and high_deteriorating:
        return "high - all criteria"
    elif high_severe:
        return "high - emergency"
    elif high_deteriorating:
        return "high - deteriorating"
    elif medium:
        return "medium"
    return "low"


def _transform_wide(df):
    _df = df.copy()
    _df = _df[_df.ipc_phase.isin(["3+", "4+", "5"])]
    _df = _df.sort_values(["location_code", "From"], ascending=True)

    index_cols = [
        "location_code",
        "ipc_type",
        "population_analyzed",
        "From",
        "To",
        "year",
    ]
    value_cols = ["population_fraction_in_phase", "population_in_phase"]
    df_all_wide = _df.pivot(
        index=index_cols, columns="ipc_phase", values=value_cols
    ).reset_index()
    df_all_wide = df_all_wide.sort_values(["location_code", "From"])

    # Check that it's one third the length,
    # because we made the 3+, 4+, and 5 categories wide
    assert len(df_all_wide) == (len(_df) / 3)

    df_all_wide["pop_comparable"] = (
        abs(df_all_wide.groupby("location_code")["population_analyzed"].diff())
        / df_all_wide.groupby("location_code")["population_analyzed"].shift()
        <= POP_THRESH
    )

    df_all_wide.columns = ["_".join(col).strip("_") for col in df_all_wide.columns]

    # Calculate percentage point change only when comparable
    df_all_wide["pt_change_3+"] = (
        df_all_wide.groupby("location_code")["population_fraction_in_phase_3+"].diff()
        * 100
    )
    df_all_wide["pt_change_4+"] = (
        df_all_wide.groupby("location_code")["population_fraction_in_phase_4+"].diff()
        * 100
    )

    # Set to NaN where populations aren't comparable
    df_all_wide.loc[
        ~df_all_wide["pop_comparable"], ["pt_change_3+", "pt_change_4+"]
    ] = None

    df_all_wide.rename(
        columns={
            "population_fraction_in_phase_3+": "proportion_3+",
            "population_fraction_in_phase_4+": "proportion_4+",
            "population_fraction_in_phase_5": "proportion_5",
            "population_in_phase_3+": "population_3+",
            "population_in_phase_4+": "population_4+",
            "population_in_phase_5": "population_5",
        },
        inplace=True,
    )

    for col in [
        "population_3+",
        "population_4+",
        "pt_change_3+",
        "pt_change_4+",
        "population_5",
    ]:
        df_all_wide[col] = (
            pd.to_numeric(df_all_wide[col], errors="coerce").round().astype("Int64")
        )

    # df_all_wide["pt_change_3+"] = df_all_wide["pt_change_3+"].fillna(0)
    # df_all_wide["pt_change_4+"] = df_all_wide["pt_change_4+"].fillna(0)
    return df_all_wide


def process_latest_ipc(df):
    _df = df.copy()
    cur = pd.Timestamp.now()
    _df = _df[_df.To > cur]
    priority_order = ["current", "first projection", "second projection"]
    _df["ipc_type_cat"] = pd.Categorical(
        _df["ipc_type"], categories=priority_order, ordered=True
    )
    df_ipc_latest_filt = (
        _df.sort_values(["location_code", "ipc_type_cat"])
        .drop_duplicates(subset=["location_code"], keep="first")
        .drop(columns=["ipc_type_cat"])
    )  # Remove the helper column

    assert _df.location_code.nunique() == len(df_ipc_latest_filt)
    print(f"Latest IPC reports from {_df.location_code.nunique()} countries")
    return df_ipc_latest_filt
