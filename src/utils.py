def merge_ipc_hotspots(df_ipc, df_hs):
    df_merged = df_hs.merge(
        df_ipc,
        suffixes=["_hs", "_ipc"],
        on="location_code",
        how="outer",
    )
    assert len(df_merged) == max(len(df_hs), len(df_ipc))

    alert_map = {"low": 1, "medium": 2, "high": 3, "very high": 4}
    reverse_map = {1: "low", 2: "medium", 3: "high", 4: "very high"}

    # Map to numeric, find max, then map back
    df_merged["max_alert_level"] = (
        df_merged[["alert_level_hs", "alert_level_ipc"]]
        .apply(lambda x: x.map(alert_map))
        .max(axis=1, skipna=True)
        .map(reverse_map)
    )

    df_clean = df_merged[
        [
            "country",
            "location_code",
            "max_alert_level",
            "alert_level_hs",
            "alert_level_ipc",
            "ipc_type",
            "date",
            "From",
            "To",
            "comment",
            "alert_level_detail",
            "proportion_3+",
            "proportion_4+",
            "proportion_5",
            "population_3+",
            "population_4+",
            "population_5",
            "pt_change_3+",
            "pt_change_4+",
        ]
    ]

    df_clean = df_clean.rename(
        columns={
            "date": "hotspot_date",
            "comment": "hotspot_comment",
            "From": "ipc_start_date",
            "To": "ipc_end_date",
            "location_code": "iso3",
        }
    )
    return df_clean
