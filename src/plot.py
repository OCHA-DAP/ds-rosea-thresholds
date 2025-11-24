import pandas as pd
from great_tables import GT, html, md, px, style
from great_tables import loc as gt_loc


def ipc_table(df, title):
    cur = pd.Timestamp.now()
    return (
        GT(df, rowname_col="phase")
        .fmt_number(columns="population", decimals=0, use_seps=True)
        .fmt_number(columns="pt_change", decimals=0)
        .fmt_percent(columns="proportion", decimals=0)
        .data_color(columns=["population"], palette="Reds", domain=[0, 6000000])
        .data_color(columns=["proportion"], palette="Reds", domain=[0, 1])
        .data_color(
            columns=["pt_change"],
            palette=["#2ecc71", "#a6d96a", "#ffffff", "#f46d43", "#d73027"],
            domain=[-25, 25],
        )
        .cols_label(
            population=html("Population in phase"),
            proportion=html("Proportion of analyzed population"),
            pt_change=html("Percent point change *"),
        )
        .tab_header(
            title=md(title),
        )
        .tab_source_note(
            source_note=html(
                f"""
                Updated as of {cur.strftime("%-d %b %Y")}. Referencing the latest data
                that overlaps with the current date. See further details from the
                <a href='https://www.ipcinfo.org/ipc-country-analysis/en/'>IPC website
                </a>.<br><br> * Null values are present when the population analyzed in
                the previous IPC report is more than 10%
                different than the current report.
                """
            )
        )
    )


def summary_table(df, changes_df=None):
    cur = pd.Timestamp.now()

    df_display = df.copy()
    df_display["ipc_type"] = df["ipc_type"].replace(
        {
            "first projection": "1st projection",
            "second projection": "2nd projection",
        }
    )

    # Combine date columns into a range
    df_display["ipc_date_range"] = (
        df_display["ipc_start_date"].dt.strftime("%-d %b %Y")
        + " - "
        + df_display["ipc_end_date"].dt.strftime("%-d %b %Y")
    )

    # Add direction column
    if changes_df is not None:
        severity_order = {"low": 0, "medium": 1, "high": 2, "very high": 3}

        for idx in changes_df.index:
            # Get first alert column that changed
            alert_cols = ["max_alert_level", "alert_level_hs", "alert_level_ipc"]
            for col in alert_cols:
                if col in changes_df.columns:
                    old_val = changes_df.loc[idx, (col, "other")]
                    new_val = changes_df.loc[idx, (col, "self")]
                    try:
                        if severity_order[old_val] < severity_order[new_val]:
                            df_display.loc[idx, "country"] += (
                                ' <span style="color: red;">▲</span>'
                            )
                        elif severity_order[old_val] > severity_order[new_val]:
                            df_display.loc[idx, "country"] += (
                                ' <span style="color: green;">▼</span>'
                            )
                    except Exception as e:
                        print(f"Error: {e}")
                        break
                    break

    gt = (
        GT(
            df_display.drop(
                columns=["hotspot_comment", "iso3", "ipc_start_date", "ipc_end_date"]
            ),
            rowname_col="country",
        )
        .cols_label(
            alert_level_hs=html("Hotspot Alert Level"),
            alert_level_ipc=html("IPC Alert Level"),
            max_alert_level=html("Maximum Alert Level"),
            ipc_type=html("IPC Projection"),
            ipc_date_range=html("IPC Period"),
            hotspot_date=html("Hotspot Date"),
        )
        .fmt_date(
            columns=["hotspot_date", "ipc_end_date", "ipc_start_date"],
            date_style="day_m_year",
        )
    )

    # Highlight changed cells
    if changes_df is not None:
        for col in changes_df.columns.get_level_values(0).unique():
            if col in df.columns:
                # Only highlight rows where at least one value is not NaN
                mask = changes_df[col].notna().any(axis=1)
                rows_to_highlight = changes_df.index[mask].tolist()

                if rows_to_highlight:
                    gt = gt.tab_style(
                        style=[
                            style.borders(
                                sides="all", color="black", style="solid", weight=px(3)
                            )
                        ],
                        locations=gt_loc.body(columns=[col], rows=rows_to_highlight),
                    )

    # Alert level colors and header
    gt = (
        gt.tab_style(
            style=[style.fill(color="#8B0000"), style.text(color="white")],
            locations=gt_loc.body(
                columns=["max_alert_level"],
                rows=lambda df_table: (df_table["max_alert_level"] == "very high"),
            ),
        )
        .tab_style(
            style=[style.fill(color="#DC143C"), style.text(color="white")],
            locations=gt_loc.body(
                columns=["max_alert_level"],
                rows=lambda df_table: (df_table["max_alert_level"] == "high"),
            ),
        )
        .tab_style(
            style=[style.fill(color="#FF8C00"), style.text(color="white")],
            locations=gt_loc.body(
                columns=["max_alert_level"],
                rows=lambda df_table: (df_table["max_alert_level"] == "medium"),
            ),
        )
        .tab_style(
            style=[style.fill(color="#2E8B57"), style.text(color="white")],
            locations=gt_loc.body(
                columns=["max_alert_level"],
                rows=lambda df_table: (df_table["max_alert_level"] == "low"),
            ),
        )
        .tab_header(
            title=md("Alert Summary, Per Country. Combined Hotspot and IPC Alerts."),
        )
        .tab_source_note(
            source_note=html(f"Updated as of {cur.strftime('%-d %b %Y')}.")
        )
        .tab_options(
            table_font_size="16px",
        )
    )

    return gt
