import pandas as pd
from great_tables import GT, html, md, px, style
from great_tables import loc as gt_loc

_ALERT_COLORS = {
    "very high": "#8B0000",
    "high": "#DC143C",
    "medium": "#FF8C00",
    "low": "#2E8B57",
}


def _alert_badge(alert_level):
    if pd.isna(alert_level):
        return "—"
    color = _ALERT_COLORS.get(str(alert_level).lower(), "#999")
    label = str(alert_level).title()
    return (
        f'<span style="background-color:{color};color:white;padding:2px 10px;'
        f'border-radius:4px;font-weight:bold;display:inline-block">{label}</span>'
    )


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
                </a>.<br><br> * A value of 0 indicates no change from the previous
                report. Null values indicate the comparison could not be made
                because the two reports analyzed significantly different
                population sizes (>10% difference).
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
        df_display["ipc_start_date"].dt.strftime("%-d %b")
        + " – "
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
                            df_display.loc[idx, "country"] += " ↑"
                        elif severity_order[old_val] > severity_order[new_val]:
                            df_display.loc[idx, "country"] += " ↓"
                    except KeyError as e:
                        print(f"No change in alert level for {idx}: {e}")
                        break
                    break

    # Build combined HTML display columns
    def make_asap_cell(row):
        badge = _alert_badge(row["alert_level_hs"])
        if pd.isna(row["hotspot_date"]):
            return badge
        date_str = row["hotspot_date"].strftime("%-d %b %Y")
        return f'{badge}<br><span style="color:#555;font-size:0.85em">{date_str}</span>'

    def make_ipc_cell(row):
        if pd.isna(row["alert_level_ipc"]):
            return "—"
        badge = _alert_badge(row["alert_level_ipc"])
        parts = []
        if pd.notna(row["ipc_type"]):
            parts.append(str(row["ipc_type"]))
        if pd.notna(row["ipc_date_range"]):
            parts.append(str(row["ipc_date_range"]))
        sub = " · ".join(parts)
        if sub:
            return f'{badge}<br><span style="color:#555;font-size:0.85em">{sub}</span>'
        return badge

    df_display["max_badge"] = df_display["max_alert_level"].apply(_alert_badge)
    df_display["asap_display"] = df_display.apply(make_asap_cell, axis=1)
    df_display["ipc_display"] = df_display.apply(make_ipc_cell, axis=1)

    gt = GT(
        df_display[["country", "max_badge", "asap_display", "ipc_display"]],
        rowname_col="country",
    ).cols_label(
        max_badge=html("Max alert"),
        asap_display=html("ASAP hotspot"),
        ipc_display=html("IPC"),
    )

    # Highlight changed cells
    col_map = {
        "max_alert_level": "max_badge",
        "alert_level_hs": "asap_display",
        "alert_level_ipc": "ipc_display",
    }
    if changes_df is not None:
        for orig_col in changes_df.columns.get_level_values(0).unique():
            display_col = col_map.get(orig_col)
            if display_col:
                mask = changes_df[orig_col].notna().any(axis=1)
                rows_to_highlight = changes_df.index[mask].tolist()

                if rows_to_highlight:
                    gt = gt.tab_style(
                        style=[
                            style.borders(
                                sides="all", color="black", style="solid", weight=px(3)
                            )
                        ],
                        locations=gt_loc.body(
                            columns=[display_col], rows=rows_to_highlight
                        ),
                    )

    gt = (
        gt.tab_header(title=md("Alert status by country: ASAP + IPC"))
        .tab_source_note(
            source_note=html(
                "↑ Alert level increased &nbsp;&nbsp;"
                "↓ Alert level decreased &nbsp;&nbsp;"
                '<span style="border:2px solid black;padding:0 4px">&nbsp;&nbsp;</span>'
                " Updated data"
            )
        )
        .tab_source_note(source_note=html(f"Updated {cur.strftime('%-d %b %Y')}."))
        .tab_options(table_font_size="16px")
    )

    return gt
