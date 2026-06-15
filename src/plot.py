import pandas as pd
from great_tables import GT, html, md, style
from great_tables import loc as gt_loc

_ALERT_COLORS = {
    "very high": "#8B0000",
    "high": "#DC143C",
    "medium": "#FF8C00",
    "low": "#2E8B57",
}


def _alert_badge(alert_level, bordered=False):
    if pd.isna(alert_level):
        return "—"
    color = _ALERT_COLORS.get(str(alert_level).lower(), "#999")
    label = str(alert_level).title()
    border = "border:1px solid white;box-shadow:0 0 0 3px black;" if bordered else ""
    return (
        f'<span style="background-color:{color};color:white;padding:2px 10px;'
        f'border-radius:4px;font-weight:bold;display:inline-block;{border}">{label}</span>'
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

    # Determine which cells changed so the badge border can be applied in HTML
    changed_rows_by_col = {}
    if changes_df is not None:
        for orig_col in ["max_alert_level", "alert_level_hs", "alert_level_ipc"]:
            if orig_col in changes_df.columns.get_level_values(0):
                mask = changes_df[orig_col].notna().any(axis=1)
                changed_rows_by_col[orig_col] = set(changes_df.index[mask].tolist())

    # Build combined HTML display columns (row.name is the original df index)
    def make_asap_cell(row):
        bordered = row.name in changed_rows_by_col.get("alert_level_hs", set())
        badge = _alert_badge(row["alert_level_hs"], bordered=bordered)
        if pd.isna(row["hotspot_date"]):
            return badge
        date_str = row["hotspot_date"].strftime("%-d %b %Y")
        sub_style = "display:block;color:#555;font-size:0.85em;margin-top:2px"
        return f'{badge}<span style="{sub_style}">{date_str}</span>'

    def make_ipc_cell(row):
        if pd.isna(row["alert_level_ipc"]):
            return "—"
        bordered = row.name in changed_rows_by_col.get("alert_level_ipc", set())
        badge = _alert_badge(row["alert_level_ipc"], bordered=bordered)
        parts = []
        if pd.notna(row["ipc_type"]):
            parts.append(str(row["ipc_type"]))
        if pd.notna(row["ipc_date_range"]):
            parts.append(str(row["ipc_date_range"]))
        sub = " · ".join(parts)
        sub_style = "display:block;color:#555;font-size:0.85em;margin-top:2px"
        if sub:
            return f'{badge}<span style="{sub_style}">{sub}</span>'
        return badge

    df_display["max_badge"] = df_display.apply(
        lambda row: _alert_badge(
            row["max_alert_level"],
            bordered=row.name in changed_rows_by_col.get("max_alert_level", set()),
        ),
        axis=1,
    )
    df_display["asap_display"] = df_display.apply(make_asap_cell, axis=1)
    df_display["ipc_display"] = df_display.apply(make_ipc_cell, axis=1)

    # Sort by decreasing max alert level
    _severity = {"very high": 3, "high": 2, "medium": 1, "low": 0}
    df_display["_sort_key"] = df_display["max_alert_level"].map(_severity).fillna(-1)
    df_display = df_display.sort_values("_sort_key", ascending=False).drop(
        columns=["_sort_key"]
    )

    gt = (
        GT(
            df_display[["country", "max_badge", "asap_display", "ipc_display"]],
            rowname_col="country",
        )
        .cols_label(
            max_badge=html("Max alert"),
            asap_display=html("ASAP hotspot"),
            ipc_display=html("IPC"),
        )
        .cols_width(max_badge="240px", asap_display="240px", ipc_display="240px")
        .tab_style(
            style=style.css("vertical-align: top;"),
            locations=gt_loc.body(),
        )
        .tab_style(
            style=style.css("vertical-align: top; padding-right: 1em;"),
            locations=gt_loc.stub(),
        )
        .tab_header(title=md("Alert status by country: ASAP + IPC"))
        .tab_style(
            style=style.css("text-align: left;"),
            locations=gt_loc.title(),
        )
        .tab_source_note(
            source_note=html(
                "↑ Alert level increased &nbsp;&nbsp;"
                "↓ Alert level decreased &nbsp;&nbsp;"
                '<span style="border:1px solid white;box-shadow:0 0 0 3px black;'
                'padding:0 4px;border-radius:4px">'
                "&nbsp;&nbsp;</span>"
                " Updated data"
            )
        )
        .tab_source_note(source_note=html(f"Updated {cur.strftime('%-d %b %Y')}."))
        .tab_style(
            style=style.css("padding-top: 8px;"),
            locations=gt_loc.source_notes(),
        )
        .tab_options(table_font_size="16px", stub_border_style="hidden")
    )

    return gt
