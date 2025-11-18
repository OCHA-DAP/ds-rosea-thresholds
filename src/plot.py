import pandas as pd
from great_tables import GT, html, md, style
from great_tables import loc as gt_loc


def ipc_table(df, title):
    cur = pd.Timestamp.now()
    return (
        GT(df, rowname_col="phase")
        .fmt_number(columns="population", decimals=0, use_seps=True)
        .fmt_number(columns="pt_change", decimals=0)
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
            pt_change=html("Percent change *"),
        )
        .tab_header(
            title=md(title),
        )
        .tab_source_note(
            source_note=html(
                f"""
                Updated as of {cur.strftime('%b %-d, %Y')}. Referencing the latest data
                that overlaps with the current date. See further details from the
                <a href='https://www.ipcinfo.org/ipc-country-analysis/en/'>IPC website
                </a>.<br><br> * A value of 0 may also indicate that no comparison
                could be made due to large differences in analyzed populations.
                """
            )
        )
    )


def summary_table(df):
    cur = pd.Timestamp.now()
    return (
        GT(
            df.drop(columns=["hotspot_comment", "iso3"]),
            rowname_col="country",
        )
        .cols_label(
            alert_level_hs=html("Hotspot Alert Level"),
            alert_level_ipc=html("IPC Alert Level"),
            max_alert_level=html("Maximum Alert Level"),
            ipc_type=html("IPC Projection"),
            ipc_end_date=html("IPC Period End Date"),
            ipc_start_date=html("IPC Period Start Date"),
            hotspot_date=html("Hotspot Date"),
        )
        .fmt_date(
            columns=["hotspot_date", "ipc_end_date", "ipc_start_date"],
            date_style="m_day_year",
        )
        .tab_style(
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
            source_note=html(f"Updated as of {cur.strftime('%b %-d, %Y')}.")
        )
    )
