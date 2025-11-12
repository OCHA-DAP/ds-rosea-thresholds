import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium", app_title="ROSEA ASAP Thresholds")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    img1 = mo.image(
        src="assets/UNOCHA_logo_horizontal_blue_CMYK.png",
        height=100,
    ).center()

    img2 = mo.image(
        src="assets/centre_logo.png",
        height=100,
    ).center()

    mo.hstack([img1, img2])
    return


@app.cell
def _(mo):
    mo.center(mo.md("# ROSEA ASAP Threshold Exploration"))
    return


@app.cell
def _():
    import ocha_stratus as stratus
    from dotenv import load_dotenv, find_dotenv
    import plotly.express as px
    import pandas as pd
    import requests
    import zipfile
    import io
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    import numpy as np
    from datetime import datetime

    _ = load_dotenv(find_dotenv(usecwd=True))

    iso3s = {
        "Angola": "AGO",
        "Burundi": "BDI",
        "Comoros": "COM",
        "Djibouti": "DJI",
        "Eswatini": "SWZ",
        "Kenya": "KEN",
        "Lesotho": "LSO",
        "Madagascar": "MDG",
        "Malawi": "MWI",
        "Namibia": "NAM",
        "Rwanda": "RWA",
        "Tanzania": "TZA",
        "Uganda": "UGA",
        "Zambia": "ZMB",
        "Zimbabwe": "ZWE",
    }
    iso3_to_country = {v: k for k, v in iso3s.items()}

    color_map = {
        'Major hotspot': 'darkred',
        'Hotspot': 'red', 
        'No hotspot': '#b0b0b0'
    }
    return (
        color_map,
        datetime,
        go,
        io,
        iso3s,
        make_subplots,
        pd,
        px,
        requests,
        stratus,
        zipfile,
    )


@app.cell
def _(io, mo, pd, requests, stratus, zipfile):
    @mo.cache
    def get_asap_hotspots(filter_iso3s=None):
        url = "https://agricultural-production-hotspots.ec.europa.eu/files/hotspots_ts.zip"
        response = requests.get(url)
        df = pd.read_csv(zipfile.ZipFile(io.BytesIO(response.content)).open('hotspots_ts.csv'), sep=';')
        if filter_iso3s:
            df = df[df["asap0_name"].isin(filter_iso3s)]
        df = df.sort_values(['asap0_name', 'date']).reset_index(drop=True)
        return df

    @mo.cache
    def get_asap_warnings_from_blob():
        df_warnings = stratus.load_csv_from_blob("ds-rosea-thresholds/processed/asap/warnings_filtered.csv", sep=";")
        remap_values = {
            0: 0, 10: 0, 20: 0,  # No warnings
            2: 1, 3: 1, 4: 1, 12: 1, 13:1, 14:1,  # Warning level 1
            5: 2,  # Warning level 2
            6: 3, 7: 3, 8:3,  # Warning level 3
            9: 4,  # Warning level 4
            98: -1, 99: -1  # Out of season
        }

        df_warnings["warning_val_crop"] = df_warnings["w_crop"].map(remap_values)
        df_warnings["warning_val_range"] = df_warnings["w_range"].map(remap_values)
        return df_warnings
    return get_asap_hotspots, get_asap_warnings_from_blob


@app.cell
def _(get_asap_hotspots, get_asap_warnings_from_blob, iso3s):
    df_hotspots = get_asap_hotspots(list(iso3s.keys()))
    df_warnings = get_asap_warnings_from_blob()
    return df_hotspots, df_warnings


@app.cell
def _(mo):
    mo.md(r"""## 1. Hotspot overview per country""")
    return


@app.cell
def _(color_map, df_hotspots, px):
    df_bar_plot = df_hotspots.groupby(['asap0_name', 'hs_name']).size().reset_index(name='count')
    _fig = px.bar(df_bar_plot, 
                 x='asap0_name', 
                 y='count', 
                 color='hs_name',
                 color_discrete_map=color_map,
                 category_orders={'hs_name': ['Major hotspot', 'Hotspot', 'No hotspot']},
                 title='Countries by Hotspot Status')
    _fig.update_layout(xaxis_tickangle=-45, template="simple_white")
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## 2. Hotspots and Warnings comparison

    ** Note: We're using a cached version of the Warnings!
    """
    )
    return


@app.cell
def _(df_warnings, mo):
    country_dropdown = mo.ui.dropdown(label="Select a country", options=sorted(list(df_warnings.asap0_name.unique())), value="Kenya")
    country_dropdown
    return (country_dropdown,)


@app.cell
def _(country_dropdown, df_hotspots, df_warnings):
    df_warnings_ = df_warnings[df_warnings.asap0_name == country_dropdown.value]
    df_hotspots_ = df_hotspots[df_hotspots.asap0_name == country_dropdown.value]
    return df_hotspots_, df_warnings_


@app.cell
def _(df_hotspots_, mo):
    date_dropdown = mo.ui.dropdown(label="Select a hotspot date", options=df_hotspots_.date.unique())
    date_dropdown
    return (date_dropdown,)


@app.cell
def _(date_dropdown, datetime):
    date_formatted = datetime.strptime(date_dropdown.value, "%Y-%m-%d").strftime("%B %d, %Y") if date_dropdown.value else None
    return (date_formatted,)


@app.cell
def _(mo):
    type_radio = mo.ui.radio(label="Warnings to display", options=["Croplands", "Rangelands"], inline=True, value="Croplands")
    type_radio
    return (type_radio,)


@app.cell
def _(date_dropdown, date_formatted, df_hotspots_, mo):
    mo.stop(date_dropdown.value is None, mo.callout("Select a date above for a summary of hotspot conditions from ASAP."))
    hs_name = list(df_hotspots_[df_hotspots_.date == date_dropdown.value].hs_name)[0]
    hs_comment = list(df_hotspots_[df_hotspots_.date == date_dropdown.value].comment)[0]

    mo.callout(
        mo.md(
            f"""
            ## **{hs_name}**: {date_formatted}
            {hs_comment}
            """
        )
    )
    return


@app.cell
def _(
    color_map,
    country_dropdown,
    date_dropdown,
    date_formatted,
    df_hotspots_,
    df_warnings_,
    go,
    make_subplots,
    type_radio,
):
    cmap = {
        -1: "white",
        0: "green",
        1: "yellow",
        2: "orange", 
        3: "red",
        4: "darkred"
    }
    normalized_scale = [(i/(len(cmap)-1), color) for i, color in enumerate(cmap.values())]

    warning_col = "warning_val_crop" if type_radio.value=="Croplands" else "warning_val_range"
    df_warnings_pivot = df_warnings_.pivot(index='asap2_name', columns='date', values=warning_col)
    df_hotspots_pivot = df_hotspots_.pivot(index='asap0_name', columns='date', values='hs_code')

    _fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.9, 0.1])

    _fig.add_trace(go.Heatmap(
        z=df_warnings_pivot.values,
        x=df_warnings_pivot.columns,
        y=df_warnings_pivot.index,
        showscale=False,
        colorscale=normalized_scale,
        zmin=-1,
        zmax=4,
    ), row=1, col=1)

    _fig.add_trace(go.Heatmap(
        z=df_hotspots_pivot.values,
        x=df_hotspots_pivot.columns,
        y=df_hotspots_pivot.index,
        showscale=False,
        colorscale=[[0, color_map["No hotspot"]], [0.5, color_map["Hotspot"]], [1, color_map["Major hotspot"]]],
        hovertemplate='%{y}<br>%{x}<br>Value: %{z}<br>' +
                      '<extra></extra>',
        zmin=0,
        zmax=2
    ), row=2, col=1)


    _fig.update_layout(
        title=f"ASAP Warnings ({type_radio.value}) and Hotspot Data for {country_dropdown.value}",
        margin={'l': 0, 'r': 0, 't': 30, 'b': 0},
        template="simple_white"
    )
    _fig.update_xaxes(showspikes=True, spikecolor="black", spikethickness=1, spikemode='across')

    # Highlight the date if selected
    if date_dropdown.value:
        _fig.add_vline(
            x=date_dropdown.value, 
            line_width=4, 
            line_color="white",
            opacity=1,
        )
        _fig.add_vline(
            x=date_dropdown.value, 
            line_width=2, 
            line_color="black",
            opacity=1.0,
        )
        _fig.add_annotation(
            x=date_dropdown.value,
            y=.15,
            text=f"Selected: {date_formatted}",
            showarrow=False,
            xref="x",
            yref="paper",
            borderwidth=1
        )

    _fig
    return


@app.cell
def _(mo):
    mo.md(r"""## 3. Validate define alert levels""")
    return


@app.cell
def _(df_hotspots):
    df_hotspots['consecutive_count'] = df_hotspots.groupby(['asap0_id', (df_hotspots['hs_code'] != df_hotspots['hs_code'].shift()).cumsum()])['hs_code'].cumcount()
    return


@app.cell
def _(df_hotspots):
    def classify_row(row):
        if (row["hs_code"] == 0) and (row["consecutive_count"] == 0):
            return "low"
        elif (row["hs_code"] == 1) and (row["consecutive_count"] == 0):
            return "medium"
        elif (row["hs_code"] == 1) and (row["consecutive_count"] == 3):
            return "high"
        elif (row["hs_code"] == 2) and (row["consecutive_count"] == 0):
            return "very high"
        else:
            return None


    df_hotspots["alert_level"] = df_hotspots.apply(classify_row, axis=1)
    return


@app.cell
def _():
    # Formatting information for plots
    level_colors = {
        "very high": "#e8857d",  # Muted red
        "high": "#d19970",  # Muted orange
        "medium": "#6b9ce8",  # Muted teal
        "low": "#b0b0b0",  # Muted grey
    }
    cerf_color = "#393D3F"
    fa_color = "#393D3F"

    surge_formatting = {
        "Physical": {"line": None, "pos": 0.9, "color": "#1ebfb3"},
        "Remote": {"line": None, "pos": 0.8, "color": "#5f1ebf"},
    }

    shape_config = {
        "emergency": {"symbol": "diamond", "line_width": 0},
        "deteriorating": {"symbol": "diamond-open", "line_width": 2},
        "both": {"symbol": "circle", "line_width": 0},
        None: {"symbol": "circle", "line_width": 0},
    }
    return cerf_color, fa_color, level_colors, surge_formatting


@app.cell
def _(iso3s, pd, stratus):
    # Clean surge data
    df_ = stratus.load_csv_from_blob("ds-rosea-thresholds/rosea_surge_20202024.csv")
    df_clean = df_.copy()
    df_clean["start_date"] = pd.to_datetime(
        df_clean["Date of departure"], format="mixed", dayfirst=True
    )
    df_clean["end_date"] = pd.to_datetime(
        df_clean["Date of return"], format="mixed", dayfirst=True
    )
    df_clean["country"] = df_clean["Destination Country"]
    df_clean["event_type"] = "ROSEA_" + df_clean["Type"].fillna("Response")
    df_clean["value"] = df_clean["Days"]  # Use mission duration as value
    df_clean["source"] = "ROSEA"
    df_clean["surge_type"] = df_clean["Surge"]
    df_clean["location_code"] = df_clean["country"].map(iso3s)
    df_surge = df_clean[
        ["country", "surge_type", "start_date", "end_date", "location_code"]
    ]

    # Clean flash appeal data
    df_fa = stratus.load_csv_from_blob("ds-rosea-thresholds/flash_appeals.csv")
    df_fa["date"] = pd.to_datetime(
        df_fa[" Original PDF Publication Date "], dayfirst=True
    )
    df_fa.rename(
        columns={"Country Name": "country", " Final Requirements": "requirements"},
        inplace=True,
    )
    df_fa["location_code"] = df_fa["country"].map(iso3s)
    df_fa = df_fa[["country", "date", "requirements", "location_code"]]

    # Clean the cerf data
    df_cerf = stratus.load_csv_from_blob("ds-rosea-thresholds/cerf.csv")
    df_cerf["regionName_l"] = (
        df_cerf["regionName"].astype(str).str.strip().str.casefold()
    )
    df_cerf["emergencyTypeName_l"] = (
        df_cerf["emergencyTypeName"].astype(str).str.strip().str.casefold()
    )
    df_cerf["window_l"] = (
        df_cerf["windowFullName"].astype(str).str.strip().str.casefold()
    )
    df_cerf["dateUSGSignature"] = pd.to_datetime(
        df_cerf["dateUSGSignature"], errors="coerce"
    )
    df_cerf["totalAmountApproved"] = pd.to_numeric(
        df_cerf["totalAmountApproved"], errors="coerce"
    ).fillna(0)
    region_keep = {"eastern africa", "southern africa"}
    mask = df_cerf["emergencyTypeName_l"].eq("drought") & df_cerf["regionName_l"].isin(
        region_keep
    )
    df_cerf = df_cerf.loc[mask].dropna(subset=["dateUSGSignature"]).copy()
    df_cerf.rename(
        columns={
            "countryCode": "location_code",
            "countryName": "country",
            "dateUSGSignature": "date",
        },
        inplace=True,
    )
    df_cerf = df_cerf.replace("United Republic of Tanzania", "Tanzania")
    df_cerf = df_cerf[df_cerf["year"] >= 2017]
    return df_cerf, df_fa, df_surge


@app.cell
def _(mo):
    category_multiselect = mo.ui.multiselect(
        options=["very high", "high", "medium", "low"],
        label="Select category to investigate:",
        value=["very high"],
    )

    display_surge = mo.ui.switch(label="Overlay validation points", value=False)
    mo.hstack([category_multiselect, display_surge])
    return category_multiselect, display_surge


@app.cell
def _(category_multiselect, df_hotspots, pd):
    # Remove rows that didn't alert
    df_summary_sel = df_hotspots[df_hotspots.alert_level.notnull()].copy()

    # Get to and from dates
    df_summary_sel['date'] = pd.to_datetime(df_summary_sel['date'])
    df_summary_sel['From'] = df_summary_sel['date']
    df_summary_sel['To'] = df_summary_sel.groupby('asap0_id')['date'].shift(-1) - pd.Timedelta(days=1)
    df_summary_sel['To'] = df_summary_sel['To'].fillna(pd.to_datetime(df_hotspots.date.max()))

    df_save = df_summary_sel
    df_summary_sel = df_summary_sel[df_summary_sel.alert_level.isin(category_multiselect.value)]
    return df_save, df_summary_sel


@app.cell
def _(
    category_multiselect,
    cerf_color,
    df_cerf,
    df_fa,
    df_hotspots,
    df_summary_sel,
    df_surge,
    display_surge,
    fa_color,
    go,
    level_colors,
    pd,
    surge_formatting,
):
    # -----------------------------------------
    # Create plot and setup

    end_date = pd.Timestamp("2027-10-31")
    start_date = pd.Timestamp("2016-01-01")

    fig = go.Figure()
    all_countries = df_hotspots.asap0_name.unique()

    # Create position mapping for ALL countries
    country_positions = {country: i for i, country in enumerate(sorted(all_countries))}
    countries = sorted(all_countries)  # Use this for y-axis labels

    # -----------------------------------------
    # Add rectangles for duration of all hotspots in the classification
    for _, _row in df_summary_sel.iterrows():
        y_pos = country_positions[_row["asap0_name"]]
        fig.add_shape(
            type="rect",
            x0=_row["From"],
            x1=_row["To"],
            y0=y_pos - 0.35,
            y1=y_pos + 0.35,
            fillcolor=level_colors[_row["alert_level"]],
            opacity=0.2,
            line=dict(width=0),
            layer="below",
        )

    # -----------------------------------------
    # Add validation data
    if display_surge.value:
        # ----- SURGE DATA
        for _surge_type in df_surge["surge_type"].unique():
            surge_data = df_surge[df_surge["surge_type"] == _surge_type]
            _surge_color = surge_formatting[_surge_type]["color"]
            _line_type = surge_formatting[_surge_type]["line"]

            x_coords = []
            y_coords = []

            for _, surge_row in surge_data.iterrows():
                if surge_row["country"] in country_positions:
                    y_pos = country_positions[surge_row["country"]]
                    line_y = y_pos
                    x_coords.extend(
                        [surge_row["start_date"], surge_row["end_date"], None]
                    )
                    y_coords.extend([line_y, line_y, None])

            if x_coords:
                fig.add_trace(
                    go.Scatter(
                        x=x_coords,
                        y=y_coords,
                        mode="lines",
                        line=dict(color=_surge_color, width=3, dash=_line_type),
                        name=f"Surge: {_surge_type}",
                        hoverinfo="skip",
                        legendgroup="validation",
                        legendgrouptitle_text="Validation",
                    )
                )

        # ----- FLASH APPEALS
        df_fa["date"] = pd.to_datetime(df_fa["date"])

        fig.add_trace(
            go.Scatter(
                x=df_fa["date"],
                y=[country_positions.get(country, -1) for country in df_fa["country"]],
                mode="markers",
                marker=dict(
                    color=fa_color,
                    size=10,
                    symbol="x",
                    line=dict(width=1.5, color="white"),
                    opacity=0.8,
                ),
                name="Flash Appeal",
                legendgroup="validation",
                legendgrouptitle_text="Validation",
                hovertemplate="Date: %{x}<br>"
                + "Requirements: $%{customdata[0]:,.0f}"
                + "<extra></extra>",
                customdata=df_fa[["requirements"]].values,
            )
        )
        # ----- CERF
        fig.add_trace(
            go.Scatter(
                x=df_cerf["date"],
                y=[
                    country_positions.get(country, -1) for country in df_cerf["country"]
                ],
                mode="markers",
                marker=dict(
                    color=cerf_color,
                    size=10,
                    symbol="circle",
                    line=dict(width=1.5, color="white"),
                    opacity=0.8,
                ),
                name="CERF Disbursement",
                legendgroup="validation",
                legendgrouptitle_text="Validation",
                hovertemplate="Date: %{x}<br>"
                + "Type: %{customdata[1]}"
                + "<extra></extra>",
                customdata=df_cerf[["projectTitle", "windowFullName"]].values,
            ),
        )

    # -----------------------------------------
    # Add activation dates
    if not display_surge.value:
        for _, _row in df_summary_sel.iterrows():

            fig.add_trace(
                go.Scatter(
                    x=[_row["From"]],
                    y=[country_positions[_row["asap0_name"]]],
                    mode="markers",
                    marker=dict(
                        color=level_colors[_row["alert_level"]],
                        size=10,
                        symbol='circle',
                        line=dict(width=0, color="white"),
                    ),
                    name=_row["alert_level"],
                    showlegend=False,
                )
            )

    # -----------------------------------------
    # Workaround to add some data to the plot area so that the
    # empty grey shapes show up if they are all that is selected

    if len(fig.data) == 0:
        print("no data!")
        # Add invisible trace to establish plot area
        fig.add_trace(
            go.Scatter(
                x=[start_date, end_date],
                y=[0, len(countries) - 1],
                mode="markers",
                marker=dict(size=0.1, color="rgba(0,0,0,0)"),
                showlegend=False,
                hoverinfo="skip",
            )
        )

    # -----------------------------------------
    # Traces just for the legend

    for _cat in category_multiselect.value:
        fig.add_trace(
            go.Scatter(
                x=[None],
                y=[None],
                mode="markers",
                marker=dict(
                    symbol="square",
                    size=15,
                    color=level_colors[_cat],
                    opacity=0.6,
                ),
                name=_cat.capitalize(),
                showlegend=True,
                legendgroup="threshold",
                legendgrouptitle_text="ASAP Classification",
            )
        )

    # -----------------------------------------
    # Layout functions
    if len(category_multiselect.value) == 4:
        title_insert = "all"
    elif len(category_multiselect.value) > 1:
        title_insert = ", ".join(category_multiselect.value)
    elif len(category_multiselect.value) == 1:
        title_insert = category_multiselect.value[0]
    else:
        title_insert = "none"

    title_insert
    fig.update_layout(
        title={
            "text": f"ASAP classifications meeting criteria for <b>{title_insert}</b> conditions",
            "x": 0.5,
            "font": {"size": 20},
        },
        margin=dict(l=0, r=0, t=40, b=0),
        xaxis=dict(
            title="Date",
            showgrid=True,
            gridcolor="#eeeeee",
            gridwidth=0.5,
            range=[start_date, end_date],
        ),
        yaxis=dict(
            title="Country",
            tickvals=list(range(len(countries))),  # Show ALL countries
            ticktext=countries,  # ALL country names
            showgrid=True,
            gridcolor="#eeeeee",
            gridwidth=0.5,
            range=[-0.55, len(countries) - 0.5],
            zeroline=False,  # Remove the zero line which might be interfering
        ),
        plot_bgcolor="white",
        # width=1000,
        height=600,
        hovermode="closest",
        legend=dict(
            x=0.99,  # Right side
            y=0.98,  # Top
            xanchor="right",
            yanchor="top",
            bgcolor="rgba(255,255,255,0.6)",  # Semi-transparent white background
            bordercolor="white",
            borderwidth=1,
            orientation="v",  # Vertical orientation
        ),
    )

    fig
    return


@app.cell
def _(mo):
    mo.md(r"""## 4. View output results""")
    return


@app.cell
def _(df_save):
    df_save
    return


@app.cell
def _(df_save, stratus):
    stratus.upload_csv_to_blob(df_save, blob_name="ds-rosea-thresholds/processed/asap/hotspot_historical_classified.csv")
    return


if __name__ == "__main__":
    app.run()
