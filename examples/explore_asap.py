import marimo

__generated_with = "0.15.2"
app = marimo.App(width="medium")


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
    return go, io, iso3s, make_subplots, pd, px, requests, stratus, zipfile


@app.cell
def _(io, iso3s, pd, requests, zipfile):
    url = "https://agricultural-production-hotspots.ec.europa.eu/files/hotspots_ts.zip"
    response = requests.get(url)
    df = pd.read_csv(zipfile.ZipFile(io.BytesIO(response.content)).open('hotspots_ts.csv'), sep=';')
    df = df[df["asap0_name"].isin(list(iso3s.keys()))]
    df = df.sort_values(['asap0_name', 'date']).reset_index(drop=True)
    return (df,)


@app.cell
def _(df, px):
    # Assuming your dataframe is called 'df'
    # Group by country and hotspot status, count occurrences
    chart_data = df.groupby(['asap0_name', 'hs_name']).size().reset_index(name='count')

    # Define the order for categories
    category_order = ['Major hotspot', 'Hotspot', 'No hotspot']

    # Define custom colors
    color_map = {
        'Major hotspot': 'darkred',
        'Hotspot': 'red', 
        'No hotspot': '#b0b0b0'
    }

    # Create stacked bar chart
    fig = px.bar(chart_data, 
                 x='asap0_name', 
                 y='count', 
                 color='hs_name',
                 color_discrete_map=color_map,
                 category_orders={'hs_name': category_order},
                 title='Countries by Hotspot Status')

    # Rotate x-axis labels for better readability
    fig.update_layout(xaxis_tickangle=-45, template="simple_white")
    return


@app.cell
def _(df, pd):
    df['date'] = pd.to_datetime(df['date'])
    df['From'] = df['date']
    df['To'] = df.groupby('asap0_name')['date'].shift(-1) - pd.Timedelta(days=1)
    return


@app.cell
def _(stratus):
    df_warnings = stratus.load_csv_from_blob("ds-rosea-thresholds/processed/asap/warnings_filtered.csv", sep=";")
    remap_values = {
        0: 0, 10: 0, 20: 0,
        2: 1, 3: 1, 4: 1, 12: 1, 13:1, 14:1,
        5: 2,
        6: 3, 7: 3, 8:3,
        9: 4,
        98: -1, 99: -1
    }

    df_warnings["warning_val_crop"] = df_warnings["w_crop"].map(remap_values)
    df_warnings["warning_val_range"] = df_warnings["w_range"].map(remap_values)
    return (df_warnings,)


@app.cell
def _(df_warnings, mo):
    country_dropdown = mo.ui.dropdown(label="Select a country", options=sorted(list(df_warnings.asap0_name.unique())), value="Kenya")
    country_dropdown
    return (country_dropdown,)


@app.cell
def _(country_dropdown, df, df_warnings):
    df_warnings_ = df_warnings[df_warnings.asap0_name == country_dropdown.value]
    df_hotspots_ = df[df.asap0_name == country_dropdown.value]
    return df_hotspots_, df_warnings_


@app.cell
def _(df_hotspots_, mo):
    date_dropdown = mo.ui.dropdown(label="Select a hotspot date", options=df_hotspots_.date.unique())
    date_dropdown
    return (date_dropdown,)


@app.cell
def _(date_dropdown):
    date_formatted = date_dropdown.value.strftime("%B %d, %Y") if date_dropdown.value else None
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
        colorscale=[[0, '#b0b0b0'], [0.5, 'red'], [1, 'darkred']],
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


if __name__ == "__main__":
    app.run()
