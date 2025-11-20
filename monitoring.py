import marimo

__generated_with = "0.15.2"
app = marimo.App(css_file="assets/custom.css")


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
    mo.center(mo.md("# ROSEA Slow Onset Monitoring"))
    return


@app.cell
def _(mo):
    mo.Html("<hr></hr>")
    return


@app.cell
def _():
    import geopandas as gpd
    import ocha_stratus as stratus
    import pandas as pd
    import plotly.express as px
    from dotenv import find_dotenv, load_dotenv
    from fsspec.implementations.http import HTTPFileSystem

    from src import plot
    from src.constants import ISO3S

    color_map = {
        "low": "#2E8B57",  # Sea Green
        "medium": "#FF8C00",  # Dark Orange
        "high": "#DC143C",  # Crimson
        "very high": "#8B0000",  # Dark Red
    }

    _ = load_dotenv(find_dotenv(usecwd=True))  # noqa
    return HTTPFileSystem, ISO3S, color_map, gpd, pd, plot, px, stratus


@app.cell
def _(HTTPFileSystem, gpd, mo):
    @mo.persistent_cache
    def get_admin(iso3s):
        url = "https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.parquet"
        filters = [("iso_3", "in", iso3s)]
        filesystem = HTTPFileSystem()
        gdf = gpd.read_parquet(url, filters=filters, filesystem=filesystem)
        gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.001)
        return gdf

    return (get_admin,)


@app.cell
def _(ISO3S, get_admin, stratus):
    gdf = get_admin(list(ISO3S.values()))

    df_clean = stratus.load_csv_from_blob(
        "ds-rosea-thresholds/monitoring/summary.csv",
        parse_dates=["hotspot_date", "ipc_start_date", "ipc_end_date"],
    )

    gdf_merged = gdf[["iso_3", "geometry"]].merge(
        df_clean, right_on="iso3", left_on="iso_3"
    )
    return df_clean, gdf_merged


@app.cell
def _(df_clean, mo):
    low = mo.stat(
        value=len(df_clean[df_clean.max_alert_level == "low"]),
        label="Low Risk countries",
    )
    med = mo.stat(
        value=len(df_clean[df_clean.max_alert_level == "medium"]),
        label="Medium Risk countries",
    )
    high = mo.stat(
        value=len(df_clean[df_clean.max_alert_level == "high"]),
        label="High Risk countries",
    )
    vhigh = mo.stat(
        value=len(df_clean[df_clean.max_alert_level == "very high"]),
        label="Very High Risk countries",
    )
    mo.hstack([low, med, high, vhigh], justify="center")
    return


@app.cell
def _(color_map, gdf_merged, px):
    # Create hover data formatting
    hover_data_formatted = {}
    for col in ["alert_level_hs", "hotspot_date"]:
        if col in gdf_merged.columns:
            hover_data_formatted[col] = True

    # Format proportion data as percentage
    if "proportion_3+" in gdf_merged.columns:
        hover_data_formatted["proportion_3+"] = ":.1%"

    # Format population data with thousands separator
    if "population_3+" in gdf_merged.columns:
        hover_data_formatted["population_3+"] = ":,.0f"

    hover_data_formatted["iso3"] = False  # Hide ISO3 in hover

    # Create the figure using the modern approach
    fig = px.choropleth_map(
        gdf_merged,
        geojson=gdf_merged.geometry,
        locations=gdf_merged.index,
        color="max_alert_level",
        hover_name="country",
        hover_data=hover_data_formatted,
        color_discrete_map=color_map,
        zoom=2.5,
        center={"lat": -9, "lon": 30},
        opacity=0.75,
    )

    # Update layout for better presentation
    fig.update_layout(
        height=350,
        margin={"l": 0, "r": 0, "t": 0, "b": 0},
        legend={
            "title": {
                "text": "Alert Level",
                "font": {"size": 16, "family": "Arial, sans-serif"},
            },
            "orientation": "v",
            "yanchor": "top",
            "y": 0.95,
            "xanchor": "left",
            "x": 0.02,
            "bgcolor": "rgba(255,255,255,0.7)",
            "font": {"size": 14},
        },
    )
    return


@app.cell
def _(df_clean, mo, plot):
    df_table = df_clean.drop(df_clean.columns[-9:], axis=1)
    mo.accordion({"#### See summary table": plot.summary_table(df_table)})
    return


@app.cell
def _(df_clean, mo):
    country_select = mo.ui.dropdown(
        label="## Select a country for more details",
        options=df_clean.country.unique(),
        value=df_clean.country.unique()[0],
    )
    mo.center(country_select)
    return (country_select,)


@app.cell
def _(country_select, df_clean):
    sel = df_clean[df_clean.country == country_select.value].reset_index()
    return (sel,)


@app.cell
def _(mo, pd, sel):
    ipc_alert_level = (
        "None" if pd.isna(sel["alert_level_ipc"][0]) else sel["alert_level_ipc"][0]
    )
    ipc_alert = mo.stat(caption="IPC Alert Level", value=ipc_alert_level)
    hotspot_alert = mo.stat(
        caption="Hotspot Alert Level", value=sel["alert_level_hs"][0]
    )
    hotspot_date = mo.stat(
        caption="Hotspot issued",
        value=sel["hotspot_date"][0].strftime("%b %-d, '%y"),
    )

    mo.hstack([ipc_alert, hotspot_alert, hotspot_date], justify="center")
    return


@app.cell
def _(country_select, mo, pd, plot, sel):
    mo.stop(pd.isna(sel["alert_level_ipc"][0]), mo.md("No IPC data available."))

    ipc_start = sel["ipc_start_date"][0].strftime("%-d %b %Y")
    ipc_end = sel["ipc_end_date"][0].strftime("%-d %b %Y")
    ipc_dates = f"{ipc_start} - {ipc_end}"

    title = f"{country_select.value} IPC Summary: {ipc_dates} ({sel['ipc_type'][0].capitalize()})"  # noqa

    df_pivot = sel.melt(
        id_vars=["index", "country", "iso3"],  # columns to keep as identifiers
        value_vars=[
            "proportion_3+",
            "proportion_4+",
            "proportion_5",
            "population_3+",
            "population_4+",
            "population_5",
            "pt_change_3+",
            "pt_change_4+",
        ],
        var_name="metric",
        value_name="value",
    )

    # Extract the phase (3+ or 4+) and metric type (proportion, population, pt_change)
    df_pivot[["metric_type", "phase"]] = df_pivot["metric"].str.extract(
        r"(.+?)_(\d+\+?)"
    )
    df_pivot = (
        df_pivot.pivot_table(
            index=["index", "country", "iso3", "phase"],
            columns="metric_type",
            values="value",
            aggfunc="first",
        )
        .reset_index()
        .drop(columns=["country", "iso3", "index"])
    )

    plot.ipc_table(df_pivot, title)
    return


@app.cell
def _(mo, sel):
    summary_text = sel["hotspot_comment"][0]
    mo.md(summary_text)
    return


if __name__ == "__main__":
    app.run()
