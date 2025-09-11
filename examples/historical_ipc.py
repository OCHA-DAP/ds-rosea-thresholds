import marimo

__generated_with = "0.15.2"
app = marimo.App(app_title="ROSEA IPC Thresholds")


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
    mo.center(mo.md("# ROSEA IPC Threshold Exploration"))
    return


@app.cell
def _(mo):
    mo.Html("<hr></hr>")
    return


@app.cell
def _(mo):
    mo.callout(
        "This notebook is still a work in progress, and meant for internal use. Please contact hannah.ker@un.org at the OCHA Centre for Humanitarian Data for any questions.",
        kind="warn",
    )
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    This notebook provides an overview of recent and historical [IPC acute food insecurity](https://www.ipcinfo.org/) estimates across countries in Southern and Eastern Africa. Users should be able to gain an understanding of historical trends in food security over time per country, and explore potential thresholds for triggering an alert due to conditions of concern.


    To standardize this analysis, we have only selected data from IPC reports at the _national_ level. We have also focused on the _fraction_ of a population in a given IPC phase, rather than the raw number of people.

    Historical IPC reports for a given country may overlap, such as in cases where IPC replaces what was a projection with current values. To remove overlapping values, we select only those from the most recently published report. This means that we prioritize current values over those that were previously projected (even in cases where the projected estimate was greater). 

    We access IPC data via the [HDX Humanitarian API](https://hdx-hapi.readthedocs.io/en/latest/data_usage_guides/food_security_nutrition_and_poverty/).
    """
    )
    return


@app.cell
def _():
    import pandas as pd
    import plotly.express as px
    import requests 
    import os
    import plotly.graph_objects as go
    from dotenv import load_dotenv

    load_dotenv()

    iso3s = {
        'Angola': 'AGO',
        'Burundi': 'BDI',
        'Comoros': 'COM',
        'Djibouti': 'DJI',
        'Eswatini': 'SWZ',
        'Kenya': 'KEN',
        'Lesotho': 'LSO',
        'Madagascar': 'MDG',
        'Malawi': 'MWI',
        'Namibia': 'NAM',
        'Rwanda': 'RWA',
        'Tanzania': 'TZA',
        'Uganda': 'UGA',
        'Zambia': 'ZMB',
        'Zimbabwe': 'ZWE'
    }

    severity_levels = ["3+", "4+"]
    return go, iso3s, pd, px, requests


@app.cell
def _(pd, requests):
    def get_ipc_from_hapi(iso3=None):
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
            .agg({"population_fraction_in_phase": "sum"})
            .reset_index()
        )
        return pd.concat([df_all, dff])
    return combine_4_plus, get_ipc_from_hapi


@app.cell
def _(mo):
    mo.md(
        r"""
    ## 1. Explore Distribution of All IPC Values

    Across all historical reports in all countries, the plot below shows the distribution of the proportion of a country's population in either IPC Phase 3+ or 4+. Use the toggle to disaggragate by country.
    """
    )
    return


@app.cell
def _(combine_4_plus, get_ipc_from_hapi, iso3s, pd):
    # Get all IPC data
    df_all = get_ipc_from_hapi()

    # Filter to only locations that we care about
    df_all = df_all[df_all.location_code.isin(list(iso3s.values()))]

    # Filter to just 3+ and 4+
    df_all_ = combine_4_plus(df_all)
    df_all_ = df_all_[df_all_.ipc_phase.isin(["3+", "4+"])]

    # Pick only most recent values in case there are overlaps
    # Prioritize current over first proj over second proj
    priority = {"current": 1, "first projection": 2, "second projection": 3}
    df_all_["priority"] = df_all_["ipc_type"].map(priority)
    df_all_long = df_all_.sort_values(["location_code", "priority"]).drop_duplicates(["location_code", "ipc_phase", "From", "To"], keep="first")
    df_all_long = df_all_long.sort_values(["location_code", "From"], ascending=True)

    # Convert the data to wide format
    df_all_wide = df_all_long.pivot(index=[col for col in df_all_long.columns if col not in ['ipc_phase', 'population_fraction_in_phase']], 
                       columns='ipc_phase', 
                       values='population_fraction_in_phase').reset_index()
    df_all_wide = df_all_wide.sort_values(['location_code', 'From'])

    # Check that it's half the length, because we made the 3+ and 4+
    # categories wide
    assert(len(df_all_wide) == (len(df_all_long) / 2))

    # Calculate percentage point change by location
    df_all_wide['pt_change_3+'] = df_all_wide.groupby('location_code')['3+'].diff() * 100
    df_all_wide['pt_change_4+'] = df_all_wide.groupby('location_code')['4+'].diff() * 100

    # Convert back to a long format for visualization
    df_all_wide.rename(columns={
        "3+": "proportion_3+",
        "4+": "proportion_4+"
    }, inplace=True)

    df_all_long = pd.wide_to_long(
        df_all_wide,
        stubnames=['proportion_', 'pt_change_'],
        i=['location_code', 'ipc_type', 'From', 'To', 'year', 'priority'],
        j='phase',
        sep='',
        suffix=r'\d\+'
    ).reset_index()
    df_all_long = df_all_long.rename(
        columns={
            "proportion_": "proportion",
            "pt_change_": "pt_change"
        }
    )
    return df_all_long, df_all_wide


@app.cell
def _(mo):
    box_display = mo.ui.switch(value=True, label="Disaggregate box plot by country")
    return (box_display,)


@app.cell
def _(box_display):
    box_display
    return


@app.cell
def _(box_display, df_all_long, px):
    # Display box plot
    if box_display.value:
        fig_box = px.box(df_all_long, x='location_code', y='proportion', facet_row="phase", template="simple_white", title="Distribution of population in phase by country", height=350)
        fig_box.update_yaxes(title="% of population")
        fig_box.update_xaxes(title='Country')
    else:
        fig_box = px.box(df_all_long, y='proportion', x='phase', template='simple_white', title="Distribution of population in phase", height=350)
        fig_box.update_yaxes(title="% of population")
        fig_box.update_xaxes(title='IPC Phase')
    fig_box.update_layout(margin=dict(l=0, r=0, t=40, b=0))
    return


@app.cell
def _(df_all_wide, pd):
    pd.wide_to_long(
        df_all_wide,
        stubnames=['proportion_', 'pt_change_'],
        i=['location_code', 'ipc_type', 'From', 'To', 'year', 'priority'],
        j='phase',
        sep='',
        suffix=r'\d\+'
    ).reset_index()
    return


@app.cell
def _(df_all_wide, mo):
    mo.accordion({"#### See all IPC data": df_all_wide})
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## 2. Drill Down By Country

    The heatmap below gives us an overview of historical trends in a country's IPC3+ or 4+ population over time.
    """
    )
    return


@app.cell
def _(iso3s, mo):
    # With search functionality
    iso3_dropdown = mo.ui.dropdown(
        options=iso3s,
        label="Choose a country",
        searchable=True,
    )
    value_radio = mo.ui.radio(
        options=["proportion", "pt_change"], 
        label="Choose value to display:", 
        value="proportion", 
        inline=True
    )
    return iso3_dropdown, value_radio


@app.cell
def _(iso3_dropdown, mo, value_radio):
    mo.hstack([iso3_dropdown, value_radio], justify='start')
    return


@app.cell
def _(df_all_long, iso3_dropdown):
    df_sel = df_all_long[df_all_long.location_code == iso3_dropdown.value]
    return (df_sel,)


@app.cell
def _(df_sel, iso3_dropdown, mo):
    mo.stop(len(df_sel) == 0, mo.md(f"**IPC data not available for {iso3_dropdown.selected_key}**"))
    return


@app.cell
def _(df_sel, go, iso3_dropdown, pd, value_radio):
    _fig = go.Figure()
    color_map = {'3+': 'blue', '4+': 'red'}
    title_var = "Percent change in affected population" if value_radio.value == "pt_change" else "Proportion of population in phase"
    y_axis_range = [-100, 100] if value_radio.value == "pt_change" else [0,1]

    # Add trace for each row
    for _, row in df_sel.iterrows():
        dates = pd.date_range(row['From'], row['To'])
        _fig.add_trace(go.Scatter(
            x=dates, 
            fill='tozeroy',
            y=[row[value_radio.value]] * len(dates),
            line=dict(color=color_map[row['phase']]),
            showlegend=False
        ))
    # Add legend entries for color mapping
    _fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', 
                            line=dict(color='blue'), name='3+', showlegend=True))
    _fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines', 
                            line=dict(color='red'), name='4+', showlegend=True))
    _fig.update_layout(
        template="simple_white",
        yaxis=dict(showgrid=True, gridcolor='lightgrey', range=y_axis_range),
        title=f"{title_var} in {iso3_dropdown.selected_key}"
    )
    _fig
    return


@app.cell
def _(df_sel, mo):
    mo.accordion({"#### See country-level data": df_sel.sort_values("From")})
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## 3. Select thresholds for alerts

    Use the sliders below to design potential triggers for IPC data monitoring. The table will show historical cases, per country, that would have met the set trigger conditions.
    """
    )
    return


@app.cell
def _(mo):
    ipc_3 = mo.ui.slider(start=0, stop=1, step=0.01, label="**Minimum proportion in IPC 3+**", value=0.3, show_value=True, debounce=True)
    ipc_4 = mo.ui.slider(start=0, stop=1, step=0.01, label="**Minimum proportion in IPC 4+**", value=0.1, show_value=True, debounce=True)
    mo.hstack([ipc_3, ipc_4])
    return ipc_3, ipc_4


@app.cell
def _(mo):
    ipc_3_change = mo.ui.slider(start=0, stop=100, step=1, label="**Minimum point increase in IPC 3+**", value=10, show_value=True, debounce=True)
    ipc_4_change = mo.ui.slider(start=0, stop=100, step=1, label="**Minimum point increase in IPC 4+**", value=5, show_value=True, debounce=True)
    mo.hstack([ipc_3_change, ipc_4_change])
    return ipc_3_change, ipc_4_change


@app.cell
def _(df_all_wide, ipc_3, ipc_3_change, ipc_4, ipc_4_change, iso3s):
    df_triggers = df_all_wide[
        (df_all_wide["proportion_3+"] >= ipc_3.value) & 
        (df_all_wide["proportion_4+"] >= ipc_4.value) 
    ]

    if ipc_3_change.value != 0:
        df_triggers = df_triggers[
            df_all_wide["pt_change_3+"] >= (ipc_3_change.value)
        ]

    if ipc_4_change.value != 0:
        df_triggers = df_triggers[
            df_all_wide["pt_change_4+"] >= (ipc_4_change.value)
        ]


    iso3_to_country = {v: k for k, v in iso3s.items()}
    df_triggers['country'] = df_triggers['location_code'].map(iso3_to_country)
    df_triggers.sort_values("From", inplace=True)
    df_triggers["From"] = df_triggers['From'].dt.strftime('%b %d, %Y')
    df_triggers["To"] = df_triggers['To'].dt.strftime('%b %d, %Y')
    df_triggers = df_triggers[["country", "From", "To", "proportion_3+", "proportion_4+", "pt_change_3+", "pt_change_4+"]]
    df_triggers
    return


if __name__ == "__main__":
    app.run()
