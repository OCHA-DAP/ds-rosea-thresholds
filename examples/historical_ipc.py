import marimo

__generated_with = "0.16.2"
app = marimo.App(
    width="medium",
    app_title="ROSEA IPC Thresholds",
    css_file="assets/custom.css",
)


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
    import numpy as np
    from plotly.subplots import make_subplots
    import matplotlib.colors as mcolors

    load_dotenv()

    import ocha_stratus as stratus

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
    iso3_to_country = {v: k for k, v in iso3s.items()}
    return (
        go,
        iso3_to_country,
        iso3s,
        make_subplots,
        mcolors,
        np,
        os,
        pd,
        px,
        requests,
        stratus,
    )


@app.cell
def _(os, pd, requests):
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
                "population_in_phase",
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
            dff.groupby(["From", "To", "location_code", "ipc_type", "year", "ipc_phase", "population_analyzed"])
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
def _(get_ipc_from_hapi, iso3s):
    # Get all IPC data
    df_all = get_ipc_from_hapi()

    # Filter to only locations that we care about
    df_all = df_all[df_all.location_code.isin(list(iso3s.values()))]

    # Transform to get the population analyzed 
    # In theory could also do this by calc population_in_phase * population_fraction_in_phase
    pop_all = df_all[df_all['ipc_phase'] == 'all'].set_index(['location_code', 'ipc_type', 'From', 'To'])['population_in_phase']
    df_all['population_analyzed'] = df_all.set_index(['location_code', 'ipc_type', 'From', 'To']).index.map(pop_all).values
    df_all = df_all.drop(columns="population_in_phase")
    return (df_all,)


@app.cell
def _(combine_4_plus, df_all, pd):
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
    index_cols = ['location_code', 'ipc_type', 'population_analyzed', 'From', 'To', 'year', 'priority']
    df_all_wide = df_all_long.pivot(
            index=index_cols, 
           columns='ipc_phase', 
           values='population_fraction_in_phase').reset_index()
    df_all_wide = df_all_wide.sort_values(['location_code', 'From'])

    # Check that it's half the length, because we made the 3+ and 4+
    # categories wide
    assert(len(df_all_wide) == (len(df_all_long) / 2))

    # Calculate if populations are comparable (within 10%)
    POP_THRESH = 0.1
    df_all_wide['pop_comparable'] = (
        abs(df_all_wide.groupby('location_code')['population_analyzed'].diff()) / 
        df_all_wide.groupby('location_code')['population_analyzed'].shift() <= POP_THRESH
    )

    # Calculate percentage point change only when comparable
    df_all_wide['pt_change_3+'] = df_all_wide.groupby('location_code')['3+'].diff() * 100
    df_all_wide['pt_change_4+'] = df_all_wide.groupby('location_code')['4+'].diff() * 100

    # Set to NaN where populations aren't comparable
    df_all_wide.loc[~df_all_wide['pop_comparable'], ['pt_change_3+', 'pt_change_4+']] = None

    # Convert back to a long format for visualization
    df_all_wide.rename(columns={
        "3+": "proportion_3+",
        "4+": "proportion_4+"
    }, inplace=True)

    df_all_long = pd.wide_to_long(
        df_all_wide,
        stubnames=['proportion_', 'pt_change_'],
        i=['location_code', 'ipc_type', 'From', 'To', 'year', 'priority', 'population_analyzed'],
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
def _():
    # ---- TODO
    # df_all_sel = df_all[df_all.ipc_phase == 'all']
    # df_all_sel['percentage_of_max'] = df_all_sel.groupby('location_code')['population_in_phase'].transform(lambda x: x / x.max() * 100)
    # df_all_sel[['location_name', 'percentage_of_max']]
    # df_all_sel.groupby('location_code')['population_in_phase'].max()
    return


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
def _(df_all_wide, mo):
    mo.accordion({"#### See all IPC data": df_all_wide})
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## 2. Validate thresholds""")
    return


@app.cell
def _(mo):
    mo.md(r"""The table below summarizes some initial proposed thresholds for four levels of alert based on incoming IPC reports. Each alert level is tied to a specific support package. Note that the "high" and "extreme" alert levels each have two potential trigger conditions, designed to capture both severe crises OR rapidly deteriorating conditions. **Note** that we do not evaluate for deteriorating conditions across reports where the population analyzed is significantly different (>10%).  The table below also summarizes some key statistics per alert level based on a historical analysis of IPC data (as shown in the charts below).""")
    return


@app.cell
def _(
    df_all_wide,
    h_d_d3,
    h_d_p3,
    h_s_p3,
    h_s_p4,
    iso3_to_country,
    m_p3,
    np,
    vh_d_d3,
    vh_d_d4,
    vh_d_p3,
    vh_s_p3,
    vh_s_p4,
):
    def assign_cat_row(row):
        P3 = row.get("proportion_3+", np.nan)
        P4 = row.get("proportion_4+", np.nan)
        D3 = row.get("pt_change_3+", np.nan)
        D4 = row.get("pt_change_4+", np.nan)

        VH_S = (P3 >= vh_s_p3.value and P4 >= vh_s_p4.value)
        VH_D = (P3 >= vh_d_p3.value and D3 >= vh_d_d3.value*100 and D4 >= vh_d_d4.value*100)
        H_S = (P3 >= h_s_p3.value and P4 >= h_s_p4.value)
        H_D = (P3 >= h_d_p3.value and D3 >= h_d_d3.value*100)
        M = (P3 >= m_p3.value)

        if (VH_S and VH_D):
            return "extreme - both"
        elif VH_S: 
            return "extreme - severe"
        elif VH_D:
            return "extreme - deteriorating"
        elif (H_S and H_D):
            return "high - both"
        elif H_S:
            return "high - severe"
        elif H_D:
            return "high - deteriorating"
        elif M:
            return "medium"
        return "low"

    df_summary = df_all_wide.copy()
    df_summary["category"] = df_summary.apply(assign_cat_row, axis=1)
    df_summary["country"] = df_summary['location_code'].map(iso3_to_country)
    df_summary["start_year"] = df_summary["From"].dt.year

    split_categories = df_summary['category'].str.split(' - ', expand=True)

    df_summary['cat_1'] = split_categories[0]
    df_summary['cat_2'] = split_categories[1] 
    return (df_summary,)


@app.cell
def _(df_summary, np):
    summary_stats = {}
    for cat in df_summary.cat_1.unique():
        _df_summary_sel = df_summary[df_summary.cat_1 == cat]
        summary_stats[cat] = {}
        summary_stats[cat]["n_rep"] = len(_df_summary_sel)
        summary_stats[cat]["p_rep"] = np.round((len(_df_summary_sel) / len(df_summary)) *100, 1)
        reports_by_year = _df_summary_sel.groupby('start_year').size()
        summary_stats[cat]["avg"] = np.round(reports_by_year.mean(),1)
    return (summary_stats,)


@app.cell
def _():
    # total = mo.stat(
    #     value=len(df_summary_sel),
    #     label="Number of IPC reports that meet criteria",
    # )
    # proportion = mo.stat(
    #     value=(f"{np.round((len(df_summary_sel) / len(df_all_wide)) *100, 1)}%"),
    #     label="Proportion of all IPC reports"
    # )

    # # Count reports per year
    # reports_by_year = df_summary_sel.groupby('start_year').size()
    # average_reports_per_year = np.round(reports_by_year.mean(),1)

    # reports_per_year = mo.stat(
    #     value=average_reports_per_year,
    #     label="Average reports per year in criteria"
    # )

    # # Country with most reports
    # reports_by_country = df_summary_sel.groupby('country').size()
    # max_reports = reports_by_country.idxmax()

    # mo.hstack([total, proportion, reports_per_year], justify="center")
    return


@app.cell
def _(
    h_d_d3,
    h_d_p3,
    h_s_p3,
    h_s_p4,
    m_p3,
    mo,
    summary_stats,
    vh_d_d3,
    vh_d_d4,
    vh_d_p3,
    vh_s_p3,
    vh_s_p4,
):
    mo.md(
        f"""
    | **Category**    | **Description**                                                                                                                                                                                                                                                                                                                    | **Support Package**                                             | **Num. Reports that <br>meet criteria**    | **Percentage of all<br>IPC reports**    | **Average<br>annual**             |
    | :-------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------                                                                                      | :-------------------------------------------------------------- | :----------------------------------------- | :-------------------------------------- | -:----------------------          |
    | Low             | All other reports                                                                                                                                                                                                                                                                                                                  | No Support                                                      | {summary_stats["low"]["n_rep"]}            | {summary_stats["low"]["p_rep"]}         | {summary_stats["low"]["avg"]}     |
    | Medium          | Between {m_p3.value*100}% and {h_s_p3.value*100}% of the population in IPC 3+                                                                                                                                                                                                                                                      | Remote Support + <br>Flash Appeal                               | {summary_stats["medium"]["n_rep"]}         | {summary_stats["medium"]["p_rep"]}      | {summary_stats["medium"]["avg"]}  |
    | High            | **Deteriorating crises**:<br> At least {h_d_p3.value*100}% of the population in IPC 3+ AND at least a {h_d_d3.value*100}% increase in IPC 3+<br>  OR<br> **Severe crises**:<br> At least {h_s_p3.value*100}% of the total population in IPC 3+ AND {h_s_p4.value*100}% of total in IPC4+                                           | Physical Surge Support + <br>Flash Appeal                       | {summary_stats["high"]["n_rep"]}           | {summary_stats["high"]["p_rep"]}        | {summary_stats["high"]["avg"]}    |
    | Extreme         | **Deteriorating crises**:<br> At least {vh_d_p3.value*100}% of the population in IPC 3+ AND at least a {vh_d_d3.value*100}% increase in IPC 3+ AND at least {vh_d_d4.value*100}% increase in IPC 4+<br>  OR<br> **Severe crises**:<br> At least {vh_s_p3.value*100}% of the population in IPC 3+ AND {vh_s_p4.value*100}% in IPC4+ | Physical Surge Support + <br>Flash Appeal + <br>CERF request    | {summary_stats["extreme"]["n_rep"]}        | {summary_stats["extreme"]["p_rep"]}     | {summary_stats["extreme"]["avg"]} |
    """
    )
    return


@app.cell
def _(df_all_wide, iso3_to_country, pd):
    gaps = []
    df_sorted = df_all_wide.sort_values(['location_code', 'From'])
    end_date = pd.Timestamp('2027-10-31')
    start_date = pd.Timestamp('2017-01-01')

    for country, group in df_sorted.groupby('location_code'):
        # Merge overlapping periods
        periods = []
        for _, _row in group.iterrows():
            periods.append((_row['From'], _row['To']))

        periods.sort()
        merged = []
        for start, end in periods:
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], end))
            else:
                merged.append((start, end))

        # Gap from 2017 to first report
        if merged and merged[0][0] > start_date:
            gaps.append({
                'location_code': country,
                'gap_start': start_date,
                'gap_end': merged[0][0] - pd.Timedelta(days=1)
            })

        # Gaps between merged periods
        for i in range(len(merged) - 1):
            gap_start = merged[i][1] + pd.Timedelta(days=1)
            gap_end = merged[i+1][0] - pd.Timedelta(days=1)
            if gap_start <= gap_end:
                gaps.append({
                    'location_code': country,
                    'gap_start': gap_start,
                    'gap_end': gap_end
                })

        # Gap from last merged period to end date
        if merged:
            last_end = merged[-1][1]
            if last_end < end_date:
                gaps.append({
                    'location_code': country,
                    'gap_start': last_end + pd.Timedelta(days=1),
                    'gap_end': end_date
                })

    df_gaps = pd.DataFrame(gaps)
    df_gaps["country"] = df_gaps['location_code'].map(iso3_to_country)
    return df_gaps, end_date, start_date


@app.cell
def _(mo):
    mo.md(r"""Use the charts below to investigate past performance of these thresholds and compare against historical occurrences of surge support, flash appeals, and CERF disbursements. As one can see from the charts, the temporal coverage of IPC reporting windows across countries can be inconsistent. Thus, some desired windows of past activation may be missing due to missing IPC data, rather than misconfigured thresholds. A lack of IPC reports for a given period of time may not necessarily indicate a lack of food security concern.""")
    return


@app.cell
def _(mo):
    category_radio = mo.ui.radio(
        options=["extreme", "high", "medium", "low"], 
        label="Select category to investigate:", 
        value="extreme", 
        inline=True
    )

    display_surge = mo.ui.switch(label="Overlay validation points", value=False)
    mo.hstack([category_radio, display_surge])
    return category_radio, display_surge


@app.cell
def _(category_radio, df_summary):
    df_summary_sel = df_summary[df_summary.category.str.contains(category_radio.value)].copy()
    df_summary_sel["start_year"] = df_summary_sel["From"].dt.year
    return (df_summary_sel,)


@app.cell
def _(iso3s, pd, stratus):
    # Clean surge data
    df_ = stratus.load_csv_from_blob("ds-rosea-thresholds/rosea_surge_20202024.csv")
    df_clean = df_.copy()
    df_clean['start_date'] = pd.to_datetime(df_clean['Date of departure'], format='mixed', dayfirst=True)
    df_clean['end_date'] = pd.to_datetime(df_clean['Date of return'], format='mixed', dayfirst=True)
    df_clean['country'] = df_clean['Destination Country']
    df_clean['event_type'] = 'ROSEA_' + df_clean['Type'].fillna('Response')
    df_clean['value'] = df_clean['Days']  # Use mission duration as value
    df_clean['source'] = 'ROSEA'
    df_clean['surge_type'] = df_clean['Surge']
    df_clean['location_code'] = df_clean['country'].map(iso3s)
    df_surge = df_clean[['country', "surge_type", "start_date", "end_date", "location_code"]]

    # Clean flash appeal data
    df_fa = stratus.load_csv_from_blob("ds-rosea-thresholds/flash_appeals.csv")
    df_fa['date'] = pd.to_datetime(df_fa[" Original PDF Publication Date "], dayfirst=True)
    df_fa.rename(columns={"Country Name": "country", " Final Requirements": "requirements"}, inplace=True)
    df_fa['location_code'] = df_fa['country'].map(iso3s)
    df_fa = df_fa[['country', "date", "requirements", "location_code"]]

    # Clean the cerf data
    df_cerf = stratus.load_csv_from_blob("ds-rosea-thresholds/cerf.csv")
    df_cerf["regionName_l"] = df_cerf["regionName"].astype(str).str.strip().str.casefold()
    df_cerf["emergencyTypeName_l"] = df_cerf["emergencyTypeName"].astype(str).str.strip().str.casefold()
    df_cerf["window_l"] = df_cerf["windowFullName"].astype(str).str.strip().str.casefold()
    df_cerf["dateUSGSignature"] = pd.to_datetime(df_cerf["dateUSGSignature"], errors="coerce")
    df_cerf["totalAmountApproved"] = pd.to_numeric(df_cerf["totalAmountApproved"], errors="coerce").fillna(0)
    region_keep = {"eastern africa", "southern africa"}
    mask = df_cerf["emergencyTypeName_l"].eq("drought") & df_cerf["regionName_l"].isin(region_keep)
    df_cerf = df_cerf.loc[mask].dropna(subset=["dateUSGSignature"]).copy()
    df_cerf.rename(columns={"countryCode": "location_code", "countryName": "country", "dateUSGSignature": "date"}, inplace=True)
    df_cerf = df_cerf.replace("United Republic of Tanzania", "Tanzania")
    df_cerf = df_cerf[df_cerf["year"] >= 2017]
    return df_cerf, df_fa, df_surge


@app.cell
def _():
    # Formatting information for plots
    level_colors = {
        'extreme': '#e8857d',        # Muted red
        'high': '#d19970',           # Muted orange
        'medium': '#6b9ce8',         # Muted teal
        'low': '#b0b0b0',            # Muted grey
    }
    cerf_color = "#393D3F"
    fa_color = "#393D3F"

    surge_formatting = {
        "Physical": {'line': None, 'pos': 0.9, 'color': "#1ebfb3"},
        "Remote": {'line': None, 'pos': 0.8, 'color': '#5f1ebf'},
    }

    shape_config = {
        'severe': {'symbol': 'diamond', 'line_width': 0},
        'deteriorating': {'symbol': 'diamond-open', 'line_width': 2}, 
        'both': {'symbol': 'circle', 'line_width': 0},
        None: {'symbol': 'circle', 'line_width': 0}
    }
    return cerf_color, fa_color, level_colors, shape_config, surge_formatting


@app.cell
def _(
    category_radio,
    cerf_color,
    df_cerf,
    df_fa,
    df_gaps,
    df_summary,
    df_summary_sel,
    df_surge,
    display_surge,
    end_date,
    fa_color,
    go,
    level_colors,
    pd,
    shape_config,
    start_date,
    surge_formatting,
):
    # -----------------------------------------
    # Create plot and setup

    fig = go.Figure()
    all_countries = df_summary.country.unique()

    # Create position mapping for ALL countries
    country_positions = {country: i for i, country in enumerate(sorted(all_countries))}
    countries = sorted(all_countries)  # Use this for y-axis labels

    # Convert dates with proper format specification
    df_summary_sel['From'] = pd.to_datetime(df_summary_sel['From'], format='%b %d, %Y')
    df_summary_sel['To'] = pd.to_datetime(df_summary_sel['To'], format='%b %d, %Y')

    # -----------------------------------------
    # Add rectangles for duration of all IPC reports in the classification
    for _, _row in df_summary_sel.iterrows():
        y_pos = country_positions[_row['country']]
        fig.add_shape(
            type="rect",
            x0=_row['From'], x1=_row['To'],
            y0=y_pos - 0.35, y1=y_pos + 0.35,
            fillcolor=level_colors[_row['cat_1']],
            opacity=0.2,
            line=dict(width=0),
            layer='below'
        )

    # -----------------------------------------
    # Add rectangles for gaps in the IPC data
    for _, _row in df_gaps.iterrows():
        y_pos = country_positions[_row['country']]
        fig.add_shape(
            type="rect",
            x0=_row['gap_start'], x1=_row['gap_end'],
            y0=y_pos - 0.35, y1=y_pos + 0.35,
            fillcolor='#eeeeee',
            opacity=0.6,
            line=dict(width=0),
            layer='below'
        )

    # -----------------------------------------
    # Add validation data
    if display_surge.value:
        # ----- SURGE DATA
        for _surge_type in df_surge['surge_type'].unique():
            surge_data = df_surge[df_surge['surge_type'] == _surge_type]
            _surge_color = surge_formatting[_surge_type]['color']
            _line_type = surge_formatting[_surge_type]['line']

            x_coords = []
            y_coords = []

            for _, surge_row in surge_data.iterrows():
                if surge_row['country'] in country_positions:
                    y_pos = country_positions[surge_row['country']]
                    line_y = y_pos
                    x_coords.extend([surge_row['start_date'], surge_row['end_date'], None])
                    y_coords.extend([line_y, line_y, None])

            if x_coords:
                fig.add_trace(go.Scatter(
                    x=x_coords, y=y_coords,
                    mode='lines',
                    line=dict(color=_surge_color, width=3, dash=_line_type),
                    name=f'Surge: {_surge_type}',
                    hoverinfo='skip',
                    legendgroup="validation",
                    legendgrouptitle_text="Validation", 
                ))

        # ----- FLASH APPEALS
        df_fa['date'] = pd.to_datetime(df_fa['date'])

        fig.add_trace(go.Scatter(
            x=df_fa['date'],
            y=[country_positions.get(country, -1) for country in df_fa['country']],
            mode='markers',
            marker=dict(
                color=fa_color,
                size=10,
                symbol='x',
                line=dict(width=1.5, color='white'),
                opacity=0.8
            ),
            name='Flash Appeal',
            legendgroup="validation",
            legendgrouptitle_text="Validation", 
            hovertemplate='Date: %{x}<br>' +
                         'Requirements: $%{customdata[0]:,.0f}' +
                         '<extra></extra>',
            customdata=df_fa[['requirements']].values
        ))
        # ----- CERF
        fig.add_trace(go.Scatter(
            x=df_cerf['date'],
            y=[country_positions.get(country, -1) for country in df_cerf['country']],
            mode='markers',
            marker=dict(
                color=cerf_color,
                size=10,
                symbol='circle',
                line=dict(width=1.5, color='white'),
                opacity=0.8
            ),
            name='CERF Disbursement',
            legendgroup="validation",
            legendgrouptitle_text="Validation", 
            hovertemplate='Date: %{x}<br>' +
                         'Type: %{customdata[1]}' +
                         '<extra></extra>',
            customdata=df_cerf[["projectTitle", "windowFullName"]].values
        ),)

    # -----------------------------------------
    # Add activation dates
    if not display_surge.value:
        for _, _row in df_summary_sel.iterrows():
            config = shape_config.get(_row['cat_2'], shape_config[None])

            fig.add_trace(go.Scatter(
                x=[_row['From']],
                y=[country_positions[_row['country']]],
                mode='markers',
                marker=dict(
                    color=level_colors[_row['cat_1']],
                    size=10,
                    symbol=config['symbol'],
                    line=dict(width=config['line_width'], color='white')
                ),
                name=_row['cat_1'],
                showlegend=False,
                hovertemplate='<b>%{text}</b><br>' +
                             'Start: %{x}<br>' +
                             'Status: %{customdata[4]}<br>' +
                             'Proportion 3+/4+: %{customdata[0]:.0%}/%{customdata[1]:.0%}<br>' +
                             'Point change 3+/4+: %{customdata[2]:.0f}/%{customdata[3]:.0f}<br>' +
                             'Population analyzed: %{customdata[5]:,.0f}'
                             '<extra></extra>',
                text=[_row['country']],
                customdata=[[_row['proportion_3+'], _row['proportion_4+'], 
                            _row['pt_change_3+'], _row['pt_change_4+'], _row['category'], _row["population_analyzed"]]]
            ))

        # Add shape legend items only for shapes that appear in the data
        added_shapes = set()
        for cat_2 in df_summary_sel['cat_2'].fillna('all criteria').unique():
            config = shape_config.get(cat_2 if cat_2 != 'all criteria' else None, shape_config[None])
            if cat_2 not in added_shapes:
                fig.add_trace(go.Scatter(
                    x=[None], y=[None],
                    mode='markers',
                    marker=dict(symbol=config['symbol'], size=12, color=level_colors[category_radio.value],
                               line=dict(width=config['line_width'], color='white')),
                    name=cat_2,
                    showlegend=True,
                    legendgroup='activation',
                    legendgrouptitle_text="Activation"
                ))
                added_shapes.add(cat_2)
    # -----------------------------------------
    # Traces just for the legend

    fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode='markers',
        marker=dict(symbol='square', size=15, color=level_colors[category_radio.value], opacity=0.6),
        name=category_radio.value.capitalize(),
        showlegend=True,
        legendgroup="threshold",
        legendgrouptitle_text="IPC Threshold", 
    ))

    fig.add_trace(go.Scatter(
        x=[None], y=[None],
        mode='markers',
        marker=dict(symbol='square', size=15, color='#eeeeee', opacity=0.6),
        name='No IPC reports',
        showlegend=True,
        legendgroup="threshold",
        legendgrouptitle_text="IPC Threshold", 
    ))

    # -----------------------------------------
    # Layout functions

    fig.update_layout(
        title={
            'text': f"IPC reports meeting criteria for <b>{category_radio.value}</b> conditions",
            'x': 0.5,
            'font': {'size': 20}
        },
        margin=dict(l=0, r=0, t=40, b=0),
        xaxis=dict(
            title='Date',
            showgrid=True,
            gridcolor='#eeeeee',
            gridwidth=0.5,
            range=[start_date, end_date]
        ),
        yaxis=dict(
            title='Country',
            tickvals=list(range(len(countries))),  # Show ALL countries
            ticktext=countries,                    # ALL country names
            showgrid=True,
            gridcolor='#eeeeee',
            gridwidth=0.5,
            range=[-0.55, len(countries) - 0.5],
            zeroline=False  # Remove the zero line which might be interfering
        ),
        plot_bgcolor='white',
        # width=1000,
        height=600,
        hovermode='closest',
        legend=dict(
            x=0.99,          # Right side
            y=0.98,          # Top
            xanchor='right',
            yanchor='top',
            bgcolor='rgba(255,255,255,0.6)',  # Semi-transparent white background
            bordercolor='white',
            borderwidth=1,
            orientation='v'   # Vertical orientation
        )
    )

    fig
    return


@app.cell
def _(mo):
    mo.md(r"""We can drill down even further into specific countries by looking at the plot below. Here, we can get a closer look at how well various alert levels correlate with the timing of past surge support and funding disbursements.""")
    return


@app.cell
def _(iso3s, mo):
    # With search functionality
    iso3_dropdown = mo.ui.dropdown(
        options=iso3s,
        label="Choose a country",
        searchable=True,
        value="Angola"
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
    mo.hstack([iso3_dropdown, value_radio])
    return


@app.cell
def _(df_cerf, df_fa, df_summary, df_surge, iso3_dropdown):
    df_levels_sel = df_summary[df_summary.location_code == iso3_dropdown.value]
    df_cerf_sel = df_cerf[df_cerf.location_code==iso3_dropdown.value]
    df_surge_sel = df_surge[df_surge.location_code==iso3_dropdown.value]
    df_fa_sel = df_fa[df_fa.location_code==iso3_dropdown.value]
    return df_cerf_sel, df_fa_sel, df_levels_sel, df_surge_sel


@app.cell
def _(df_levels_sel, iso3_dropdown, mo):
    mo.stop(len(df_levels_sel) == 0, mo.md(f"**IPC data not available for {iso3_dropdown.selected_key}**"))
    return


@app.cell
def _(
    cerf_color,
    df_cerf_sel,
    df_fa_sel,
    df_levels_sel,
    df_surge_sel,
    fa_color,
    go,
    iso3_dropdown,
    level_colors,
    make_subplots,
    mcolors,
    pd,
    surge_formatting,
    value_radio,
):
    y_axis_range = [-25, 25] if value_radio.value == "pt_change" else [0,0.65]
    y_axis_title = "Percent point change<br>(3+ / 4+)" if value_radio.value == "pt_change" else "Proportion of population<br>(3+ / 4+)"

    # -----------------------------------------
    # Create plot
    _fig = go.Figure()
    _fig = make_subplots(rows=2, cols=1,
                        shared_xaxes=True,
                        vertical_spacing=0.06,
                        row_heights=[0.85, 0.15])

    # -----------------------------------------
    # Add all IPC reports
    legend_added = set()  # Track which categories we've added to legend

    for _, row in df_levels_sel.iterrows():
        dates = pd.date_range(row['From'], row['To'])
        show_legend = row['cat_1'] not in legend_added
        if show_legend:
            legend_added.add(row['cat_1'])

        base_color = level_colors[row['cat_1']]

        for val, opacity in [("3+", 0.2), ("4+", 0.6)]:
            _fig.add_trace(go.Scatter(
                x=dates, 
                fill='tozeroy',
                fillcolor=f"rgba{(*mcolors.hex2color(base_color), opacity)}",
                y=[row[f"{value_radio.value}_{val}"]] * len(dates),
                # line=dict(width=0),
                line=dict(color=level_colors[row['cat_1']]),
                showlegend=show_legend and val=="3+",
                name=row['cat_1'],
                legendgroup="threshold",
                legendgrouptitle_text="IPC Threshold",
                customdata=[[row['proportion_3+'], row['pt_change_3+'], 
                            row['proportion_4+'], row['pt_change_4+'], 
                            row['category'], row['From'], row['To'], row['population_analyzed']]] * len(dates),
                hovertemplate=(
                    "Report Period: %{customdata[5]|%b %d} - %{customdata[6]|%b %d}<br>"
                    'Status: %{customdata[4]}<br>' +
                    'Proportion 3+/4+: %{customdata[0]:.0%}/%{customdata[2]:.0%}<br>' +
                    'Point change 3+/4+: %{customdata[1]:.0f}/%{customdata[3]:.0f}<br>' +
                    'Population analyzed: %{customdata[7]:,.0f}'
                    "<extra></extra>"
                )
            ), col=1, row=1)

    # -----------------------------------------
    # Add surge activities as scatter traces
    for _surge_type in df_surge_sel['surge_type'].unique():
        _surge_data = df_surge_sel[df_surge_sel['surge_type'] == _surge_type]
        _surge_color = surge_formatting[_surge_type]['color']
        _line_type = surge_formatting[_surge_type]['line']
        _y_pos = surge_formatting[_surge_type]['pos']

        _x_coords = []
        _y_coords = []

        for _, _surge_row in _surge_data.iterrows():
            _line_y = _y_pos

            _x_coords.extend([_surge_row['start_date'], _surge_row['end_date'], None])
            _y_coords.extend([_line_y, _line_y, None])

        if _x_coords:
            _fig.add_trace(go.Scatter(
                x=_x_coords, y=_y_coords,
                mode='lines',
                line=dict(color=_surge_color, width=3, dash=_line_type),
                name=f'Surge: {_surge_type}',
                hoverinfo='skip',
                legendgroup="validation",
                legendgrouptitle_text="Validation", 
            ), col=1, row=2)

    # ------------------------------------------
    # Add points for Flash Appeals
    _fig.add_trace(go.Scatter(
        x=df_fa_sel['date'],
        y=[1] * len(df_fa_sel),
        mode='markers',
        marker=dict(
            color=fa_color,
            size=15,
            symbol='x',
            line=dict(width=1.5, color='white'),
            opacity=0.8
        ),
        name='Flash Appeal',
        legendgroup="validation",
        legendgrouptitle_text="Validation", 
        hovertemplate='Date: %{x}<br>' +
                     'Requirements: $%{customdata[0]:,.0f}' +
                     '<extra></extra>',
        customdata=df_fa_sel[['requirements']].values
    ), col=1, row=2)

    # ------------------------------------------
    # Add points for CERF disbursements
    _fig.add_trace(go.Scatter(
        x=df_cerf_sel['date'],
        y=[1] * len(df_cerf_sel),
        mode='markers',
        marker=dict(
            color=cerf_color,
            size=15,
            symbol='circle',
            line=dict(width=1.5, color='white'),
            opacity=0.8
        ),
        name='CERF Disbursement',
        legendgroup="validation",
        legendgrouptitle_text="Validation", 
        hovertemplate='Date: %{x}<br>' +
                     'Type: %{customdata[1]}' +
                     '<extra></extra>',
        customdata=df_cerf_sel[["projectTitle", "windowFullName"]].values
    ), col=1, row=2)

    # ------------------------------------------
    # Update overall layout
    _fig.update_layout(
        template="simple_white",
        margin=dict(l=0, r=0, t=40, b=0),
        title={
            'text': f"Summary of IPC data and historical response for <b>{iso3_dropdown.selected_key}</b>",
            'x': 0.5,
            'font': {'size': 20}
        },
        legend=dict(
            x=0.98,          # Right side
            y=0.98,          # Top
            xanchor='right',
            yanchor='top',
            bgcolor='rgba(255,255,255,0.6)',  # Semi-transparent white background
            bordercolor='white',
            borderwidth=1,
            orientation='v'   # Vertical orientation
        )
    )

    _fig.update_xaxes(
        showticklabels=False, 
        showline=True, 
        linecolor='black', 
        ticks="", 
        tickformat='%Y', 
        dtick="M12", 
        row=1, col=1
    ) 

    _fig.update_yaxes(
        showgrid=True, 
        gridcolor='#eeeeee', 
        range=y_axis_range, 
        showline=False, 
        ticks="", 
        title=y_axis_title,
        row=1, col=1
    ) 

    _fig.update_yaxes(visible=False, range=[0.65, 1.05], row=2, col=1)

    _fig
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## 3. Adjust threshold values

    Use the inputs below to change the threshold levels for each alert category. The table and plots above will be updated accordingly.
    """
    )
    return


@app.cell
def _(mo):
    # VERY HIGH THRESHOLD
    vh_s_p3 = mo.ui.number(start=0, stop=1, step=0.01, value=0.3, label="Prop. 3+")
    vh_s_p4 = mo.ui.number(start=0, stop=1, step=0.01, value=0.05, label="Prop. 4+")
    vh_d_p3 = mo.ui.number(start=0, stop=1, step=0.01, value=0.3, label="Prop. 3+")
    vh_d_d3 = mo.ui.number(start=0, stop=1, step=0.01, value=0.05, label="Incr. 3+")
    vh_d_d4 = mo.ui.number(start=0, stop=1, step=0.01, value=0.02, label="Incr. 4+")


    mo.accordion({
        "**VERY HIGH**": mo.hstack([
        vh_s_p3, vh_s_p4, 
        mo.md("**OR**"), 
        vh_d_p3, vh_d_d3, vh_d_d4
    ], justify='start')
    })
    return vh_d_d3, vh_d_d4, vh_d_p3, vh_s_p3, vh_s_p4


@app.cell
def _(mo):
    # HIGH THRESHOLD
    h_s_p3 = mo.ui.number(start=0, stop=1, step=0.01, value=0.25, label="Prop. 3+")
    h_s_p4 = mo.ui.number(start=0, stop=1, step=0.01, value=0.03, label="Prop. 4+")

    h_d_p3 = mo.ui.number(start=0, stop=1, step=0.01, value=0.25, label="Prop. 3+")
    h_d_d3 = mo.ui.number(start=0, stop=1, step=0.01, value=0.03, label="Incr. 3+")

    mo.accordion({
        "**HIGH**": mo.hstack([
        h_s_p3, h_s_p4,
        mo.md("**OR**"), 
        h_d_p3, h_d_d3
    ], justify='start')
    })
    return h_d_d3, h_d_p3, h_s_p3, h_s_p4


@app.cell
def _(mo):
    # MED THRESHOLD
    m_p3 = mo.ui.number(start=0, stop=1, step=0.01, value=0.15, label="Prop. 3+")

    mo.accordion({
        "**MEDIUM**": m_p3
    })
    return (m_p3,)


@app.cell
def _(mo):
    mo.accordion({
        "**LOW**": "All other reports"
    })
    return


if __name__ == "__main__":
    app.run()
