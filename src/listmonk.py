import os

import pandas as pd
import requests

from src.utils import load_boolean_env

LISTMONK_TEST_LIST = 12
LISTMONK_LIST = 13
DS_TEAM_LIST = 6

LISTMONK_TEMPLATE_ID = 8
METHODS_URL = "https://docs.google.com/document/d/1Wv9JIuk6V0tafRB9FLuB6jcy_EwBU7_iXYqQuOAJeBI/edit?usp=sharing"
DASHBOARD_URL = "https://ocha-dap.github.io/ds-rosea-thresholds/"


def generate_rosea_content(table_html):
    intro_html = f"""
    <div>
        <p>Dear Colleagues,</p>
        <p>Please see the table below for an update on our monitoring of slow-onset
        shocks, to guide the OCHA Regional Office for Eastern and Southern Africa’s
        (ROSEA) support to countries without a permanent OCHA presence.
        See <a href='{METHODS_URL}'>
        this document</a> for a description of the data sources and methods used.</p>
        <p>You can also find more detailed, country-level information on
        <a href='{DASHBOARD_URL}'>
        this dashboard</a>.</p>
        <p>See more information from original data sources on the 
        <a href='https://agricultural-production-hotspots.ec.europa.eu/'>ASAP Website</a>
        and the <a href='https://www.ipcinfo.org'>IPC Website</a>.
        </p>
    </div>
    """  # noqa

    exit_html = """
    <div>
        <p>These emails are sent anytime new data is available.
        Updates are highlighted in the table above with a black
        outline, with any changes in the Maximum Alert Level
        also indicated by a directional (up/down) arrow. Data
        updates may be shared without any changes in
        Maximum Alert Level.</p>
    </div>
    """  # noqa
    return intro_html + table_html + exit_html


def send_rosea_campaign(body_html):
    LISTMONK_URL = os.getenv("LISTMONK_URL")
    LISTMONK_API_KEY = os.getenv("LISTMONK_API_KEY")
    LISTMONK_API_UID = os.getenv("LISTMONK_API_UID")
    TEST = load_boolean_env("TEST_EMAIL", True)

    if TEST:
        print("Sending email to TEST list...")
        listmonk_list = [LISTMONK_TEST_LIST]
        campaign_name_suffix = " [TEST]"
    else:
        print("Sending email to ROSEA distribution list...")
        # listmonk_list = [LISTMONK_LIST, DS_TEAM_LIST]
        # campaign_name_suffix = ""

    cur = pd.Timestamp.now().strftime("%-d %b %y")
    campaign = {
        "name": f"ROSEA {cur} (SO) {campaign_name_suffix}",
        "subject": f"ROSEA Slow Onset Monitoring - {cur}",
        "lists": listmonk_list,
        "type": "regular",
        "content_type": "richtext",
        "body": body_html,
        "template_id": LISTMONK_TEMPLATE_ID,
    }

    response = requests.post(
        f"{LISTMONK_URL}/api/campaigns",
        json=campaign,
        auth=(LISTMONK_API_UID, LISTMONK_API_KEY),
    )

    campaign_data = response.json()

    if response.status_code == 200:
        campaign_id = campaign_data["data"]["id"]

        send_response = requests.put(
            f"{LISTMONK_URL}/api/campaigns/{campaign_id}/status",
            json={"status": "running"},
            auth=(LISTMONK_API_UID, LISTMONK_API_KEY),
        )
        print("campaign sent!")
        return send_response
    else:
        print("failed to create campaign")
        print(campaign_data)
        return response
