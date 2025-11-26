import os

import pandas as pd
import requests

LISTMONK_LIST_ID = 3
LISTMONK_TEMPLATE_ID = 8
METHODS_URL = "https://docs.google.com/document/d/1Wv9JIuk6V0tafRB9FLuB6jcy_EwBU7_iXYqQuOAJeBI/edit?usp=sharing"
DASHBOARD_URL = (
    "https://rosea-monitoring-slow-onset-fqejb9gkb8d7ecc3.eastus2-01.azurewebsites.net/"
)


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
    </div>
    """
    return intro_html + table_html


def send_rosea_campaign(body_html):
    LISTMONK_URL = os.getenv("LISTMONK_URL")
    LISTMONK_API_KEY = os.getenv("LISTMONK_API_KEY")
    LISTMONK_API_UID = os.getenv("LISTMONK_API_UID")

    cur = pd.Timestamp.now().strftime("%-d %b %y")
    campaign = {
        "name": f"ROSEA {cur} (SO)",
        "subject": f"ROSEA Slow Onset Monitoring - {cur}",
        "lists": [LISTMONK_LIST_ID],
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
