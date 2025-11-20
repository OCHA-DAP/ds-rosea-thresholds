import os

import pandas as pd
import requests

LISTMONK_LIST_ID = 3
LISTMONK_TEMPLATE_ID = 8


def send_rosea_campaign(image_url):
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
        "body": f'<img src="{image_url}" alt="Summary Table" style="max-width: 100%;">',
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
