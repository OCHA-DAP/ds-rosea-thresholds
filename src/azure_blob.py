import os
from datetime import datetime, timedelta

from azure.storage.blob import BlobSasPermissions, generate_blob_sas


def get_blob_url(blob_name, expiry_days=180, container="projects", stage="dev"):
    account_name = "imb0chd0dev" if stage == "dev" else "imb0chd0prod"
    access_key = os.getenv("DCSI_AZ_BLOB_DEV_ACCESS_KEY")

    sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container,
        blob_name=blob_name,
        account_key=access_key,
        permission=BlobSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(days=expiry_days),
    )

    return f"https://{account_name}.blob.core.windows.net/{container}/{blob_name}?{sas_token}"
