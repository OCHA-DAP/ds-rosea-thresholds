from src.constants import ISO3S
from src.datasources import ipc

df_ipc_raw = ipc.get_reports(filter_iso3s=list(ISO3S.values()))
df_ipc_classified = ipc.classify_reports(df_ipc_raw)