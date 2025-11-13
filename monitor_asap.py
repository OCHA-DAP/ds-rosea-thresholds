from src.constants import iso3s
from src.datasources import asap

df_hs_raw = asap.get_hotspots(filter_countries=list(iso3s.keys()))
df_hs_classified = asap.classify_hotspots(df_hs_raw)
df_hs_latest = df_hs_classified[df_hs_classified.date == df_hs_classified.date.max()]
