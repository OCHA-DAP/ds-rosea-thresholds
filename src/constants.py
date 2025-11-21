# ----- SHARED -----
ISO3S = {
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


# ----- ASAP Thresholds -----

# Number of consecutive Hotspots to reach "High" level
HIGH_CONSEC = 3

# ----- IPC Thresholds -----

# Very high - severe
VH_S_POP_4 = 500000
# Very high - deteriorating
VH_D_PR_3 = 0.25
VH_D_IN_4 = 0.03

# High - severe
H_S_POP_4 = 200000
# High - deteriorating
H_D_PR_3 = 0.25
H_D_IN_3 = 0.05

# Medium
M_PR_3 = 0.18
M_POP_4 = 50000
