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
# See the notebooks in examples/ for the analysis where these thresholds
# were set

# Very high
VH_S_POP_4 = 500000  # Severe case. IPC population in 4+
VH_D_PR_3 = 0.25  # Deteriorating case. IPC proportion of population in 3+
VH_D_IN_4 = 0.03  # Deteriorating case. IPC increase in 4+ population

# High
H_S_POP_4 = 200000  # Severe case. IPC population in 4+
H_D_PR_3 = 0.25  # Deteriorating case. IPC proportion of population in 3+
H_D_IN_3 = 0.05  # Deteriorating case. IPC increase in 3+ population

# Medium
M_PR_3 = 0.18  # IPC proportion of population in 3+
M_POP_4 = 50000  # IPC population in 4+

# We only calculate population differences when they are within 10% of each other
# This threshold is copied from Signals
POP_THRESH = 0.1
