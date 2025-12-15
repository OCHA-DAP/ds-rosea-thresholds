# ROSEA Threshold Monitoring
Country-based support for slow onset and sudden onset shocks. Here [here](https://docs.google.com/document/d/1Wv9JIuk6V0tafRB9FLuB6jcy_EwBU7_iXYqQuOAJeBI/edit?tab=t.0#heading=h.ieffsjdjd8lt) for a description of the 
data processing methodology used to classify countries into various risk levels (slow onset).

Runs daily monitoring and sends emails in the event of a change in risk level for a given country.

[This marimo notebook](https://rosea-monitoring-slow-onset-fqejb9gkb8d7ecc3.eastus2-01.azurewebsites.net/) shows the latest updates for slow onset shocks.

## Setup

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then:
```bash
git clone <your-repo-url>
cd <your-repo-name>
uv sync
uv run pre-commit install
```

This code needs the following environment variables to run:

```
DSCI_AZ_BLOB_DEV_SAS=""
DSCI_AZ_BLOB_DEV_SAS_WRITE=""
HAPI_APP_IDENTIFIER=""
LISTMONK_API_KEY=""
LISTMONK_API_UID=""
LISTMONK_URL=""
```

### Run slow onset monitoring

```
uv run python check_slow_onset.py
```

### Run slow onset monitoring notebook

```
uv run marimo run monitoring.py
```

## Development


**Code quality:**
```bash
uv run ruff check . --fix  # Lint and auto-fix
uv run ruff format .       # Format code
```

**Add dependencies:**
```bash
uv add package-name        # Runtime
uv add --dev package-name  # Development
```