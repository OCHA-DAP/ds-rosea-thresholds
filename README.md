# ROSEA Threshold Monitoring
Country-based support for slow onset and sudden onset shocks. See [here](https://docs.google.com/document/d/1Wv9JIuk6V0tafRB9FLuB6jcy_EwBU7_iXYqQuOAJeBI/edit?tab=t.0#heading=h.ieffsjdjd8lt) for a description of the 
data processing methodology used to classify countries into various risk levels (slow onset).

Runs daily monitoring and sends emails in the event of data updates for a given country.

[This page](https://ocha-dap.github.io/ds-rosea-thresholds/) summarizes current conditions for slow onset shocks.

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
HAPI_APP_IDENTIFIER=""
LISTMONK_API_KEY=""
LISTMONK_API_UID=""
LISTMONK_URL=""
```

Additionally, the following environment variable can be set to determine whether
emails get sent to a "test" email distribution list or not:

```
TEST_EMAIL="true"
```

### Check for data updates

Run the following script to check for new data and update the local CSV files:

```
uv run python check_slow_onset.py
```

This compares freshly fetched data against `data/current.csv`. If changes are detected,
it rotates `current.csv` to `previous.csv` and saves the new data as `current.csv`.

Use `--force` to update the files even if no changes are detected:

```
uv run python check_slow_onset.py --force
```

### Send email

Run the following script to send an email based on the current data:

```
uv run python send_email.py
```

This reads `data/current.csv` and `data/previous.csv`, generates a summary table
highlighting any differences, and sends the email via Listmonk.


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