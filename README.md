# ds-rosea-thresholds
Country-based support for slow onset and sudden onset shocks.

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

## Development

**Run code:**
```bash
uv run python your_script.py
uv run jupyter lab
uv run marimo edit notebook.py
```

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