# Project Details
The data docs are generated and updated automatically by great expectations (located in `/uncommited/data_docs/local_site/`). An easy way to view this if you use VS Code is using the `Live Server` extension.


# Setup
Note: Make sure python version matches the current   great expectations [requirements](https://pypi.org/project/great-expectations/).


```bash
uv python install 3.13

uv init project_name

cd project_name

uv python pin 3.13

uv add yfinance pandas great_expectations prefect databricks-sql-connector python-dotenv

uv pip install -e .

source .venv/bin/activate

```

## Configure Secrets
Create a new file `config/.env` with the secrets below:
```
RECEIVER_EMAIL=”the email you want to send a notification to”

SENDER_EMAIL=”the email you want to send from”

SENDER_EMAIL_APP_PASSWORD=”the password if needed, e.g., app password for gmail”

SERVER_HOSTNAME="databricks server hostname"

HTTP_PATH="databricks http path"

ACCESS_TOKEN="databricks access token"
```

## Databricks

The last three are from databricks. Sign up for databricks free edition, then generate a personal access token by going to:

- Profile → Settings→ Developer → Generate new token

Get the server hostname and http path by going to:

- SQL warehouses → Severless Starter Warehouse → Connection details

Next, go to the SQL editor and setup the database:

```sql
CREATE DATABASE IF NOT EXISTS meteo_db;

USE meteo_db;

CREATE TABLE IF NOT EXISTS meteo_weather (
    date STRING,
    temperature_2m DOUBLE,
    precipitation DOUBLE,
    relative_humidity_2m DOUBLE,
    apparent_temperature DOUBLE,
    visibility DOUBLE,
    cloud_cover DOUBLE
) USING DELTA;
```

## Run Code
To run the entire pipeline:
```
python -m main
```
If you want to run a specific file in `/src` instead:
```
python -m src.[file name without the '.py']
```
