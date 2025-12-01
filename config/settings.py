from pathlib import Path
import os

ROOT_DIR = Path(__file__).resolve().parents[1]

DATA_DIR = ROOT_DIR / 'data'
SRC_DIR = ROOT_DIR / 'src'
CONFIG_DIR = ROOT_DIR / 'config'

DATA_FILENAME = 'api_data.csv'
DOTENV_PATH = CONFIG_DIR / '.env'

# for open-meteo
FORECAST_NDAYS = 5

# for great expectations
# default settings from
# https://docs.greatexpectations.io/docs/core/configure_project_settings/configure_data_docs/
DATADOC_BASE_DIR = ROOT_DIR / "uncommitted/data_docs/local_site/"
DATADOC_SITE_CONFIG = {
    "class_name": "SiteBuilder",
    "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
    "store_backend": {
        "class_name": "TupleFilesystemStoreBackend",
        # need str() otherwise Path is not serializable
        "base_directory": str(DATADOC_BASE_DIR),
    },
}