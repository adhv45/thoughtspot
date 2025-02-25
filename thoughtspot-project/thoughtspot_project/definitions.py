import pandas as pd
import sqlite3
from .jobs import thoughtspot_job
from dagster import Definitions, load_assets_from_modules
from thoughtspot_project import assets

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    jobs=[thoughtspot_job],
    assets=all_assets,
)