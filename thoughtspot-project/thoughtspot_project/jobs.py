from dagster import define_asset_job, load_assets_from_modules
from thoughtspot_project import assets

all_assets = load_assets_from_modules([assets])

# Define a job to materialize all assets (assuming asset names match those in assets.py)
thoughtspot_job = define_asset_job(
    name="thoughtspot",
    selection=all_assets
)
