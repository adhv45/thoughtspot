from dagster import repository, fs_io_manager, with_resources, load_assets_from_modules
from thoughtspot_project import assets

all_assets = load_assets_from_modules([assets])

@repository
def my_repo():
    return with_resources(
        all_assets,
        resource_defs={"io_manager": fs_io_manager.configured({"base_dir": "data/thoughtspot.db"})},
    )
