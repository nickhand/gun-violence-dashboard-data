"""The main command line module that defines the "gv_dashboard_data" tool."""

import datetime

import click
import simplejson as json
from loguru import logger

from . import DATA_DIR
from .courts import run as run_courts_scraper
from .geo import (
    get_council_districts,
    get_neighborhoods,
    get_pa_house_districts,
    get_pa_senate_districts,
    get_police_districts,
    get_school_catchments,
    get_zip_codes,
)
from .homicides import PPDHomicideTotal
from .shootings import ShootingVictimsData, load_existing_shootings_data
from .streets import StreetHotSpots


@click.group()
@click.version_option()
def cli():
    """Processing data for the Controller's Office gun violence dashboard.

    https://nickhand.dev/philly-gun-violence-map
    """
    pass


@cli.command()
@click.option("--debug", is_flag=True)
def save_geojson_layers(debug=False):
    """Save the various geojson layers needed in the dashboard."""

    # ------------------------------------------------
    # Part 1: Hot spot streets
    # -------------------------------------------------
    hotspots = StreetHotSpots(debug=debug)
    hotspots.save()

    # Functions
    geo_funcs = [
        get_zip_codes,
        get_police_districts,
        get_council_districts,
        get_neighborhoods,
        get_school_catchments,
        get_pa_house_districts,
        get_pa_senate_districts,
    ]

    for func in geo_funcs:

        tag = func.__name__.split("get_")[-1]
        filename = f"{tag}.geojson"
        path = DATA_DIR / "processed" / "geo" / filename

        if debug:
            logger.debug(f"Saving {filename}")

        func().to_crs(epsg=4326).to_file(path, driver="GeoJSON")


@cli.command()
@click.option("--debug", is_flag=True, help="Whether to log debug statements.")
@click.option(
    "--ignore-checks", is_flag=True, help="Whether to ignore any validation checks."
)
@click.option(
    "--homicides-only", is_flag=True, help="Whether to process the Homicide data."
)
@click.option(
    "--shootings-only", is_flag=True, help="Whether to process the shooting data."
)
@click.option(
    "--force-homicide-update",
    is_flag=True,
    help="Whether to force the homicide update.",
)
def daily_update(
    debug=False,
    ignore_checks=False,
    homicides_only=False,
    shootings_only=False,
    force_homicide_update=False,
):
    """Run the daily pre-processing update.

    This runs the following steps:

        1. Download a fresh copy of the shooting victims database.

        2. Merge data for hot spot blocks.

        3. Merge data for court information.

        4. Save the processed shooting victims database.

        5. Save the cumulative daily shooting victims total.

        6. Scrape and save the homicide count from the PPD's website.
    """
    # Do all parts
    process_all = not (homicides_only or shootings_only)

    # Initialize meta
    meta = {}
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ------------------------------------------------------
    # Part 1: Homicide count scraped from PPD
    # ------------------------------------------------------
    if process_all or homicides_only:

        # Run the update
        homicide_count = PPDHomicideTotal(debug=debug)
        homicide_count.update(force=force_homicide_update)

        # Update the meta
        meta["last_updated_homicides"] = now

    # ---------------------------------------------------
    # Part 2: Main shooting victims data file
    # ---------------------------------------------------
    if process_all or shootings_only:
        victims = ShootingVictimsData(debug=debug, ignore_checks=ignore_checks)
        data = victims.get()

        # Save victims data to annual files
        victims.save(data)

        # Update the meta
        meta["last_updated_shootings"] = now

    # Update meta data
    meta_path = DATA_DIR / "meta.json"
    existing_meta = json.load(meta_path.open(mode="r"))

    # Remove old key
    if "last_updated" in existing_meta:
        existing_meta.pop("last_updated")

    # Add new info
    existing_meta.update(meta)

    # Save the download time
    json.dump(existing_meta, meta_path.open(mode="w"))


@cli.command()
@click.option(
    "--ntasks",
    type=int,
    default=1,
    help="The number of tasks to use on AWS",
)
@click.option(
    "--sleep",
    default=2,
    help="Total waiting time b/w scraping calls (in seconds)",
    type=int,
)
@click.option("--debug", is_flag=True, help="Whether to log debug statements.")
@click.option("--dry-run", is_flag=True, help="Do not save the results; dry run only.")
@click.option(
    "--sample",
    type=int,
    default=None,
    help="Only run a random sample of incident numbers.",
)
@click.option(
    "--log-freq",
    default=10,
    help="Log frequency within loop of scraping",
    type=int,
)
@click.option(
    "--seed",
    type=int,
    default=42,
    help="Random seed for sampling",
)
def scrape_courts_portal(
    ntasks=1, sleep=2, debug=False, sample=None, dry_run=False, log_freq=10, seed=42
):
    """
    Scrape courts information from the PA's Unified Judicial System's portal.
    """
    # Load the existing data
    shootings = load_existing_shootings_data()

    # Run the scraper
    run_courts_scraper(
        shootings,
        dry_run=dry_run,
        sample=sample,
        log_freq=log_freq,
        seed=seed,
        sleep=sleep,
        ntasks=ntasks,
        debug=debug,
    )


if __name__ == "__main__":
    cli(prog_name="gv_dashboard_data")
