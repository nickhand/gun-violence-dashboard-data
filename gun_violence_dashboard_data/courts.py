"""Scrape court information from the PA's Unified Judicial System portal."""

from datetime import datetime

import click
import pandas as pd
import simplejson as json
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from phl_courts_scraper_batch.__main__ import scrape
from s3fs import S3FileSystem

from . import BUCKET_NAME, DATA_DIR

# This is where scraping results are saved
DATA_PATH = DATA_DIR / "processed" / "scraped_courts_data.csv"


def run(
    data,
    dry_run=False,
    sample=None,
    log_freq=10,
    seed=42,
    sleep=2,
    ntasks=10,
    debug=False,
):
    """Run the courts scraper."""

    # Load the environment variables
    load_dotenv(find_dotenv())

    # Get the unique set of DC numbers
    incident_numbers = data[["dc_key"]].drop_duplicates()

    # Load any existing data
    if DATA_PATH.exists():

        # Existing
        existing = pd.read_csv(DATA_PATH, dtype={"dc_key": str})

        # NOTE: We can remove incident numbers that we know have a court case already
        data_to_remove = existing[existing["has_court_case"] == True]

        # Remove these from the incident numbers
        sel = incident_numbers["dc_key"].isin(data_to_remove["dc_key"])
        incident_numbers = incident_numbers[~sel]

    # Log
    logger.info(f"Scraping {len(incident_numbers)} incident numbers")

    # Get the folder on s3 for this run
    date_string = datetime.today().strftime("%y-%m-%d")
    s3_subfolder = f"courts-data/{date_string}"
    input_filename = f"s3://{BUCKET_NAME}/{s3_subfolder}/incident_numbers.csv"

    # Initialize the s3 file system
    s3 = S3FileSystem()

    # Save the incident numbers to s3
    with s3.open(input_filename, "w") as f:
        incident_numbers.to_csv(f, header=None)

    # Output folder
    output_folder = f"s3://{BUCKET_NAME}/{s3_subfolder}/results"

    # Loag
    logger.info(f"Uploaded incident numbers to {input_filename}")
    logger.info(f"Output will be saved to {output_folder}")

    # Set up the arguments
    kwargs = dict(
        flavor="portal",
        input_filename=input_filename,
        output_folder=output_folder,
        search_by="Incident Number",
        browser="firefox",
        dry_run=dry_run,
        sample=sample,
        log_freq=log_freq,
        seed=seed,
        sleep=sleep,
        aws=True,
        ntasks=ntasks,
        no_wait=False,
        debug=debug,
    )

    # Call the scrape function
    ctx = click.Context(scrape)
    ctx.invoke(scrape, **kwargs)

    # Invalidate cache
    s3.invalidate_cache()

    # Get the scraped results
    with s3.open(f"{output_folder}/portal_results.json", "r") as f:
        results = json.load(f)

    # Get the input incident numbers
    with s3.open(f"{output_folder}/portal_input.csv", "r") as f:
        output = pd.read_csv(f, header=None, names=["dc_key"], dtype=str)

    # Extract the dc_numbers from the results
    dc_numbers_with_cases = (
        pd.DataFrame(
            {"dc_key": ["20" + rr["dc_number"] for r in results for rr in r]}, dtype=str
        )
        .drop_duplicates()
        .assign(has_court_case=True)
    )

    # Combine the results!
    output = output.merge(dc_numbers_with_cases, on="dc_key", how="left").assign(
        has_court_case=lambda df: df.has_court_case.fillna(False)
    )

    # Update the saved data
    output.to_csv(DATA_PATH, index=False)

    # Return
    return output


def merge(data, debug=False):
    """Merge courts data."""

    # Load existing data
    existing = pd.read_csv(DATA_PATH, dtype={"dc_key": str})

    if debug:
        logger.debug("Merging in court case information")

    # Make a copy
    out = data.copy()

    # Return the merge and fill missing ones with False
    return out.merge(existing, on="dc_key", how="left").assign(
        has_court_case=lambda df: df["has_court_case"].fillna(False)
    )
