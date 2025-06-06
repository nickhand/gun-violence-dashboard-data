"""Module for downloading and analyzing the shooting victims database."""

import gzip
import tempfile
from dataclasses import dataclass
from typing import Literal, Optional

import boto3
import carto2gpd
import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import simplejson as json
from dotenv import find_dotenv, load_dotenv
from loguru import logger
from pydantic import BaseModel, Field, validator
from shapely.geometry import Point

from . import BUCKET_NAME, DATA_DIR, EPSG
from .courts import merge as merge_court_info
from .geo import *
from .streets import StreetHotSpots
from .utils import validate_data_schema


class Geometry(Point):
    """
    Shapely point geometry.
    """

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, Point):
            raise TypeError("shapely point object required")
        return v

    @classmethod
    def __modify_schema__(cls, field_schema, field):
        pass


def upload_to_s3(data, filename):
    """Upload data to a public AWS s3 bucket."""

    # Load the credentials
    load_dotenv(find_dotenv())

    # Initialize the s3 resource
    s3 = boto3.client("s3")

    # Compress JSON
    json_str = data.to_json() + "\n"
    json_bytes = json_str.encode("utf-8")

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpfile = f"{tmpdir}/{filename}"
        with gzip.open(tmpfile, "w") as fout:
            fout.write(json_bytes)

        # Upload to s3
        s3.upload_file(
            tmpfile,
            BUCKET_NAME,
            filename,
            ExtraArgs={
                "ContentType": "application/json",
                "ContentEncoding": "gzip",
                "ACL": "public-read",
            },
        )


def carto2gpd_post(url, table_name, where=None, fields=None):
    """Query carto API with a post call"""

    # Get the fields
    if fields is None:
        fields = "*"
    else:
        if "the_geom" not in fields:
            fields.append("the_geom")
        fields = ",".join(fields)

    # Build the query
    query = f"SELECT {fields} FROM {table_name}"
    if where:
        query += f" WHERE {where}"

    # Make the request
    params = dict(q=query, format="geojson", skipfields=["cartodb_id"])
    r = requests.post(url, data=params)

    if r.status_code == 200:
        return gpd.GeoDataFrame.from_features(r.json(), crs="EPSG:4326")
    else:
        raise ValueError("Error querying carto API")


def add_geographic_info(df):
    """Add geographic info."""

    # Get a fresh copy
    df = df.copy().reset_index(drop=True)

    # The original length
    original_length = len(df)

    # Check city limits
    city_limits = get_city_limits().to_crs(df.crs)
    outside_limits = ~df.geometry.within(city_limits.squeeze().geometry)
    missing = outside_limits.sum()

    # Set missing geometry to null
    logger.info(f"{missing} shootings outside city limits")
    if missing > 0:
        df.loc[outside_limits, "geometry"] = np.nan

    # Try to replace any missing geometries from criminal incidents
    dc_key_list = ", ".join(
        df.loc[df.geometry.isnull(), "dc_key"].apply(lambda x: f"'{x}'")
    )

    # Query with a post request
    url = "https://phl.carto.com/api/v2/sql"
    table_name = "incidents_part1_part2"
    where = f"dc_key IN ( {dc_key_list} )"
    incidents = carto2gpd_post(url, table_name, where=where, fields=["dc_key"]).to_crs(
        df.crs
    )
    incidents["dc_key"] = incidents["dc_key"].astype(str)

    # Did we get any matches
    matches = len(incidents)
    logger.info(f"Found {matches} matches for {missing} missing geometries")

    # Merge
    if matches > 0:
        missing_sel = df.geometry.isnull()
        missing = df.loc[missing_sel]
        df2 = missing.drop(columns=["geometry"]).merge(
            incidents[["dc_key", "geometry"]].drop_duplicates(subset=["dc_key"]),
            on="dc_key",
            how="left",
        )
        df = pd.concat([df.loc[~missing_sel], df2]).reset_index(drop=True)

    def _add_geo_info(data, geo):
        out = gpd.sjoin(data, geo, how="left", predicate="within")

        # NOTE: sometimes this will match multiple geo boundaries
        # REMOVE THEM
        duplicated = out.index.duplicated()
        if duplicated.sum():
            out = out.loc[~duplicated]

        return out.drop(labels=["index_right"], axis=1)

    # Add geographic columns
    geo_funcs = [
        get_zip_codes,
        get_police_districts,
        get_council_districts,
        get_neighborhoods,
        get_school_catchments,
        get_pa_house_districts,
        get_pa_senate_districts,
    ]
    for geo_func in geo_funcs:
        df = df.pipe(_add_geo_info, geo_func().to_crs(df.crs))

    # if geo columns are missing, geometry should be empty point
    df.loc[df["neighborhood"].isnull(), "geometry"] = np.nan

    # Check the length
    if len(df) != original_length:
        raise ValueError("Length of data has changed; this shouldn't happen!")

    return df


def load_existing_shootings_data():
    """Load existing shootings data."""
    files = sorted((DATA_DIR / "processed").glob("shootings_20*.json"))
    return pd.concat([gpd.read_file(f) for f in files])


class ShootingVictimsSchema(BaseModel):
    """Schema for the shooting victims dataset."""

    dc_key: str = Field(
        title="Incident number",
        description="The unique incident number assigned by the Police Department.",
    )
    race: Literal["B", "H", "W", "A", "Other/Unknown"] = Field(
        title="Race/Ethnicity",
        description=(
            "The race/ethnicity of the shooting victim. "
            "Allowed values include: 'B' = Black, Non-Hispanic, 'H' = Hispanic, "
            "'W' = White, Non-Hispanic, 'A' = Asian, and 'Other/Unknown'"
        ),
    )
    sex: Literal["M", "F"] = Field(
        title="Sex", description="The sex of the shooting victim."
    )
    fatal: Literal[True, False] = Field(
        title="Fatal?", description="Whether the incident was fatal."
    )
    date: str = Field(
        title="Date",
        description="The datetime of the incident in the format 'Y/m/d H:M:S'",
    )
    age_group: Literal[
        "Younger than 18", "18 to 30", "31 to 45", "Older than 45", "Unknown"
    ] = Field(tile="Age group", description="The victim's age group (or unknown).")

    has_court_case: Literal[True, False] = Field(
        title="Associated Court Case?",
        description="Does the incident number have an associated court case?",
    )

    # Not all ages are known
    age: Optional[float] = Field(
        default=None,
        title="Age",
        description="The victim's age; missing in some cases.",
    )

    # Optional geographic add-ons
    geometry: Optional[Geometry] = Field(
        default=None,
        description="The lat/lng point location of the shooting incident; missing in some cases.",
    )
    street_name: Optional[str] = Field(
        default=None,
        title="Street name",
        description="The name of the street the incident occurred on, if available.",
    )
    block_number: Optional[float] = Field(
        default=None,
        title="Block number",
        description="The block number where the incident occurred, if available.",
    )
    zip_code: Optional[str] = Field(
        default=None,
        title="ZIP Code",
        description="The ZIP code where the incident occurred, if available.",
    )
    council_district: Optional[str] = Field(
        default=None,
        title="Council district",
        description="The council district where the incident occurred, if available.",
    )
    police_district: Optional[str] = Field(
        default=None,
        title="Police district",
        description="The police district where the incident occurred, if available.",
    )
    neighborhood: Optional[str] = Field(
        default=None,
        title="Neighborhood name",
        description="The name of the neighborhood where the incident occurred, if available.",
    )
    school_name: Optional[str] = Field(
        default=None,
        title="School catchment",
        description="The elementary school catchment where the incident occurred, if available.",
    )
    house_district: Optional[str] = Field(
        default=None,
        title="PA House district",
        description="The PA House district where the incident occurred, if available.",
    )
    senate_district: Optional[str] = Field(
        default=None,
        title="Block number",
        description="The PA Senate district where the incident occurred, if available.",
    )
    segment_id: Optional[str] = Field(
        default=None,
        title="Block number",
        description="The ID of the street segment where the incident occurred, if available.",
    )

    @validator("dc_key")
    def verify_dc_key(cls, v):
        if not isinstance(v, str):
            assert not np.isnan(v), "cannot be NaN"
        else:
            assert not v.endswith(".0"), "bad string formatting"
        return v


@dataclass
class ShootingVictimsData:
    """Class for downloading and analyzing the shooting victims
    database from Open Data Philly."""

    debug: bool = False
    ignore_checks: bool = False

    ENDPOINT: str = "https://phl.carto.com/api/v2/sql"
    TABLE_NAME: str = "shootings"

    @validate_data_schema(ShootingVictimsSchema)
    def get(self) -> gpd.GeoDataFrame:
        """Download and return the formatted data."""

        if self.debug:
            logger.debug("Downloading shooting victims database")

        # Raw data from carto
        df = carto2gpd.get(self.ENDPOINT, self.TABLE_NAME)

        # Remove officer involved
        df = df.query("officer_involved == 'N'")

        # Verify DC key first
        missing_dc_keys = df["dc_key"].isnull()
        if missing_dc_keys.sum() and not self.ignore_checks:
            n = missing_dc_keys.sum()
            raise ValueError(f"Found {n} rows with missing DC keys")

        # Format
        df = (
            df.assign(
                time=lambda df: df.time.replace("<Null>", np.nan).fillna("00:00:00"),
                date=lambda df: pd.to_datetime(
                    df.date_.str.slice(0, 10).str.cat(df.time, sep=" ")
                ),
                dc_key=lambda df: df.dc_key.astype(float).astype(int).astype(str),
                year=lambda df: df.date.dt.year,
                race=lambda df: df.race.fillna("Other/Unknown"),
                age=lambda df: df.age.astype(float),
                age_group=lambda df: np.select(
                    [
                        df.age <= 17,
                        (df.age > 17) & (df.age <= 30),
                        (df.age > 30) & (df.age <= 45),
                        (df.age > 45),
                    ],
                    ["Younger than 18", "18 to 30", "31 to 45", "Older than 45"],
                    default="Unknown",
                ),
                fatal=lambda df: df.fatal.apply(lambda x: True if x == 1 else False),
            )
            .assign(
                race=lambda df: df.race.where(df.latino != 1, other="H"),
            )
            .drop(labels=["point_x", "point_y", "date_", "time", "objectid"], axis=1)
            .sort_values("date", ascending=False)
            .reset_index(drop=True)
            .assign(
                date=lambda df: df.date.dt.strftime("%Y/%m/%d %H:%M:%S")
            )  # Convert date back to string
            .to_crs(epsg=EPSG)
        )

        # Add the other category for race/ethnicity
        main_race_categories = ["H", "W", "B", "A"]
        sel = df.race.isin(main_race_categories)
        df.loc[~sel, "race"] = "Other/Unknown"

        # Remove dates in the future
        future_dates = pd.to_datetime(df.date) > pd.Timestamp.now()
        if future_dates.sum() > 0:
            logger.warning(f"Found {future_dates.sum()} future date(s) in the data")
            df = df.loc[~future_dates].reset_index(drop=True)

        # CHECKS
        if not self.ignore_checks:
            old_df = load_existing_shootings_data()
            TOLERANCE = 100

            # Check for too many rows
            if len(df) - len(old_df) > TOLERANCE:
                logger.info(f"Length of new data: {len(df)}")
                logger.info(f"Length of old data: {len(old_df)}")
                raise ValueError(
                    "New data seems to have too many rows...please manually confirm new data is correct."
                )

            # Check for too few rows
            TOLERANCE = 10
            if len(old_df) - len(df) > TOLERANCE:
                logger.info(f"Length of new data: {len(df)}")
                logger.info(f"Length of old data: {len(old_df)}")
                raise ValueError(
                    "New data seems to have too few rows...please manually confirm new data is correct."
                )

        # Add geographic info
        df = add_geographic_info(df)

        # Handle NaN/None
        df = df.assign(
            geometry=lambda df: df.geometry.fillna(Point()),
        )

        # Value-added info for hot spots and court info
        hotspots = StreetHotSpots(debug=self.debug)
        df = (
            df.pipe(hotspots.merge)
            .pipe(merge_court_info, debug=self.debug)
            .assign(segment_id=lambda df: df.segment_id.replace("", np.nan))
        )

        # Trim to the schema fields
        fields = ShootingVictimsSchema.__fields__.keys()
        df = df[fields]

        return df

    def save(self, data):
        """Save annual, processed data files."""

        # Get the years from the date
        years = pd.to_datetime(data["date"]).dt.year

        # Get unique years
        # IMPORTANT: this must be int so it is JSON serializable
        unique_years = [int(year) for year in sorted(np.unique(years), reverse=True)]
        json.dump(unique_years, (DATA_DIR / "processed" / "data_years.json").open("w"))

        # Save each year's data to separate file
        for year in unique_years:
            if self.debug:
                logger.debug(f"Saving {year} shootings as a GeoJSON file")

            # Get data for this year
            # Save in EPSG = 4326
            data_yr = data.loc[years == year].to_crs(epsg=4326)

            data_yr.to_file(
                DATA_DIR / "processed" / f"shootings_{year}.json",
                driver="GeoJSON",
                index=False,
            )

            # Save to s3
            upload_to_s3(data_yr, f"shootings_{year}.json")
