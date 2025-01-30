"""
Scrape the total homicide count from the Philadelphia Police Department's 
Crime Stats website.
"""

from dataclasses import dataclass
from datetime import date

from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
import requests
import pandas as pd
from bs4 import BeautifulSoup
from cached_property import cached_property
from loguru import logger

from . import DATA_DIR


def get_webdriver(debug=False):
    """
    Initialize a selenium web driver with the specified options.

    Parameters
    ----------
    debug: bool
        Whether to use the headless version of Chrome
    """
    # Create the options
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    if not debug:
        options.add_argument("--headless")

    return webdriver.Chrome(options=options)


@dataclass
class PPDHomicideTotal:
    """Total number of homicides scraped from the Philadelphia Police
    Department's website.

    This provides:
        - Annual totals since 2007 for past years.
        - Year-to-date homicide total for the current year.

    Source
    ------
    https://www.phillypolice.com/crime-maps-stats/
    """

    debug: bool = False

    URL = "https://www.phillypolice.com/crime-data/crime-statistics/"

    def __post_init__(self):

        # Get the driver
        driver = get_webdriver(debug=self.debug)

        # Navigate to the page
        driver.get(self.URL)

        # Wait for the tables to load
        delay = 5  # seconds
        try:
            WebDriverWait(driver, delay).until(
                EC.presence_of_element_located((By.CLASS_NAME, "container-crime"))
            )

            # Get the page source
            self.soup = BeautifulSoup(driver.page_source, "html.parser")

        except TimeoutException:
            raise ValueError("Page took too long to load")

    @cached_property
    def as_of_date(self):
        """The current "as of" date on the page."""

        # Get the month and year from YTD table
        month_day = (
            self.soup.select_one(".crime-title")
            .select_one("span.crime-text")
            .text.split("to")[-1]
            .strip()
        )

        # Get the year
        year = int(
            self.soup.select_one(".container-crime.year-to-date")
            .select_one(".data-heading")
            .text.strip()
        )
        return pd.to_datetime(f"{month_day} {year} 11:59:00")

    @cached_property
    def annual_totals(self):
        """The annual totals for homicides in Philadelphia."""

        # Years
        years = [
            int(div.text.strip())
            for div in self.soup.select_one(".container-crime.full-year").select(
                ".data-heading"
            )
        ]

        # YTD totals
        totals = [
            int(div.text.strip())
            for div in self.soup.select_one(".container-crime.full-year").select(
                ".counted-data"
            )
        ]
        # Return ytd totals, sorted in ascending order
        out = pd.DataFrame({"year": years, "annual": totals})
        return out.sort_values("year", ascending=False)

    @cached_property
    def ytd_totals(self):
        """The year-to-date totals for homicides in Philadelphia."""

        # Years
        years = [
            int(div.text.strip())
            for div in self.soup.select_one(".container-crime.year-to-date").select(
                ".data-heading"
            )
        ]

        # YTD totals
        totals = [
            int(div.text.strip())
            for div in self.soup.select_one(".container-crime.year-to-date").select(
                ".counted-data"
            )
        ]

        # Return ytd totals, sorted in ascending order
        out = pd.DataFrame({"year": years, "ytd": totals})
        return out.sort_values("year", ascending=False)

    @property
    def path(self):
        return DATA_DIR / "raw" / "homicide_totals_daily.csv"

    def get(self):
        """Get the shooting victims data, either loading
        the currently downloaded version or a fresh copy."""

        # Load the database of daily totals
        df = pd.read_csv(self.path, parse_dates=[0])

        # Make sure it's in ascending order by date
        return df.sort_values("date", ascending=True)

    def update(self, force=False):
        """Update the local data via scraping the PPD website."""

        # Load the database
        database = self.get()

        # Latest database date
        latest_database_date = database.iloc[-1]["date"]

        # Remove last row
        if self.as_of_date == latest_database_date:
            database = database.drop(index=database.index[-1])

        # Update
        if self.debug:
            logger.debug("Parsing PPD website to update YTD homicides")

        # Merge annual totals (historic) and YTD (current year)
        data = pd.merge(self.annual_totals, self.ytd_totals, on="year", how="outer")

        # Add new row to database
        YTD = self.ytd_totals.iloc[0]["ytd"]
        database.loc[len(database)] = [self.as_of_date, YTD]

        # Sanity check on new total
        new_homicide_total = database.iloc[-1]["total"]
        old_homicide_total = database.iloc[-2]["total"]
        new_year = database.iloc[-1]["date"].year
        old_year = database.iloc[-2]["date"].year
        if (
            not force
            and new_homicide_total < old_homicide_total
            and (new_year == old_year)
        ):
            raise ValueError(
                f"New YTD homicide total ({new_homicide_total}) is less than previous YTD total ({old_homicide_total})"
            )

        # Save it
        path = DATA_DIR / "processed" / "homicide_totals.json"
        data.set_index("year").to_json(path, orient="index")

        # Save it
        if self.debug:
            logger.debug("Updating PPD homicides data file")

        # Drop duplicates and save
        database.drop_duplicates(subset=["date"], keep="last").to_csv(
            self.path, index=False
        )
