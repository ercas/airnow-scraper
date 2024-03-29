#!/usr/bin/env python3
# pylint: disable=too-few-public-methods
# pylint: disable=too-many-arguments

""" Scrape data from EPA AirNow web interface.

This scraper is built on the following link which is sent as a GET request from
the form on https://www.epa.gov/outdoor-air-quality-data/download-daily-data:

    https://www3.epa.gov/cgi-bin/broker?_service=data&_debug=0&_program=dataprog.ad_data_daily_airnow.sas&querytext=&areaname=&areacontacts=&areasearchurl=&typeofsearch=epa&result_template=2col.ftl&poll=88101','88502&year=2020&state=53&cbsa=-1&county=-1&site=530330030

The same link with nonessential params stripped, determined by trial and error:

    https://www3.epa.gov/cgi-bin/broker?_service=data&_program=dataprog.ad_data_daily_airnow.sas&poll=88101,88502&year=2020&site=530330030

The above link serves as the basis for this scraper.
"""

import datetime
import gzip
import json
import os
import time
import typing

import bs4
import requests

# codes for criteria air pollutant data - these could be determined from the
# AQS API, but certain combinations are necessary for the script to work e.g.
# PM needs both 88101 and 88502 instead of just 88101. the codes here have been
# determined by varying the form inputs and intercepting the resulting network
# requests for data links
POLLUTANTS = {
    "CO": "42101",
    "Pb": "12128,14129,85129",
    "NO2": "42602",
    "O3": "44201",
    "PM10": "81102",
    "PM2.5": "88101,88502",
    "SO2": "42401"
}

STATE_FIPS_CODES = {
    "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08",
    "CT": "09", "DE": "10", "DC": "11", "FL": "12", "GA": "13", "HI": "15",
    "ID": "16", "IL": "17", "IN": "18", "IA": "19", "KS": "20", "KY": "21",
    "LA": "22", "ME": "23", "MD": "24", "MA": "25", "MI": "26", "MN": "27",
    "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32", "NH": "33",
    "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
    "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46",
    "TN": "47", "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53",
    "WV": "54", "WI": "55", "WY": "56"
}
STATE_FIPS_CODES_REVERSED = {  # not really a constant but it basically is
    value: key
    for (key, value) in STATE_FIPS_CODES.items()
}

AQS_DEFAULT_EMAIL = "test@aqs.api"
AQS_DEFAULT_KEY = "test"

DEFAULT_OUTPUT_DIR = "./airnow_data/"

DOWNLOAD_CHUNK_SIZE = 8192

DEFAULT_SLEEP_BETWEEN_REQUESTS = 5


class AqsApiError(Exception):
    """ An exception indicating EPA AQS API errors """


class AirNowCgiError(Exception):
    """ An exception indicating an error in the EPA AirNow SAS script """


# derived from https://stackoverflow.com/a/16696317
def download_file(url: str, output_file: str, use_gzip: bool = False) -> None:
    """ Download a file to a given path.

    Args:
        url: The path of the file to be downloaded.
        output_file: The path where the file should be saved.
        use_gzip: Compress the data upon receipt. This does not affect the
            extension of output_file and .gz should be appended to that
            argument if this is used.
    """

    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        if use_gzip:
            output_fp = gzip.open(output_file, "w")
        else:
            output_fp = open(output_file, "wb")
        for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
            if chunk:
                output_fp.write(chunk)
        output_fp.close()


class AqsSite:
    """ Class to ecnapsulate AQS API site data.

    Class to encapsulate AQS API site data to enable type checking for the
    scraper and avoiding recalculation of the EPA site identifier; not intended
    to be used directly.

    Attributes:
        site_id: The EPA monitoring site ID
        json: JSON data describing the site, obtained from the EPA AQS API
    """

    def __init__(self, site_data: dict):
        """ Initialize AqsSite object.

        From https://www.epa.gov/outdoor-air-quality-data/about-air-data-reports:

            The AQS database identification code for an air monitoring site. An
            AQS site ID has the following parts:

            * FIPS state code (2 digits)
            * FIPS county code (3 digits) - FIPS is the acronym for Federal
              Information Processing Standards, which defines codes used in
              most U.S. government information systems.
            * AQS site code (4 characters) - an arbitrary code that identifies
              a particular monitoring site within a county

        Args:
            site_data: JSON data describing the site, obtained from the EPA
                AQS API.
        """

        self.site_id = "".join(
            site_data[attribute]
            for attribute in ["state_code", "county_code", "site_number"]
        )
        self.json = site_data

    def __repr__(self):
        return "AqsSite: EPA Site ID {}-{}-{}: {}, {}, {} {}".format(*tuple(
            self.json[attribute]
            for attribute in [
                "state_code", "county_code", "site_number",
                "address", "state_name", "county_name", "cbsa_code"
            ]
        ))


class Scraper:
    """ Main AirNow scraper class.

    Attributes:
        output_directory: The path where downloaded data is being saved to.
        email: The email to use for EPA AQS API authentication.
        key: The key to use for EPA AQS API authentication.
        verbose: A boolean toggling verbosity.
        use_compression: A boolean toggling whether or not downloaded data
            should be compressed on receipt.
    """

    def __init__(self,
                 output_directory: str = DEFAULT_OUTPUT_DIR,
                 email: str = AQS_DEFAULT_EMAIL,
                 key: str = AQS_DEFAULT_KEY,
                 verbose: bool = True,
                 use_compression: bool = True):
        """ Initialize AirNow scraper class.

        Args:
            output_directory: The path where downloaded data is being saved to.
            email: The email to use for EPA AQS API authentication.
            key: The key to use for EPA AQS API authentication.
            verbose: A boolean toggling verbosity.
            use_compression: A boolean toggling whether or not downloaded data
                should be compressed on receipt.
        """

        self.output_directory = output_directory
        self.email = email
        self.key = key
        self.verbose = verbose
        self.use_compression = use_compression

        if (email == AQS_DEFAULT_EMAIL) or (key == AQS_DEFAULT_KEY):
            print("warning: using default AQS credentials")

        if not os.path.isdir(output_directory):
            os.makedirs(output_directory)
        print("saving output to {}".format(output_directory))

    def print(self, *args, **kwargs) -> None:
        """ Passthrough to the print function if self.verbose is True. """
        if self.verbose:
            print(datetime.datetime.now().isoformat(), *args, **kwargs)

    def sleep(self, sleep_time: float = DEFAULT_SLEEP_BETWEEN_REQUESTS) -> None:
        """ Sleep between requests.

        Args:
            sleep_time: The number of seconds to sleep for.
        """

        self.print("> sleeping {} seconds".format(sleep_time))
        time.sleep(sleep_time)

    def list_monitoring_sites(self,
                              pollutant: str,
                              state: str,
                              start_date: datetime.datetime,
                              end_date: datetime.datetime
                              ) -> typing.List[AqsSite]:
        """ Make a request to the EPA AQS API's monitoring site list endpoint.

        Args:
            pollutant: The pollutant to search for sites with.
            state: A FIPS state code.
            start_date, end_date: The time window to search for sites between.

        Returns:
            A list of AqsSite objects, each corresponding to an individual
            monitoring site.
        """

        if pollutant not in POLLUTANTS:
            print("invalid pollutant {}; available pollutants: {}".format(
                pollutant, ", ".join(POLLUTANTS.keys())
            ))
            return []
        response = requests.get(
            "https://aqs.epa.gov/data/api/monitors/byState",
            params={
                "email": self.email,
                "key": self.key,
                "param": POLLUTANTS[pollutant].split(",")[0],
                "bdate": start_date.strftime("%Y%m%d"),
                "edate": end_date.strftime("%Y%m%d"),
                "state": state
            }
        )
        self.sleep()

        data = response.json()
        if data["Header"][0]["status"] == "Failed":
            print(data["Header"])
            raise AqsApiError(
                "incorrect parameters or AQS API was updated"
            )

        result = data["Data"]
        self.print("found {} active monitors in state {}".format(
            len(result), STATE_FIPS_CODES_REVERSED[state]
        ))
        return [AqsSite(site) for site in result]

    def get_data_url(self,
                     pollutant: str,
                     year: int,
                     site: AqsSite) -> str:
        """ Send a request to the EPA AirNow form for criteria pollutant data.

        Args:
            pollutant: The pollutant to get data for.
            year: The year to get data for.
            site: An AqsSite object containing metadata for the site whose data
                we are requesting.

        Returns:
            The generated URL where the data is located.
        """

        params: typing.Dict[str, typing.Union[int, str]] = {
            "_service": "data",
            "_program": "dataprog.ad_data_daily_airnow.sas",
            "poll": POLLUTANTS[pollutant],
            "year": year,
            "site": site.site_id
        }
        request = requests.get("https://www3.epa.gov/cgi-bin/broker", params)
        self.sleep()
        soup = bs4.BeautifulSoup(request.content, "lxml")

        if "The following data link is active" not in soup.body.text:
            raise AirNowCgiError("incorrect parameters or EPA AirNow SAS script was updated")

        return soup.find("a")["href"]

    # apologies for code repetitiveness; want to be sure args are type checked
    def get_data(self,
                 pollutant: str,
                 year: int,
                 site: AqsSite,
                 output_file: str = None) -> None:
        """ Wrapper around self.get_data_url that also downloads the data.

        Args:
            pollutant: The pollutant to get data for.
            year: The year to get data for.
            site: An AqsSite object containing metadata for the site whose data
                we are requesting.
            output_file: The path where data should be saved.
        """

        if not output_file:
            output_file = os.path.join(
                self.output_directory,
                "{}_{}_{}_{}.csv.gz".format(
                    site.site_id, pollutant, year,
                    datetime.datetime.now().strftime("%Y%m%d")
                ),
            )

        state_abbr = STATE_FIPS_CODES_REVERSED[site.json["state_code"]]
        if os.path.isfile(output_file):
            self.print("skipping:    {} ({}, {}) - already exists".format(
                site.site_id, state_abbr, year
            ))
        else:
            self.print("downloading: {} ({}, {})".format(
                site.site_id, state_abbr, year
            ))
            try:
                self.print("> downloading url")
                url = self.get_data_url(pollutant, year, site)
                self.print("> downloading file")
                download_file(url, output_file, use_gzip=self.use_compression)
                self.print("> success: {}".format(output_file))
                self.sleep()
            except AirNowCgiError:
                self.print("> error")

        self.print("")

    # given a pollutant, a list of state FIPS codes, and a start and end date,
    # download all of the relevant data
    def scrape(self,
               pollutant: str,
               states: typing.List[str],
               start_date: datetime.datetime,
               end_date: datetime.datetime) -> None:
        """ Main AirNow scraper functionality.

        Given a pollutant, a list of state FIPS codes, and a start and end
        date, download all of the relevant data.

        Args:
            pollutant: The pollutant to get data for.
            states: A list of FIPS state codes.
            start_date, end_date: The time window to search for sites between.
        """

        # TODO: may be better to separate site scraping and data scraping into
        # two distinct stages
        for state in states:
            sites = self.list_monitoring_sites(
                pollutant, state, start_date, end_date
            )
            for site in sites:

                site_output_file = os.path.join(
                    self.output_directory,
                    "{}_{}.json".format(
                        site.site_id,
                        datetime.datetime.now().strftime("%Y%m%d")
                    )
                )
                if not os.path.isfile(site_output_file):
                    self.print("writing metadata for site {}".format(
                        site.site_id
                    ))
                    self.print("")
                with open(site_output_file, "w") as output_fp:
                    json.dump(site.json, output_fp, indent=4)

                for year in range(start_date.year, end_date.year + 1):
                    self.get_data(pollutant, year, site)

        self.print("finished")


def main() -> None:
    """ Main scraper functionality. """

    parser = argparse.ArgumentParser()
    required = parser.add_argument_group("required named arguments")
    required.add_argument(
        "-p", "--pollutant", required=True,
        help="the pollutant to download data for; must be one of the"
             " following: " + ", ".join(POLLUTANTS.keys())
    )
    required.add_argument(
        "-s", "--start-date", required=True,
        help="the start date for the selected data range, or \"today\"; must"
             " be parsable using dateutil.parser.parse"
    )
    required.add_argument(
        "-e", "--end-date", required=True,
        help="the end date for the selected data range, subject to the same"
             " requirements as --start-date"
    )
    parser.add_argument(
        "-o", "--output_directory", default=DEFAULT_OUTPUT_DIR,
        help="the directory to save data to; the default is {}".format(
            DEFAULT_OUTPUT_DIR
        )
    )
    parser.add_argument(
        "-S", "--states", default=list(STATE_FIPS_CODES.values()),
        help="the FIPS codes or two-letter abbreviations  of states to scrape"
             " data for, separated by commas; by default, the default action"
             " is to download data from all states"
    )
    parser.add_argument(
        "-E", "--email", default=AQS_DEFAULT_EMAIL,
        help="the email to use to query the EPA's AQS API; if no email is"
             " given, the example email is used (may break)"
    )
    parser.add_argument(
        "-k", "--key", default=AQS_DEFAULT_KEY,
        help="the authentication key for the provided email"
    )
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="toggle verbose output"
    )

    # args = parser.parse_args("-p PM2.5 -s 2020-01-01 -e today -v".split())
    args = parser.parse_args()

    # check: selected a valid pollutant
    if args.pollutant.upper() in POLLUTANTS:
        args.pollutant = args.pollutant.upper()
    else:
        raise Exception("invalid pollutant {} selected; see --help".format(
            args.pollutant
        ))

    # date conversions; dateutil.parser.parse will throw necessary exceptions
    for attr in ["start_date", "end_date"]:
        dt_str = getattr(args, attr)
        if dt_str == "today":
            setattr(args, attr, datetime.datetime.now())
        else:
            try:
                setattr(args, attr, dateutil.parser.parse(dt_str))
            except ValueError:
                raise Exception("invalid date string {}; see --help".format(
                    dt_str
                ))

    # validate all FIPS codes
    fips_codes = []
    valid_fips_codes = set(STATE_FIPS_CODES.values())
    if isinstance(args.states, str):
        args.states = args.states.split(",")
    for state in args.states:

        # ok: state provided as a fips code
        if state in valid_fips_codes:
            fips_codes.append(state)

        # maybe not ok: try to guess from the STATE_FIPS_CODES dict
        else:
            state_normalized = state.upper()
            if state_normalized in STATE_FIPS_CODES:
                fips_codes.append(STATE_FIPS_CODES[state_normalized])
            else:
                raise Exception("could not understand state {}; see --help".format(
                    state
                ))
    args.states = fips_codes

    # initialize scraper and start scraping
    scraper = Scraper(**{
        name: getattr(args, name)
        for name in ["output_directory", "email", "key", "verbose"]
    })
    scraper.scrape(**{
        name: getattr(args, name)
        for name in ["pollutant", "start_date", "end_date", "states"]
    })


if __name__ == "__main__":
    import argparse
    import dateutil.parser

    main()
