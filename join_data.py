#!/usr/bin/env python3
""" Join data produced by airnow_scraper.py. """

import glob
import json
import os

import pandas

def read_json(path: str) -> dict:
    """ Read a JSON file.

    Args:
        path: The path to the JSON file.

    Returns:
        A dict containing the JSON data.
    """

    with open(path, "r") as input_fp:
        data = json.load(input_fp)
    return data

def join_data(root: str, output_directory: str = ".") -> None:
    """ Join scraped AirNow data.

    Args:
        root: The directory containing the data to be joined.
        output_directory: The directory to save joined data to.
    """

    data_output = os.path.join(output_directory, "data.csv")
    sites_output = os.path.join(output_directory, "sites.csv")

    pandas.json_normalize([
        read_json(path)
        for path in sorted(glob.glob(os.path.join(root, "*.json*")))
    ]).to_csv(sites_output, index=False)
    print("merged site metatdata to {}".format(sites_output))

    pandas.pandas.concat([
        pandas.read_csv(path)
        for path in sorted(glob.glob(os.path.join(root, "*.csv*")))
    ]).to_csv(data_output, index=False)
    print("merged pollutant data to {}".format(data_output))

def main() -> None:
    """ Main function. """

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_directory", metavar="INPUT_DIRECTORY",
        help="the directory containing raw data to be merged"
    )
    parser.add_argument(
        "-o", "--output-directory", default=".",
        help="the directory to save merged data to"
    )
    args = parser.parse_args()

    join_data(args.input_directory, args.output_directory)

if __name__ == "__main__":
    import argparse
    main()
