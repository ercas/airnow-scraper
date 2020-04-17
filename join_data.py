#!/usr/bin/env python3
# join data produced by airnow_scraper.py

import glob
import json
import os
import pandas

def read_json(path):
    with open(path, "r") as f:
        data = json.load(f)
    return data

def join_data(root, output_directory = "."):
    data_output = os.path.join(output_directory, "data.csv")
    sites_output = os.path.join(output_directory, "sites.csv")

    pandas.json_normalize([
        read_json(path)
        for path in sorted(glob.glob(os.path.join(root, "*.json*")))
    ]).to_csv(sites_output, index = False)
    print("merged site metatdata to {}".format(sites_output))

    pandas.pandas.concat([
        pandas.read_csv(path)
        for path in sorted(glob.glob(os.path.join(root, "*.csv*")))
    ]).to_csv(data_output, index = False)
    print("merged pollutant data to {}".format(data_output))

if (__name__ == "__main__"):
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "input_directory", metavar = "INPUT_DIRECTORY",
        help = "the directory containing raw data to be merged"
    )
    parser.add_argument(
        "-o", "--output-directory", default = ".",
        help = "the directory to save merged data to"
    )
    args = parser.parse_args()

    join_data(args.input_directory, args.output_directory)
