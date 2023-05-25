#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import argparse
import logging
import wandb
import pandas as pd
import tempfile
import os
from wandb_utils.log_artifact import log_artifact


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact
    logger.info("Downloading and reading raw data")
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path, low_memory=False)


    # Range selection
    logger.info("Cleaning raw data")
    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()
    # Convert last_review to datetime
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save output artifact
    logger.info("Uploading clean data")
    filename = args.output_artifact
    df.to_csv(filename, index=False)

    log_artifact(
        args.output_artifact,
        args.output_type,
        args.output_description,
        filename,
        run,
    )
    os.remove(filename)




if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Fully-qualified name for raw data artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='Name of the produced artifact',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='Type for the produced artifacts',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='Description for the produced artifacts',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help="Minimum price to consider for the analysis",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help="Maximum price to consider for the analysis",
        required=True
    )


    args = parser.parse_args()

    go(args)
