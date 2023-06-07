#!/usr/bin/env python
"""
Performs basic cleaning on the data and  save the results in Weights & Biases
"""
import os
import argparse
import logging

import pandas as pd
import wandb

logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    logger.info(f"Downloading input artifact: {args.input_artifact}")
    artifact = run.use_artifact(args.input_artifact)
    df = pd.read_csv(artifact.file())

    logger.info("Dropping price outliers")

    min_price = args.min_price
    max_price = args.max_price
    idx = df['price'].between(min_price, max_price)
    df = df[idx].copy()

    df['last_review'] = pd.to_datetime(df['last_review'])

    logger.info("Preparing artifact to upload to W&B")
    filename = args.output_artifact
    df.to_csv(filename, index=False)

    artifact = wandb.Artifact(
        name=args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(filename)
    run.log_artifact(artifact)

    logging.info(f"Artifact uploaded to W&B; will be removed locally")
    os.remove(filename)

    logger.info("Basic cleaning finished")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This steps cleans the data")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Name for the basic cleaning output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Type for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description of the artifact",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum price for rental",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum price for rental",
        required=True
    )

    args = parser.parse_args()

    go(args)
