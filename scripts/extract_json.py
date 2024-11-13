import os
import pickle
import re
from pathlib import Path

import boto3
from dotenv import find_dotenv, load_dotenv

print("setting up clients...")

load_dotenv(find_dotenv())

s3_client = boto3.client("s3")
glue_client = boto3.client("glue")


def list_s3_objects(bucket_name: str, prefix: str = ""):
    """List objects in an S3 bucket with an optional prefix."""
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")

    # Paginate through all objects
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if "Contents" in page:
            for obj in page["Contents"]:
                objects.append(obj["Key"])  # Append each object's key (file path)

    return objects


def check_aws_account(expected_account_name: str):
    session = boto3.Session()
    sts_client = session.client("sts")
    account_id = sts_client.get_caller_identity()["Account"]
    expected_account_id = os.environ.get(expected_account_name)

    if account_id != expected_account_id:
        raise ValueError(f"Not running in expected account: {expected_account_name}")


check_aws_account("analytical-platform-data-production")


if __name__ == "__main__":
    # Example usage
    print("starting...")

    BUCKET_NAME = "mojap-derived-tables"

    DOMAIN_NAME = "courts"
    database_name = "xhibit"
    prefix = f"prod/models/domain_name={DOMAIN_NAME}/database_name={database_name}/"

    database_objects = list_s3_objects(BUCKET_NAME, prefix)

    pattern = r"(database_name=[^/]+/table_name=[^/]+)"
    table_paths = set()

    # Print the objects
    for obj in database_objects:
        match = re.search(pattern, obj)
        if match:
            table_path = match.group(0)
            table_paths.add(table_path)

    print(f"number of tables: {len(table_paths)}")

    # Pass db and table name into glue_client.get_table
    for table_path in table_paths:

        db_name = table_path.split("/")[0].replace("database_name=", "")
        table_name = table_path.split("/")[1].replace("table_name=", "")

        print(f"table name: {table_name}")

        # WAP = Write Aappend Publish. _wap tables are named with '_wap' suffix removed
        suffixes = ["_wap", "_scd2"]
        # remove suffix from table name if present
        for suffix in suffixes:
            if suffix in table_name:
                table_name = table_name[: -len(suffix)]
                print(f"suffix: {suffix} removed")
                break

        print("db:", db_name, "\ntbl:", table_name)
        try:
            response = glue_client.get_table(DatabaseName=db_name, Name=table_name)

            location = response["Table"]["StorageDescriptor"]["Location"]
            print("s3_location:", location)

            database_dir = f"json_output/{db_name}"
            pickle_filename = f"json_output/{db_name}/{table_name}.pkl"
            Path(database_dir).mkdir(parents=True, exist_ok=True)

            with open(pickle_filename, "wb") as file:
                pickle.dump(response, file)

            print(f"Pickle file {pickle_filename} saved")

        except Exception as e:
            print(e)

    # Output JSON to local Pickle file
