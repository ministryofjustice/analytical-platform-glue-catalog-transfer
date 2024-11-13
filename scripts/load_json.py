import os
import pickle
from pathlib import Path
from typing import Generator

import boto3
from dotenv import find_dotenv, load_dotenv


def repair_table(
    database_name: str,
    table_name: str,
    query_results_bucket: str = "aws-athena-query-results-eu-west-1-apc-dev",
):
    query = f"MSCK REPAIR TABLE {database_name}.{table_name}"
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database_name},
        ResultConfiguration={"OutputLocation": f"s3://{query_results_bucket}/"},
    )
    return response


load_dotenv(find_dotenv())

glue_client = boto3.client("glue", region_name="eu-west-1")


# Check that the script is running in the correct AWS account
def check_aws_account(expected_account_name: str):
    session = boto3.Session()
    sts_client = session.client("sts")
    account_id = sts_client.get_caller_identity()["Account"]
    expected_account_id = os.environ.get(expected_account_name)

    if account_id != expected_account_id:
        raise ValueError(f"Not running in expected account: {expected_account_name}")


check_aws_account("analytical-platform-compute-development")

directory = "json_output"
pathlist: Generator[Path, None, None] = Path(directory).glob("**/*.pkl")

for path in pathlist:
    path_in_str = str(path)
    print(path_in_str)

    with open(path_in_str, "rb") as file_name:
        json_data = pickle.load(file_name)

    table = json_data["Table"]
    table_name: str = table["Name"]
    database_name: str = table["DatabaseName"]

    if "metadata_location" in table["Parameters"]:
        is_hive = False
    else:
        is_hive = True

    # source_bucket = "mojap-derived-tables"

    print(table_name)
    print(f"table is hive: {is_hive}")

    # create the database if it doesn't exist
    try:
        glue_client.get_database(Name=database_name)
        print(f"database {database_name} already exists, not creating")
    except glue_client.exceptions.EntityNotFoundException:
        try:
            glue_client.create_database(DatabaseInput={"Name": database_name})
            print("database created")
        except Exception as e:
            print("database creation failed:", e)
            break

    # create the table if it doesn't exist
    try:
        glue_client.get_table(Name=table_name, DatabaseName=database_name)
        print(f"table {table_name} exists, not creating")
        continue
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "Description": table.get("Description", ""),
                "StorageDescriptor": table["StorageDescriptor"],
                "PartitionKeys": table.get("PartitionKeys", []),
                "TableType": table.get("TableType", "EXTERNAL_TABLE"),
                "Parameters": table.get("Parameters", {}),
                # 'LastAccessTime': table.get('LastAccessTime'),
                "Retention": table.get("Retention", 0),
            },
        )
        print("table created")
    except Exception as e:
        print("table creation failed:", e)
        break

    if is_hive:
        query_results_bucket = "aws-athena-query-results-eu-west-1-apc-dev"
        #     # Add MSCK REPAIR TABLE statement after table creation
        athena_client = boto3.client("athena")
        try:
            repair_table(database_name, table_name, query_results_bucket)
            print(f"table {table_name} repaired")
        except:
            print("partition creation failed")

    # Stretch goal: use batch_create_partition to add partitions rather than using MSCK REPAIR TABLE (see https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html)
