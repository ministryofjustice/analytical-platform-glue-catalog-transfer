import os
import pickle
from pathlib import Path

import boto3
from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

glue_client = boto3.client("glue", region_name="eu-west-1")

session = boto3.Session()
sts_client = session.client("sts")
account_id = sts_client.get_caller_identity()["Account"]
expected_account_name = "analytical-platform-compute-development"
expected_account_id = os.environ.get(expected_account_name)

if account_id != expected_account_id:
    raise ValueError(f"Not running in expected account: {expected_account_name}")

directory = "json_output"
pathlist = Path(directory).glob("**/*.pkl")

for path in pathlist:
    path_in_str = str(path)

    with open(path_in_str, "rb") as file_name:
        json_data = pickle.load(file_name)

    table = json_data["Table"]

    source_bucket = "mojap-derived-tables"

    print(table)

    try:
        glue_client.create_table(
            DatabaseName=table["DatabaseName"],
            TableInput={
                "Name": table["Name"],
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

# Add MSCK REPAIR TABLE statement after table creation
