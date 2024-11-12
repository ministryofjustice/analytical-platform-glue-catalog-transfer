import pickle
import re

import boto3

print("setting up clients...")

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def list_s3_objects(bucket_name: str, prefix: str = ""):
    """List objects in an S3 bucket with an optional prefix."""
    objects = []
    paginator = s3_client.get_paginator('list_objects_v2')

    # Paginate through all objects
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                objects.append(obj['Key'])  # Append each object's key (file path)

    return objects


if __name__ == "__main__":
    # Example usage
    print("starting...")
    
    BUCKET_NAME = 'mojap-derived-tables'

    DOMAIN_NAME = 'courts'
    database_name = "xhibit_derived_beta"
    prefix = f'prod/models/domain_name={DOMAIN_NAME}/database_name={database_name}/'


    database_objects = list_s3_objects(BUCKET_NAME, prefix)

    pattern = r"(database_name=[^/]+/table_name=[^/]+)"
    table_paths = set()

    # Print the objects
    for obj in database_objects:
        match = re.search(pattern, obj)
        if match:
            table_path = match.group(0)
            table_paths.add(table_path)

    # Pass db and table name into glue_client.get_table
    for table_path in table_paths:
        db_name = table_path.split("/")[0].replace("database_name=", "")
        table_name = table_path.split("/")[1].replace("table_name=", "")

        # WAP = Write Aappend Publish. _wap tables are named with '_wap' suffix removed
        if "_wap" in table_name:
            table_name = table_name[:-4]

        print("db:", db_name, "\ntbl:", table_name)
        try:
            response = glue_client.get_table(
                DatabaseName=db_name,
                Name=table_name
            )

            location = response['Table']['StorageDescriptor']['Location']
            print("s3_location:", location)

            pickle_filename = f"{db_name}_{table_name}.pkl"
            with open(pickle_filename, "wb") as file:
                pickle.dump(response, file)

            print(f"Pickle file {pickle_filename} saved")
        
        except Exception as e:
            print(e)

    # Output JSON to local Pickle file
