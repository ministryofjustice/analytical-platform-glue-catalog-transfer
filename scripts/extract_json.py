import boto3
import pickle
import re

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

def list_s3_objects(bucket_name, prefix=None):
    """List objects in an S3 bucket with an optional prefix."""
    objects = []
    paginator = s3_client.get_paginator('list_objects_v2')

    # Paginate through all objects
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                objects.append(obj['Key'])  # Append each object's key (file path)

    return objects

# Example usage
bucket_name = 'mojap-derived-tables'
prefix = 'prod/models/domain_name=probation/database_name=delius/table_name=borough_wap/'  #prod/models/domain_name=bold/'
# 'prod/models/domain_name=courts/database_name=xhibit_derived/

objects = list_s3_objects(bucket_name, prefix)


pattern = r"(database_name=[^/]+/table_name=[^/]+)"
results = set()

# Print the objects
for obj in objects:
    match = re.search(pattern, obj)
    if match:
        result = match.group(0)
        results.add(result)

# Pass db and table name into glue_client.get_table
for i in list(results)[:1]:
    db_name = i.split("/")[0].replace("database_name=", "")
    table_name = i.split("/")[1].replace("table_name=", "")

    if "_wap" in table_name:
        table_name = table_name[:-4]
    # print("db:", db_name, "\ntbl:", table_name, "\n")

    response = glue_client.get_table(
        DatabaseName=db_name,
        Name=table_name
    )

    location = response['Table']['StorageDescriptor']['Location']
    print("db:", db_name, "\ntbl:", table_name, "\ns3_location:", location, "\n")

    with open("temp_ouput/delius_borough_table_json.pkl", "wb") as file:
        pickle.dump(response, file)

# Output JSON to local file
