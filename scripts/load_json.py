import boto3
import pickle

glue_client = boto3.client("glue", "eu-west-1")

with open("delius_borough_table_json_121124.pkl", "rb") as f:
    json_data = pickle.load(f)

table = json_data['Table']

source_bucket = "mojap-derived-tables"
dest_bucket = "mojap-compute-development-derived-tables-replication"

# table['StorageDescriptor']['Location'] = (
#     table['StorageDescriptor']['Location']
#     .replace(source_bucket, dest_bucket)
# )

# if 'metadata_location' in table['Parameters']:
#     table['Parameters']['metadata_location'] = (
#         table['Parameters']['metadata_location']
#         .replace(source_bucket, dest_bucket)
#     )

print(table)

try:
    glue_client.create_table(
                DatabaseName=table['DatabaseName'],
                TableInput={
                    'Name': table['Name'],
                    'Description': table.get('Description', ''),
                    'StorageDescriptor': table['StorageDescriptor'],
                    'PartitionKeys': table.get('PartitionKeys', []),
                    'TableType': table.get('TableType', 'EXTERNAL_TABLE'),
                    'Parameters': table.get('Parameters', {}),
                    # 'LastAccessTime': table.get('LastAccessTime'),
                    'Retention': table.get('Retention', 0)
                }
            )
    print("table created")
except Exception as e:
    print("table creation failed:", e)

# Add MSCK REPAIR TABLE statement after table creation
