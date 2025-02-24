import boto3
import re

# Initialize AWS Glue and Boto3 clients
glue = boto3.client('glue')

# Specify the database where Iceberg tables exist
DATABASE_NAME = "common_data"

def get_tables():
    """Fetches all tables in the specified AWS Glue database."""
    response = glue.get_tables(DatabaseName=DATABASE_NAME)
    return response['TableList']

def rename_table(old_name, new_name):
    """Renames a Glue table by updating its metadata."""
    table = glue.get_table(DatabaseName=DATABASE_NAME, Name=old_name)
    
    # Modify table name while keeping schema, partitions, and storage
    table_input = {
        'Name': new_name,  # New Table Name
        'StorageDescriptor': table['Table']['StorageDescriptor'],
        'TableType': table['Table']['TableType'],
        'Parameters': table['Table']['Parameters'],
        'PartitionKeys': table['Table'].get('PartitionKeys', [])
    }

    # Update table in Glue Data Catalog
    glue.update_table(DatabaseName=DATABASE_NAME, TableInput=table_input)
    print(f"Renamed table: {old_name} → {new_name}")

def process_tables():
    """Finds tables with UUIDs and renames them."""
    tables = get_tables()

    for table in tables:
        old_name = table['Name']

        # Regex pattern to detect UUID and remove it
        new_name = re.sub(r"-[a-f0-9]+$", "", old_name)

        if old_name != new_name:
            rename_table(old_name, new_name)

# Execute the renaming process
process_tables()
