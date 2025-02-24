import boto3
import re
import logging

# Initialize AWS Glue and Boto3 clients
glue = boto3.client('glue')

# Configure Logging
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s [%(levelname)s]: %(message)s"
)
logger = logging.getLogger()

# Specify the AWS Glue Database Name
DATABASE_NAME = "common_data"

def get_tables():
    """Fetch all tables in the AWS Glue database."""
    response = glue.get_tables(DatabaseName=DATABASE_NAME)
    tables = response['TableList']
    
    logger.info(f"Retrieved {len(tables)} tables from Glue Data Catalog.")
    return tables

def rename_table(old_name, new_name):
    """Renames a Glue table by updating its metadata."""
    try:
        # Fetch table details
        table = glue.get_table(DatabaseName=DATABASE_NAME, Name=old_name)
        
        # Create new table input structure
        table_input = {
            'Name': new_name,  # New table name
            'StorageDescriptor': table['Table']['StorageDescriptor'],
            'TableType': table['Table']['TableType'],
            'Parameters': table['Table']['Parameters'],
            'PartitionKeys': table['Table'].get('PartitionKeys', [])
        }

        # Update table in Glue Data Catalog
        glue.update_table(DatabaseName=DATABASE_NAME, TableInput=table_input)
        
        logger.info(f"✅ SUCCESS: Renamed table '{old_name}' → '{new_name}'")
    
    except Exception as e:
        logger.error(f"❌ ERROR: Failed to rename '{old_name}' → '{new_name}'. Reason: {e}")

def process_tables():
    """Find tables with UUIDs and rename them."""
    tables = get_tables()

    for table in tables:
        old_name = table['Name']

        # Regex to detect and remove UUID
        new_name = re.sub(r"-[a-f0-9]+$", "", old_name)

        if old_name != new_name:
            logger.info(f"📌 Processing table: {old_name} → {new_name}")
            rename_table(old_name, new_name)

# Execute the renaming process
if __name__ == "__main__":
    logger.info("🚀 Starting Glue Table Renaming Job...")
    process_tables()
    logger.info("✅ Glue Table Renaming Job Completed Successfully!")
