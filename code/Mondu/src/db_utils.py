import pymongo as pm
from typing import Dict, Any, Tuple

def setup_database(config: Dict[str, Any]) -> Tuple[pm.database.Database, pm.collection.Collection]:
    """
    Set up the database connection based on the configuration.

    :param config: Configuration dictionary
    :return: Tuple of (Database object, Collection object)
    """
    client = pm.MongoClient()
    db = client[config['DATABASE']]
    collection = db[config['COLLECTION_NAME']]
    return db, collection

def upload_mappings(mappings: Dict[str, Any], config: Dict[str, Any]):
    """
    Upload mappings to MongoDB.

    :param mappings: Dictionary of mappings to upload
    :param config: Configuration dictionary
    """
    try:
        # Connect to the database
        client = pm.MongoClient()
        db = client[config['DATABASE']]
        mappings_collection = db['mappings']

        # The document name is the device name
        device_name = config['DEVICE']

        # Prepare the update operation
        update_operation = {
            "$set": {}
        }

        # For each mapping, add it to the update operation
        for field, iri in mappings.items():
            update_operation["$set"][field] = iri

        # Perform an upsert operation
        result = mappings_collection.update_one(
            {"_id": device_name},  # Use the device name as the document ID
            update_operation,
            upsert=True  # This will insert a new document if it doesn't exist
        )

        if result.matched_count > 0:
            print(f"Updated existing mappings for device: {device_name}")
        elif result.upserted_id:
            print(f"Created new mappings for device: {device_name}")
        else:
            print(f"No changes made to mappings for device: {device_name}")

    except Exception as e:
        print(f"Error uploading mappings: {str(e)}")
    finally:
        client.close()

def get_existing_prescriptions(collection: pm.collection.Collection, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Retrieve existing prescriptions from the database.

    :param collection: MongoDB collection object
    :param config: Configuration dictionary
    :return: List of existing prescriptions
    """
    return list(collection.find(
        {
            "device_id": config['devices'][config['DEVICE'].lower()],
            "measurements": {"$exists": {'drug': True}},
            "context": {"user_id": config['users'][config['USERNAME'].lower()]}
        },
        {"measurements": 1}
    ))

def create_index(collection: pm.collection.Collection, fields: List[str]):
    """
    Create an index on the specified fields in the collection.

    :param collection: MongoDB collection object
    :param fields: List of field names to index
    """
    collection.create_index(fields)