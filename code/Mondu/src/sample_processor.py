from typing import List, Dict, Any, Optional
from tqdm import tqdm
from datetime import datetime

from src.models import Document, Measurement
from src.document_factory import DocumentFactory

def prepare_samples(data: List[Dict[str, Any]], document_factory: DocumentFactory, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Prepare samples for MongoDB insertion based on input data.

    :param data: List of dictionaries containing the input data
    :param document_factory: DocumentFactory object
    :param config: Configuration dictionary
    :return: List of prepared samples for MongoDB insertion
    """
    prepared_samples = []
    
    for record in tqdm(data):
        if config['DEVICE'] == "mimic_prescriptions":
            if record['startdate'] == '' and record['enddate'] == '':
                continue
        
        try:
            samples = document_factory.create_samples(device_id=config['devices'][config['DEVICE']], record_data=record)

            if not isinstance(samples, list):
                samples = [samples]

            for sample in samples:
                context = {}
                if hasattr(sample, 'context'):
                    context = sample.context

                if config['devices'][config['DEVICE'].lower()] in ["8", "9"]:  # mimic_sepsis or mimic_admission
                    prepared_samples.append({
                        "sample_dict": {
                            "user_id": config['users'][config['USERNAME'].lower()],
                            "device_id": config['devices'][config['DEVICE'].lower()],
                            "type": "mimic_sepsis" if config['devices'][config['DEVICE'].lower()] == "8" else "mimic_admission",
                            **context
                        },
                        "collection_dict": {
                            "$push": {'information' if config['devices'][config['DEVICE'].lower()] == "8" else 'admission': sample.measurements[0].value},
                            "$inc": {"n_samples": int(1)}
                        }
                    })
                else:
                    for measurement in sample.measurements:
                        if sample.type == "HEART_RATE" and measurement.value == 255:
                            continue

                        prepared_samples.append({
                            "sample_dict": {
                                "user_id": config['users'][config['USERNAME'].lower()],
                                "period": sample.period,
                                "device_id": config['devices'][config['DEVICE'].lower()],
                                "n_samples": {"$lt": config['MAX_SAMPLES']},
                                "type": sample.type,
                                "day": sample.day,
                                **context
                            },
                            "collection_dict": {
                                "$push": {'measurements': {
                                    'timestamp': measurement.timestamp,
                                    'value': measurement.value
                                }},
                                "$min": {"first": measurement.timestamp},
                                "$max": {"last": measurement.timestamp},
                                "$inc": {"n_samples": int(1)}
                            }
                        })
        except Exception as e:
            print(f"Error processing record: {e}")

    return prepared_samples

def upload_samples(data: List[Dict[str, Any]], document_factory: DocumentFactory, config: Dict[str, Any], collection):
    """
    Upload prepared samples to MongoDB.

    :param data: List of dictionaries containing the input data
    :param document_factory: DocumentFactory object
    :param config: Configuration dictionary
    :param collection: MongoDB collection object
    """
    samples = prepare_samples(data, document_factory, config)
    for sample in tqdm(samples):
        try:
            collection.update_one(
                sample['sample_dict'],
                sample['collection_dict'],
                upsert=True
            )
        except Exception as e:
            print(f"Error uploading sample: {e}")

def print_samples(data: List[Dict[str, Any]], document_factory: DocumentFactory, config: Dict[str, Any], quantity: Optional[int] = None):
    """
    Print prepared samples.

    :param data: List of dictionaries containing the input data
    :param document_factory: DocumentFactory object
    :param config: Configuration dictionary
    :param quantity: Optional number of samples to print
    """
    samples = prepare_samples(data, document_factory, config)
    for sample in tqdm(samples[:quantity] if quantity else samples):
        print(sample['sample_dict'])
        print(sample['collection_dict'])