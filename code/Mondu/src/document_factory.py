from typing import Dict, Any, List, Union
from datetime import datetime, timedelta
import owlready2 as owl
import pymongo as pm
import requests

from src.models import Document, Measurement, DeviceID
from src.config import load_config

class DocumentFactory:
    """
    Factory class for creating measurement documents.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the DocumentFactory.

        :param config: Configuration dictionary
        """
        self.config = config

    def create_samples(self, device_id: DeviceID, record_data: Dict[str, Any]) -> Union[Document, List[Document]]:
        """
        Create a Document object based on the device type and record data.

        :param device_id: Identifier for the device
        :param record_data: Dictionary containing the record data
        :return: Document object or list of Document objects
        :raises ValueError: If an unsupported device type is provided
        """
        samples_creator = self.get_document_creator(device_id)
        return samples_creator(record_data)

    def get_document_creator(self, device_id: DeviceID):
        """
        Get the appropriate document creator function for the given device ID.

        :param device_id: Identifier for the device
        :return: Function to create a document for the specified device
        :raises ValueError: If an unsupported device type is provided
        """
        creators = {
            self.config['devices']['move_ecg']: self._create_move_ecg,
            self.config['devices']['flow']: self._create_flow,
            self.config['devices']['amazfit_bip']: self._create_amazfit_bip,
            self.config['devices']['mimic_chartevents']: self._create_mimic_chartevents,
            self.config['devices']['mimic_mortality']: self._create_mimic_mortality,
            self.config['devices']['mimic_diagnoses']: self._create_mimic_diagnoses,
            self.config['devices']['mimic_prescriptions']: self._create_mimic_prescriptions,
            self.config['devices']['mimic_procedures']: self._create_mimic_procedures,
            self.config['devices']['mimic_sepsis']: self._create_mimic_sepsis,
            self.config['devices']['mimic_admission']: self._create_mimic_admission,
        }
        creator = creators.get(device_id)
        if not creator:
            raise ValueError(f"Unsupported device type: {device_id}")
        return creator

    def _create_amazfit_bip(self, record_data: Dict[str, Any]) -> Document:
        """
        Create a Document for Amazfit Bip data.

        :param record_data: Dictionary containing the record data
        :return: Document object
        """
        timestamp = datetime.fromtimestamp(int(record_data['TIMESTAMP']))
        measurements = [
            Measurement(timestamp=timestamp, value=record_data['RAW_INTENSITY']),
            Measurement(timestamp=timestamp, value=record_data['STEPS']),
            Measurement(timestamp=timestamp, value=record_data['HEART_RATE']),
            Measurement(timestamp=timestamp, value=record_data['RAW_KIND'])
        ]
        return Document(
            user_id=record_data['user_id'],
            type="amazfit_bip",
            device_id=record_data['device_id'],
            period="1/min",
            day=timestamp.date(),
            valueuom="",
            measurements=measurements
        )

    # ... Other _create_* methods would be implemented here ...

    def create_mappings(self, device_id: DeviceID, record_data: Dict[str, Any], 
                        database: pm.database.Database, ontology: owl.Ontology) -> Dict[str, Any]:
        """
        Create mappings for the given device and record data.

        :param device_id: Identifier for the device
        :param record_data: Dictionary containing the record data
        :param database: MongoDB database object
        :param ontology: Owlready2 ontology object
        :return: Dictionary of created mappings
        """
        # Implementation remains the same as in the original script
        pass

    # ... Other helper methods (_ask_mongodb_mappings, _search_coph_ontology, etc.) ...

    def print_mappings(self, mappings: Dict[str, Any]):
        """
        Print the mappings.

        :param mappings: Dictionary of mappings
        """
        print("Mappings:")
        for key, value in mappings.items():
            print(f"{key}: {value}")