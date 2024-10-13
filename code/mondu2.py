import logging
from datetime import timedelta, datetime
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

import mongoengine as me
import owlready2 as owl
from tqdm import tqdm
import pymongo as pm
import requests
import click
import csv


COPH_IRI = "http://www.semanticweb.org/danielbloor/ontologies/COPH.owl"
DATABASE = "COPH"
START_PATH = "/home/danielbloor/phd_work/data/mimic-iii/exported"
ECG_PATH = "/Downloads/phd/monitoring/data_DBL_1578325960/signal.csv"
AIR_QUALITY_PATH = ("/Documents/phd_work/analysis/Monitoring data/"
                    "Air_Quality/Flow/user_measures_732611_1.csv")
PPG_PATH = ("/Documents/phd_work/analysis/Monitoring data/"
            "PPG/gadgetbridge/MI_BAND_ACTIVITY_SAMPLE_270419.csv")
MIMIC_PATH = "/mimic-iii/exported/mimic_sepsis.csv"
FILE_NAME = "/diagnoses.csv"
MAX_SAMPLES = ""
SAMPLE_PERIOD = ""
DEBUG_MODE = True
COLLECTION = ""
USERNAME = ""
DEVICE = ""


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


DeviceID = str
UserID = str


devices = {
    "move_ecg": "0",
    "flow": "1",
    "amazfit_bip": "2",
    "mimic_chartevents": "3",
    "mimic_mortality": "4",
    "mimic_diagnoses": "5",
    "mimic_prescriptions": "6",
    "mimic_procedures": "7",
    "mimic_sepsis": "8",
    "mimic_admission": "9"
}

users = {
    "daniel bloor": "0",
    "thirty six": "36",
    "anonymous": "anon"
}

blood_pressure_state_severity = {
    "Relaxed": 0,
    "Elevated": 1,
    "Stage 1 Hypertension": 2,
    "Stage 2 Hypertension": 3,
    "Hypertensive Crisis": 4
}

alert_node_strings = {
    "1": "AlertGradeOne",
    "2": "AlertGradeTwo",
    "3": "AlertGradeThree"
}

@dataclass
class Measurement:
    timestamp: datetime
    value: float
    risk_score: Optional[int] = None

@dataclass
class Document:
    user_id: UserID
    type: str
    device_id: DeviceID
    period: str
    day: datetime
    valueuom: str
    measurements: List[Measurement]
    summaries: Dict[str, Any] = None

def day_generator(start_date: datetime, end_date: datetime):
    for date in range(int((end_date - start_date).days)):
        yield start_date + timedelta(date)

def get_days(start_date: datetime, end_date: datetime) -> List[str]:
    return [date.strftime("%Y-%m-%d") for date in day_generator(start_date, end_date)]

class Metadata(me.Document):
    document_version = me.StringField(required=True)
    ontology_name = me.StringField(required=True)
    ontology_version = me.StringField(required=True)
    mappings = me.DictField(required=True)

class DocumentFactory:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def create_samples(self, device_id: DeviceID, record_data: Dict[str, Any]) -> Document:
        samples_creator = self.get_document_creator(device_id)
        return samples_creator(record_data)

    def get_document_creator(self, device_id: DeviceID):
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

    def _create_flow(self, record_data: Dict[str, Any]) -> Document:
        timestamp = datetime.strptime(record_data['date'], '%Y-%m-%d %H:%M:%S')

        measurements = [
            Measurement(timestamp=timestamp, value=record_data['NO2']),
            Measurement(timestamp=timestamp, value=record_data['VOC']),
            Measurement(timestamp=timestamp, value=record_data['PM 10']),
            Measurement(timestamp=timestamp, value=record_data['PM25']),
            Measurement(timestamp=timestamp, value=record_data['AQI NO2']),
            Measurement(timestamp=timestamp, value=record_data['AQI VOC']),
            Measurement(timestamp=timestamp, value=record_data['AQI PM 10']),
            Measurement(timestamp=timestamp, value=record_data['AQI PM 25'])
        ]

        return Document(
            user_id=record_data['user_id'],
            type="flow",
            device_id=record_data['device_id'],
            period="1/min",
            day=timestamp.date(),
            valueuom="",
            measurements=measurements
        )

    def _create_move_ecg(self, record_data: Dict[str, Any]) -> Document:
        timestamp = datetime.fromisoformat(record_data['date'])

        measurements = [Measurement(timestamp=timestamp, value=record_data['signal'])]

        return Document(
            user_id=record_data['user_id'],
            type="move_ecg",
            device_id=record_data['device_id'],
            period="30 seconds",
            day=timestamp.date(),
            valueuom="",
            measurements=measurements
        )

    def _create_mimic_chartevents(self, record_data: Dict[str, Any]) -> Document:
        timestamp = datetime.fromisoformat(record_data['charttime'])

        measurements = [Measurement(timestamp=timestamp, value=record_data['value'])]

        return Document(
            user_id=record_data["subject_id"],
            type=record_data['label'],
            device_id=record_data['device_id'],
            period="Manual/day",
            day=timestamp.date(),
            valueuom=record_data['valueuom'],
            measurements=measurements
        )

    def _create_mimic_mortality(self, record_data: Dict[str, Any]) -> Document:
        measurements = [Measurement(timestamp=datetime.now(), value=record_data['expire_flag'])]

        return Document(
            user_id=str(record_data['subject_id']),
            type='mortality',
            device_id=record_data['device_id'],
            period="Clinic stay",
            day=datetime.now().date(),
            valueuom="",
            measurements=measurements
        )

    def _create_mimic_diagnoses(self, record_data: Dict[str, Any]) -> Document:
        measurements = [Measurement(
            timestamp=datetime.now(),
            value=f"{record_data['icd9_code']}:{record_data['title']}"
        )]

        return Document(
            user_id=str(record_data["user_id"]),
            type='diagnoses',
            device_id=record_data['device_id'],
            period="Clinic stay",
            day=datetime.now().date(),
            valueuom="",
            measurements=measurements
        )

    def _create_mimic_prescriptions(self, record_data: Dict[str, Any]) -> List[Document]:
        documents = []
        
        try:
            startdate = datetime.fromisoformat(record_data['startdate'])
        except ValueError:
            startdate = datetime.fromisoformat(record_data['enddate'])
        
        try:
            enddate = datetime.fromisoformat(record_data['enddate'])
        except ValueError:
            enddate = startdate + timedelta(days=1)

        days = get_days(startdate, enddate)

        for day in days:
            measurements = [Measurement(
                timestamp=datetime.strptime(day, "%Y-%m-%d"),
                value=f"{record_data['drug']}:{record_data['dose_val_rx']}:{record_data['dose_unit_rx']}"
            )]

            documents.append(Document(
                user_id=record_data["subject_id"],
                type='prescriptions',
                device_id=record_data['device_id'],
                period="Manual/day",
                day=datetime.strptime(day, "%Y-%m-%d").date(),
                valueuom="",
                measurements=measurements
            ))

        return documents

    def _create_mimic_procedures(self, record_data: Dict[str, Any]) -> Document:
        measurements = [Measurement(
            timestamp=datetime.now(),
            value=f"{record_data['icd9_code']}:{record_data['description']}"
        )]

        return Document(
            user_id=record_data["subject_id"],
            type='procedures',
            device_id=record_data['device_id'],
            period="Admission or after procedure",
            day=datetime.now().date(),
            valueuom="",
            measurements=measurements
        )

    def _create_mimic_sepsis(self, record_data: Dict[str, Any]) -> Document:
        measurements = [Measurement(timestamp=datetime.now(), value=json.dumps(record_data))]

        return Document(
            user_id=record_data['subject_id'],
            type='mimic_sepsis',
            device_id=record_data['device_id'],
            period="Various",
            day=datetime.now().date(),
            valueuom="",
            measurements=measurements
        )

    def _create_mimic_admission(self, record_data: Dict[str, Any]) -> Document:
        measurements = [Measurement(
            timestamp=datetime.fromisoformat(record_data['admittime']),
            value=json.dumps({
                'dischtime': record_data['dischtime'],
                'deathtime': record_data['deathtime'],
                'admission_type': record_data['admission_type'],
                'admission_location': record_data['admission_location'],
                'insurance': record_data['insurance'],
                'ethnicity': record_data['ethnicity'],
                'diagnosis': record_data['diagnosis'],
                'hospital_expire_flag': record_data['hospital_expire_flag']
            })
        )]

        return Document(
            user_id=str(record_data['subject_id']),
            type='admission',
            device_id=record_data['device_id'],
            period="Admission",
            day=datetime.fromisoformat(record_data['admittime']).date(),
            valueuom="",
            measurements=measurements
        )

    def create_mappings(self, device_id: DeviceID, record_data: Dict[str, Any], 
                        database: pm.database.Database, ontology: owl.Ontology) -> Dict[str, Any]:
        mapping_fields = self._ask_mongodb_mappings(record_data)
        mappings = {}
        
        try:
            for mapping in mapping_fields:
                result = database['metadata'].find_one(
                    {"$and": [{f"mappings.{mapping}": {"$exists": True}},
                              {f"mappings.{mapping}": {"$size": 2}}]},
                    {f"mappings.{mapping}": 1}
                )
                if result:
                    mappings[mapping] = result['mappings'][mapping]
        except Exception as e:
            logger.error(f"Error creating mappings: {str(e)}")

        print(f"\nMappings found:")
        for term, iris in mappings.items():
            print(f"{term}: \n  Relationship: {iris[0]}\n  Term: {iris[1]}")
        
        onto_fields = [field for field in record_data[0].keys() if field not in mappings.keys()]
        if onto_fields[0] == '':
            onto_fields = onto_fields[1:]

        print("\nThe following fields were not provided mapping:")
        for field in onto_fields:
            print(field)
        
        onto_check_answer = input(
            ("\nPlease choose your preferred solution:\n"
             "   1) Search COPH ontology for suitable terms\n"
             "   2) Search OLS (Ontology Lookup Service) for suitable terms\n"
             "   3) Provide your own term options\n"
             "Option: "))

        if onto_check_answer == "1":
            mappings.update(self._search_coph_ontology(onto_fields, ontology))
        elif onto_check_answer == "2":
            mappings.update(self._search_ols(onto_fields, ontology))
        else:
            mappings.update(self._manual_mapping(onto_fields))

        return mappings

    def _ask_mongodb_mappings(self, record_data: Dict[str, Any]) -> List[str]:
        fields_list = list(record_data[0].keys())[1:]
        print("\nFields found in data: ")
        for num, field in enumerate(fields_list):
            print(f"{num}: {field}")
        
        fields_answer = input('\nUsing the above list and numbers, please choose (separated by comma) '
                              'the key(s) from the data you wish to be used to identify appropriate '
                              'semantic mappings from the database: ')
        answer_numbers = [int(number) for number in fields_answer.replace(' ', '').split(',')]
        return [fields_list[num] for num in answer_numbers]

    def _search_coph_ontology(self, onto_fields: List[str], ontology: owl.Ontology) -> Dict[str, Any]:
        new_mappings = {}
        for field in onto_fields:
            query = input(f"Type an alternative term to search for {field}, or leave blank to use field name: ") or field
            onto_result = ontology.search(label=f"*{query}*") or ontology.search(comment=f"*{query}*")
            
            if onto_result:
                print(f"Resulting option(s) for '{query}' is/are:\n")
                for num, result in enumerate(onto_result):
                    print(f"{num}) {result.label}: {result.iri}")
                chosen_match = input("Please type the number of the chosen response (empty implies none): ")
                if chosen_match:
                    mapping_choice = onto_result[int(chosen_match)]
                    new_mappings[mapping_choice.label[0]] = mapping_choice.iri
            else:
                print("No suitable option was found")
                new_mappings.update(self._manual_mapping(field))
        
        return new_mappings

    def _search_ols(self, onto_fields: List[str], ontology: owl.Ontology) -> Dict[str, Any]:
        new_mappings = {}
        for field in onto_fields:
            new_mapping = self.ols_search(field=field, ontology=ontology)
            new_mappings[new_mapping[field]] = new_mapping[field]
        return new_mappings

    def _manual_mapping(self, fields: Union[List[str], str]) -> Dict[str, str]:
        if isinstance(fields, str):
            fields = [fields]
        
        new_mappings = {}
        for field in fields:
            while True:
                manual_label = input(f"Please type the label to use for {field}: ")
                manual_iri = input(f"Please type the iri to match with {manual_label} for {field}: ")
                prompt_verification = input(f"{manual_label}: {manual_iri}; are these correct? (Y/n)")
                if prompt_verification.lower() not in ('n', 'no'):
                    new_mappings[manual_label] = manual_iri
                    break
        return new_mappings

    def ols_search(self, field: str, ontology: owl.Ontology) -> Dict[str, str]:
        print("\nField: "+field)
        query = input("Type an alternative term to search for, or leave blank to use field name: ") or field
        
        url_queries = []
        if input("\nWould you like to filter to exact matches? [Y/n]").lower() not in ('n', 'no'):
            url_queries.append("exact=true")
        
        query_field_query = input("\nWhat would you like to query on?: \n  1) Label\n  2) Synonym\n  3) Both\nOption: ")
        if query_field_query == "1":
            url_queries.append("queryFields=label")
        elif query_field_query == "2":
            url_queries.append("queryFields=synonym")
        else:
            url_queries.append("queryFields=label,synonym")
        
        api_url = f"https://www.ebi.ac.uk/ols/api/search?q={query}&{'&'.join(url_queries)}&fieldList=iri,label,ontology_name,description"
        
        try:
            response = requests.get(api_url)
            response.raise_for_status()
            options = response.json()

            print("\nHere are the first 10 results:")
            for num, result in enumerate(options['response']['docs'][:10]):
                print(f"{num}) IRI: {result['iri']}\n   label: {result['label']}\n   ontology: {result['ontology_name']}\n   Description: {result['description']}\n")
            
            selected_mapping = input("Please choose the number of the result you wish to use (leave blank for none): ")
            if selected_mapping and int(selected_mapping) < 10:
                selected_result = options['response']['docs'][int(selected_mapping)]
                if ontology.search(label=f"*{selected_result['label']}*") is None:
                    with ontology:
                        owl.types.new_class(selected_result['label'], (ontology['Thing'],))
                return {field: selected_result['iri']}
            else:
                print("None chosen, you will now be asked to provide your own mapping")
                return self._manual_mapping(query)
        except requests.RequestException as e:
            logger.error(f"OLS search failed: {str(e)}")
            print("No response from ontology search service")
            return self._manual_mapping(query)

    def print_mappings(self, mappings: Dict[str, Any]):
        print("Mappings:")
        for key, value in mappings.items():
            print(f"{key}: {value}")


def parse_file(file_path: str) -> List[Dict[str, Any]]:
    file_data = []
    file_format = file_path.lower().rpartition('.')[-1]
    
    if file_format == "csv":
        with open(file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                file_data.append(dict(row))
    elif file_format == "json":
        with open(file_path, 'r') as file:
            file_data = json.load(file)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

    return file_data

def prepare_samples(data: List[Dict[str, Any]], document_factory: DocumentFactory, config: Dict[str, Any]) -> List[Dict[str, Any]]:
    prepared_samples = []
    
    if config['DEVICE'] == "mimic_prescriptions":
        mongo_prescriptions = config['COLLECTION'].find(
            {
                "device_id": config['devices'][config['DEVICE'].lower()],
                "measurements": {"$exists": {'drug': True}},
                "context": {"user_id": config['users'][config['USERNAME'].lower()]}
            },
            {"measurements": 1}
        )

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
            logger.error(f"Error processing record: {e}")

    return prepared_samples

def upload_samples(data: List[Dict[str, Any]], document_factory: DocumentFactory, config: Dict[str, Any]):
    samples = prepare_samples(data, document_factory, config)
    for sample in tqdm(samples):
        try:
            config['COLLECTION'].update_one(
                sample['sample_dict'],
                sample['collection_dict'],
                upsert=True
            )
        except Exception as e:
            logger.error(f"Error uploading sample: {e}")

def print_samples(data: List[Dict[str, Any]], document_factory: DocumentFactory, config: Dict[str, Any], quantity: Optional[int] = None):
    samples = prepare_samples(data, document_factory, config)
    for sample in tqdm(samples[:quantity] if quantity else samples):
        print(sample['sample_dict'])
        print(sample['collection_dict'])

@click.command()
@click.argument("filepath", type=click.Path(exists=True))
@click.option("-u", "--username", prompt="Monitoring device user's name", help="Name of monitoring device user")
@click.option("-d", "--device", prompt="Device name", help="Name of monitoring device")
@click.option("-s", "--sample_period", prompt="Interval of sample period", help="The interval a sample period represents.")
@click.option("-m", "--max_samples", prompt="Maximum samples per document", help="Most samples to upload per MongoDB document.", default=1500)
@click.option("-db", "--database", help="Database to upload to.", default='COPH')
@click.option("-c", "--collection", help="Collection to upload to.", default='measurements')
def main(filepath: str, username: str, device: str, sample_period: str,
         max_samples: int, database: str, collection: str):
    
    # Load configuration
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    
    # Update config with command-line parameters
    config.update({
        'USERNAME': username,
        'DEVICE': device,
        'SAMPLE_PERIOD': sample_period,
        'MAX_SAMPLES': max_samples,
        'DATABASE': database,
        'COLLECTION_NAME': collection
    })

    # Setup ontology
    onto_path.append(config['ONTO_PATH'])
    world = owl.default_world
    with tempfile.NamedTemporaryFile(mode='w+b', delete=False) as temp_file:
        world.set_backend(filename=temp_file.name)
        onto = world.get_ontology(config['COPH_IRI']).load()

    # Setup database connection
    client = pm.MongoClient()
    db = client[config['DATABASE']]
    config['COLLECTION'] = db[config['COLLECTION_NAME']]
    
    # Parse file and create DocumentFactory
    data = parse_file(filepath)
    document_factory = DocumentFactory(config)

    try:
        if config['DEBUG_MODE']:
            print_samples(data=data[:100], document_factory=document_factory, config=config)
            mappings = document_factory.create_mappings(config['DEVICE'], record_data=data,
                                        database=db, ontology=onto)
            document_factory.print_mappings(mappings)
        else:
            upload_samples(data=data, document_factory=document_factory, config=config)
            mappings = document_factory.create_mappings(config['DEVICE'], record_data=data,
                                        database=db, ontology=onto)
            upload_mappings(mappings, config)
    finally:
        # Clean up
        client.close()
        os.unlink(temp_file.name)

def upload_mappings(mappings: Dict[str, Any], config: Dict[str, Any]):
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
            logger.info(f"Updated existing mappings for device: {device_name}")
        elif result.upserted_id:
            logger.info(f"Created new mappings for device: {device_name}")
        else:
            logger.warning(f"No changes made to mappings for device: {device_name}")

    except Exception as e:
        logger.error(f"Error uploading mappings: {str(e)}")
    finally:
        client.close()

if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    main()