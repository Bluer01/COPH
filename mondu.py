from datetime import timedelta, datetime
import mongoengine as me
import owlready2 as owl
from tqdm import tqdm
import pymongo as pm
import pprint as pp
import requests
import click
import types
import json
import csv


COPH_IRI = "http://www.semanticweb.org/danielbloor/ontologies/COPH.owl"
DATABASE = "COPH"
START_PATH = ""
FILE_NAME = ""
MAX_SAMPLES = ""
SAMPLE_PERIOD = ""
DEBUG_MODE = False
COLLECTION = ""
USERNAME = ""
DEVICE = ""

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

mongo_prescriptions = []

users = {
    "daniel bloor": "0",
    "thirty six": "36",
    "anonymous": "anon"
}

drug_dosage_num = 1
mongo_dosage_num = 0
previous_drug = ""
previous_days = []
previous_context = {
    'user': "Placeholder",
    'prescriptions': []
}


def day_generator(start_date_, end_date_):
    for date in range(int((end_date_ - start_date_).days)):
        yield start_date_ + timedelta(date)


def get_days(start_date_, end_date_):
    days = []
    for date in day_generator(start_date_, end_date_):
        days.append(date.strftime("%Y-%m-%d"))

    return days


class Metadata(me.Document):
    document_version = me.StringField(required=True)
    ontology_name = me.StringField(required=True)
    ontology_version = me.StringField(required=True)
    mappings = me.DictField(required=True)


class DocumentFactory:
    def create_samples(self, device_id, record_data):
        samples_creator = self.get_document(device_id=device_id, record_data = record_data)
        return samples_creator(record_data)

    def get_document(self, device_id, record_data):
        if device_id in ["amazfit_bip", devices['amazfit_bip']]:
            return self._create_amazfit_bip
        elif device_id in ["move_ecg", devices['move_ecg']]:
            return self._create_move_ecg
        elif device_id in ["flow", devices['flow']]:
            return self._create_flow
        elif device_id in ["mimic_chartevents", devices['mimic_chartevents']]:
            return self._create_mimic_chartevents
        elif device_id in ["mimic_mortality", devices['mimic_mortality']]:
            return self._create_mimic_mortality
        elif device_id in ["mimic_diagnoses", devices['mimic_diagnoses']]:
            return self._create_mimic_diagnoses
        elif device_id in ["mimic_prescriptions", devices['mimic_prescriptions']]:
            return self._create_mimic_prescriptions
        elif device_id in ["mimic_procedures", devices['mimic_procedures']]:
            return self._create_mimic_procedures
        elif device_id in ["mimic_sepsis", devices['mimic_sepsis']]:
            return self._create_mimic_sepsis
        elif device_id in ["mimic_admission", devices['mimic_admission']]:
            return self._create_mimic_admission


    def _create_amazfit_bip(self, record_data):
        global SAMPLE_PERIOD

        SAMPLE_PERIOD = "1/min"

        timestamp_formatted = datetime.fromtimestamp(
            int(record_data['TIMESTAMP']))

        sample_raw_intensity = {
            'timestamp': timestamp_formatted,
            'value': record_data['RAW_INTENSITY']
        }
        sample_steps = {
            'timestamp': timestamp_formatted,
            'value': record_data['STEPS']
        }
        sample_heart_rate = {
            'timestamp': timestamp_formatted,
            'value': record_data['HEART_RATE']
        }
        sample_raw_kind = {
            'timestamp': timestamp_formatted,
            'value': record_data['RAW_KIND']
        }

        measurements = {
            "RAW_INTENSITY": sample_raw_intensity,
            "STEPS": sample_steps,
            "HEART_RATE": sample_heart_rate,
            "RAW_KIND": sample_raw_kind
        }

        context = {}

        return {'samples': measurements, 'context': context}


    def _create_flow(self, record_data):
        global SAMPLE_PERIOD

        SAMPLE_PERIOD = "1/min"

        timestamp_formatted = datetime.strptime(
            record_data['date'], '%Y-%m-%d %H:%M:%S')

        sample_no2 = {
            'timestamp': timestamp_formatted,
            'value': record_data['NO2']
        }
        sample_voc = {
            'timestamp': timestamp_formatted,
            'value': record_data['VOC']
        }
        sample_pm10 = {
            'timestamp': timestamp_formatted,
            'value': record_data['PM 10']
        }
        sample_pm25 = {
            'timestamp': timestamp_formatted,
            'value': record_data['PM25']
        }
        sample_aqi_no2 = {
            'timestamp': timestamp_formatted,
            'value': record_data['AQI NO2']
        }
        sample_aqi_voc = {
            'timestamp': timestamp_formatted,
            'value': record_data['AQI VOC']
        }
        sample_aqi_pm10 = {
            'timestamp': timestamp_formatted,
            'value': record_data['AQI PM 10']
        }
        sample_aqi_pm25 = {
            'timestamp': timestamp_formatted,
            'value': record_data['AQI PM 25']
        }

        measurements = {
            "NO2": sample_no2,
            "VOC": sample_voc,
            "PM10": sample_pm10,
            "PM25": sample_pm25,
            "AQI NO2": sample_aqi_no2,
            "AQI VOC": sample_aqi_voc,
            "AQI PM10": sample_aqi_pm10,
            "AQI PM25": sample_aqi_pm25
        }

        context = {}

        return {'samples': measurements, 'context': context}


    def _create_move_ecg(self, record_data):
        global SAMPLE_PERIOD

        SAMPLE_PERIOD = "30 seconds"

        timestamp_formatted = datetime.fromisoformat(record_data['date'])

        sample_ECG = {
            'timestamp': timestamp_formatted,
            'value': record_data['signal']
        }

        measurements = {"ECG": sample_ECG}

        context = {
            "format": record_data['format'],
            "frequency": record_data['frequency'],
            "size": record_data['size'],
            "total_size": record_data['totalsize'],
            "wear_position": record_data['wearposition']
        }

        return {'samples': measurements, 'context': context}


    def _create_mimic_chartevents(self, record_data):
        global SAMPLE_PERIOD

        SAMPLE_PERIOD = "Manual/day"

        timestamp_formatted = datetime.fromisoformat(record_data['charttime'])

        sample = {
            'timestamp': timestamp_formatted,
            'value': record_data['value']
        }

        measurements = {record_data['label']: sample}

        context = {
            "user_id": record_data["subject_id"],
            "valueuom": record_data['valueuom']
        }

        return {'samples': measurements, 'context': context}


    def _create_mimic_mortality(self, record_data):
        global SAMPLE_PERIOD

        SAMPLE_PERIOD = "Clinic stay"

        sample = {
            'subject_id': str(record_data['subject_id']),
            'flag': record_data['expire_flag']
        }

        measurements = {'mortality': sample}

        context = {}

        return {'samples': measurements, 'context': context}


    def _create_mimic_diagnoses(self, record_data):
        global SAMPLE_PERIOD

        SAMPLE_PERIOD = "Clinic stay"

        sample = {'hadm_id': record_data['hadm_id'],
                    "seq_num": record_data['seq_num'],
                        "icd9_code": record_data['icd9_code'],
                        "description": record_data['title']}

        measurements = {'diagnoses': sample}

        context = {"user_id": str(record_data["user_id"])}

        return {'diagnoses': measurements, 'context': context}

    def _create_mimic_prescriptions(self, record_data):
        global SAMPLE_PERIOD
        SAMPLE_PERIOD = "Manual/day"
        created_samples = []
        highest_dose_num = 0
        mongo_dosage_num = 0
        drug_dosage_num = 0

        if record_data['subject_id'] != previous_context['user']:
            previous_context['user'] = record_data['subject_id']
            previous_context['prescriptions'] = []

        try:
            startdate_formatted = datetime.fromisoformat(
                record_data['startdate'])
        except:
            print(f"start date: {record_data['startdate']}")
            print(f"end date: {record_data['enddate']}")
            startdate_formatted = datetime.fromisoformat(
                record_data['enddate'])
        try:
            enddate_formatted = datetime.fromisoformat(
                record_data['enddate'])
        except:
            enddate_formatted = datetime.fromisoformat(
                record_data['startdate']) + timedelta(days=1)

        days = get_days(startdate_formatted, enddate_formatted)

        for day in days:
            if len(previous_context['prescriptions']) > 0:
                for prescription in previous_context['prescriptions']:
                    if day == prescription['day'] and record_data['drug'] in prescription['drug']:
                        if prescription['drug_dosage_num'] > highest_dose_num:
                            highest_dose_num = prescription['drug_dosage_num']

            result = [record for record in mongo_prescriptions if record['day'] == day and record['drug'] == record_data['drug']]
            if len(result) > 0:
                dosages = []
                for record in result:
                    for key, value in record.items():
                        if key == 'drug_dosage_num':
                            dosages.append(value)
                    mongo_dosage_num = max(dosages)

            highest_sample_dose_num = 0
            if len(created_samples) > 0:
                sample_dose_nums = [sample['samples']['prescriptions']['drug_dosage_num'] for sample in created_samples if sample['samples']['prescriptions']['day'] == day and sample['samples']['prescriptions']['drug'] == record_data['drug']]
                if len(sample_dose_nums) > 0:
                    highest_sample_dose_num = max(sample_dose_nums)

            drug_dosage_num = max(mongo_dosage_num, highest_dose_num, highest_sample_dose_num)+1

            sample = {
                'day': day,
                'drug': record_data['drug'],
                'dose_value': record_data['dose_val_rx'],
                'dose_unit' : record_data['dose_unit_rx'],
                'drug_dosage_num': drug_dosage_num
            }

            mongo_dosage_num = 0
            highest_dose_num = 0
            drug_dosage_num = 0

            measurements = {'prescriptions': sample}


            context = {"user_id": record_data["subject_id"]}
            created_samples.append({'samples': measurements, 'context': context})
            previous_context['prescriptions'].append(
                {'day': sample['day'],
                 'drug': sample['drug'],
                 'drug_dosage_num': sample['drug_dosage_num']})

        return created_samples

    def _create_mimic_procedures(self, record_data):
        global SAMPLE_PERIOD

        SAMPLE_PERIOD = "Admission or after procedure"

        sample = {"hadm_id": record_data['hadm_id'],
                  "seq_num": record_data['seq_num'],
                  "icd9_code": record_data['icd9_code'],
                "description": record_data['description']}

        measurements = {"procedures": sample}

        context = {"user_id": record_data["subject_id"]}

        return {'procedure': measurements, 'context': context}

    def _create_mimic_sepsis(self, record_data):
        global SAMPLE_PERIOD

        SAMPLE_PERIOD = "Various"

        information = {
            'icustay_id': record_data['icustay_id'], 'hadm_id': record_data['hadm_id'],
            'suspected_infection_time_poe': record_data['suspected_infection_time_poe'],
            'suspected_infection_time_poe_days': record_data['suspected_infection_time_poe_days'],
            'specimen_poe': record_data['specimen_poe'],
            'positiveculture_poe': record_data['positiveculture_poe'],
            'antibiotic_time_poe': record_data['antibiotic_time_poe'],
            'blood_culture_time': record_data['blood_culture_time'],
            'blood_culture_positive': record_data['blood_culture_positive'],
            'ethnicity': record_data['ethnicity'],
            'race_white': record_data['race_white'], 'race_black': record_data['race_black'],
            'race_hispanic': record_data['race_hispanic'], 'race_other': record_data['race_other'],
            'metastatic_cancer': record_data['metastatic_cancer'], 'diabetes': record_data['diabetes'],
            'bmi': record_data['bmi'], 'first_service': record_data['first_service'],
            'hospital_expire_flag': record_data['hospital_expire_flag'],
            'thirtyday_expire_flag': record_data['thirtyday_expire_flag'],
            'sepsis_angus': record_data['sepsis_angus'], 'sepsis_martin': record_data['sepsis_martin'],
            'sepsis_explicit': record_data['sepsis_explicit'],
            'septic_shock_explicit': record_data['septic_shock_explicit'],
            'severe_sepsis_explicit': record_data['severe_sepsis_explicit'],
            'sepsis_nqf': record_data['sepsis_nqf'], 'sepsis_cdc': record_data['sepsis_cdc'],
            'sepsis_cdc_simple': record_data['sepsis_cdc_simple'],
            'elixhauser_hospital': record_data['elixhauser_hospital'],
            'vent': record_data['vent'], 'sofa': record_data['sofa'],
            'lods': record_data['lods'], 'sirs': record_data['sirs'],
            'qsofa': record_data['qsofa'], 'qsofa_sysbp_score': record_data['qsofa_sysbp_score'],
            'qsofa_gcs_score': record_data['qsofa_gcs_score'], 'qsofa_resprate_score': record_data['qsofa_resprate_score'],
            'blood culture': record_data['blood culture'], 'suspicion_poe': record_data['suspicion_poe'],
            'abx_poe': record_data['abx_poe'], 'sepsis-3': record_data['sepsis-3'],
            'sofa>=2': record_data['sofa>=2'], 'excluded': record_data['excluded'],
            'intime': record_data['intime'], 'outtime': record_data['outtime'],
	        'dbsource': record_data['dbsource'], 'age': record_data['age'], 'gender': record_data['gender'],
            'is_male': record_data['is_male'],  'height': record_data['height'], 'weight': record_data['weight'],
	        'icu_los': record_data['icu_los'], 'hosp_los': record_data['hosp_los']
            }

        context = {'user_id': record_data['subject_id']}

        return {'information': information, 'context': context}

    def _create_mimic_admission(self, record_data):
        global SAMPLE_PERIOD

        SAMPLE_PERIOD = "Admission"

        admission = {
            'hadm_id': record_data['hadm_id'],
            'admittime': record_data['admittime'],
            'dischtime': record_data['dischtime'],
            'deathtime': record_data['deathtime'],
            'admission_type': record_data['admission_type'],
            'admission_location': record_data['admission_location'],
            'insurance': record_data['insurance'],
            'ethnicity': record_data['ethnicity'],
            'diagnosis': record_data['diagnosis'],
            'hospital_expire_flag': record_data['hospital_expire_flag']
        }

        return {"admission": admission, "context": {'subject_id': str(record_data['subject_id'])}}


def parse_file(file_path):
    _file_data = []
    _file_format = file_path.lower().rpartition('.')[-1]
    if _file_format == "csv":
        with open(file_path, 'r') as file:
            csv_reader = csv.DictReader(file)
            for rows in csv_reader:
                _file_data.append(dict(rows))
    elif _file_format == "json":
        with open(file_path, 'r') as file:
            _file_data = json.load(file)

    return _file_data


def prepare_samples(data_, document_factory_):
    global mongo_prescriptions
    if DEVICE == "mimic_prescriptions":
        mongo_prescriptions = COLLECTION.find(
            {
            "device_id": devices[DEVICE.lower()],
            "measurements": {"$exists": {'drug': True}},
            "context": {"user_id": users[USERNAME.lower()]}
            },
            {"measurements": 1}
        )

    prepared_samples = []
    for record in tqdm(data_):

        if DEVICE == "mimic_prescriptions":
            if record['startdate'] == '' and record['enddate'] == '':
                continue
        samples = document_factory_.create_samples(
            device_id=devices[DEVICE],
            record_data=record)

        if isinstance(samples, dict):
            samples = [samples]

        for sample in samples:
            context = sample.get('context', None)
            sample_key = [key for key in sample.keys() if key != 'context']
            sample_key = sample_key[0]

            samples_to_insert = sample.get(sample_key, None)

            if devices[DEVICE.lower()] == "8":
                prepared_samples.append(
                    {"sample_dict":
                        {
                            "user_id": users[USERNAME.lower()],
                            "device_id": devices[DEVICE.lower()],
                            "type": "mimic_sepsis",
                            **context
                        },
                     "collection_dict":
                        {
                            "$push": {'information': sample['information']}
                        }
                     }
                )
            if devices[DEVICE.lower()] == "9":
                prepared_samples.append(
                    {"sample_dict":
                        {
                            "user_id": users[USERNAME.lower()],
                            "device_id": devices[DEVICE.lower()],
                            "type": "mimic_admission",
                            **context,
                            "n_samples": {"$lt": MAX_SAMPLES}
                        },
                     "collection_dict":
                        {
                            "$push": {'admission': sample['admission']},
                            "$inc": {"n_samples": int(1)}
                        }
                     }
                )
            else:
                for sample_type, sample_to_insert in samples_to_insert.items():
                    if sample_type == "HEART_RATE" and sample_to_insert.get('value', None) == 255:
                        continue
                    timestamp = sample_to_insert.get('timestamp', None)
                    if timestamp:
                        day = datetime.fromisoformat(
                            timestamp.date().isoformat())
                        min_value = timestamp
                        max_value = timestamp
                    else:
                        try:
                            day = sample_to_insert['day']
                            min_value = sample_to_insert['day']
                            max_value = sample_to_insert['day']
                        except:
                            day = 'NA'
                            min_value = 'NA'
                            max_value = 'NA'


                    prepared_samples.append(
                        {"sample_dict":
                            {
                            "user_id": users[USERNAME.lower()],
                            "period": SAMPLE_PERIOD,
                            "device_id": devices[DEVICE.lower()],
                            "n_samples": {"$lt": MAX_SAMPLES},
                            "type": sample_type,
                            "day": day,
                            **context
                            },
                        "collection_dict":
                            {
                            "$push": {'diagnoses': sample['diagnoses']['diagnoses']},
                            "$min": {"first": min_value},
                            "$max": {"last": max_value},
                            "$inc": {"n_samples": int(1)}
                            }
                        }
                    )


    return tuple(prepared_samples)

def upload_samples(data_, document_factory_):

    samples_for_mongodb = prepare_samples(data_ = data_, document_factory_ = document_factory_)

    for sample in tqdm(samples_for_mongodb):
        COLLECTION.update_one(
            sample['sample_dict'],
            sample['collection_dict'],
            upsert=True
        )


def print_samples(data_, document_factory_, quantity=None):

    samples_for_mongodb = prepare_samples(data_ = data_, document_factory_ = document_factory_)

    if quantity:
        for sample in tqdm(samples_for_mongodb[:quantity]):
            print(sample['sample_dict'])
            print(sample['collection_dict'])
    else:
        for sample in tqdm(samples_for_mongodb):
            print(sample['sample_dict'])
            print(sample['collection_dict'])


def export_types(data_):
    types = []
    for record in data_:
        if record['label'] not in types:
            types.append(record['label'])
    with open('/home/danielbloor/types.txt', 'w') as file:
        file.writelines(types)


@click.command()
@click.argument("--filepath")
@click.option("-u", "--user_name", prompt="Monitoring device user's name",
              help="Name of monitoring device user")
@click.option("-d", "--device", prompt="Device name",
              help="Name of monitoring device")
@click.option("-s", "--sample_period", prompt="Interval of sample period",
              help="The interval a sample period represents.")
@click.option("-m", "--max_samples", prompt="Maximum samples per document",
              help="Most samples to upload per MongoDB document.", default=1500)
@click.option("-db", "--database", help="Database to upload to.", default='COPH')
@click.option("-c", "--collection",
              help="Collection to upload to.", default='measurements')
def main(filepath: str, sample_period: str,
         database: str, max_samples: int,
         collection: str, device: str,
         user_name: str):
    # Setup
    global MAX_SAMPLES
    global SAMPLE_PERIOD
    MAX_SAMPLES = max_samples
    SAMPLE_PERIOD = sample_period

    world = owl.default_world
    _, filename = owl.tempfile.mkstemp()
    world.set_backend(filename=filename)
    onto = world.get_ontology(COPH_IRI).load()

    client = pm.MongoClient()
    db = client[database]
    collection = db[collection]
    data = parse_file(filepath)
    document_factory = DocumentFactory()

    upload_samples(data_=data, document_factory_=document_factory)


def direct_main():
    # Setup
    global MAX_SAMPLES
    global SAMPLE_PERIOD
    global COLLECTION
    global USERNAME
    global DEVICE
    MAX_SAMPLES = 1500
    SAMPLE_PERIOD = "1/min"

    # Here for temporary testing
    #world = owl.default_world
    #_, filename = owl.tempfile.mkstemp()
    # world.set_backend(filename=filename)
    # owl.onto_path.append("/home/danielbloor/Projects/ontologies/monitoring_ontologies/HMO_0.7/")
    #onto = world.get_ontology(COPH_IRI).load()

    filepath = f"{START_PATH}{FILE_NAME}"
    client = pm.MongoClient()
    db = client[DATABASE]
    COLLECTION = db["mimic"]
    data = parse_file(filepath)
    document_factory = DocumentFactory()
    USERNAME = "anonymous"
    DEVICE = "mimic_diagnoses"
    if DEBUG_MODE:
        print_samples(data_=data[:], document_factory_=document_factory, quantity=100)
    else:
        upload_samples(data_=data[:], document_factory_=document_factory)


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    direct_main()
