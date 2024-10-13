import json
from typing import Dict, Any

# Constants
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

# Default configuration
DEFAULT_CONFIG = {
    'DEBUG_MODE': True,
    'MAX_SAMPLES': 1500,
    'SAMPLE_PERIOD': '',
    'ONTO_PATH': '/path/to/ontologies',
    'COPH_IRI': COPH_IRI,
    'DATABASE': DATABASE,
    'devices': {
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
    },
    'users': {
        "daniel bloor": "0",
        "thirty six": "36",
        "anonymous": "anon"
    }
}

def load_config(config_path: str = 'config.json') -> Dict[str, Any]:
    """
    Load configuration from a JSON file and merge it with default configuration.

    :param config_path: Path to the configuration JSON file
    :return: Merged configuration dictionary
    """
    try:
        with open(config_path, 'r') as config_file:
            file_config = json.load(config_file)
    except FileNotFoundError:
        print(f"Config file not found at {config_path}. Using default configuration.")
        file_config = {}
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {config_path}. Using default configuration.")
        file_config = {}

    # Merge file config with default config, prioritizing file config
    config = {**DEFAULT_CONFIG, **file_config}

    return config

# Additional configuration-related functions can be added here if needed