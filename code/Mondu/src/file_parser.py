import csv
import json
from typing import List, Dict, Any

def parse_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Parse a CSV or JSON file and return its contents as a list of dictionaries.

    :param file_path: Path to the file to be parsed
    :return: List of dictionaries containing the file data
    :raises ValueError: If an unsupported file format is provided
    """
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

def get_days(start_date: datetime, end_date: datetime) -> List[str]:
    """
    Get a list of days between two dates as strings.

    :param start_date: Start date
    :param end_date: End date
    :return: List of days as strings in format 'YYYY-MM-DD'
    """
    return [date.strftime("%Y-%m-%d") for date in day_generator(start_date, end_date)]

def day_generator(start_date: datetime, end_date: datetime):
    """
    Generator function to yield days between two dates.

    :param start_date: Start date
    :param end_date: End date
    :yield: Each day between start_date and end_date
    """
    for date in range(int((end_date - start_date).days)):
        yield start_date + timedelta(date)

# You might want to add more parsing functions here if needed
# For example, functions to parse specific parts of your data files,
# or to convert certain fields to the right data types.