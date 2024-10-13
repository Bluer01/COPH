from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any, Optional
import mongoengine as me

# Type aliases for clarity
DeviceID = str
UserID = str

@dataclass
class Measurement:
    """
    Represents a single measurement.

    :param timestamp: The time when the measurement was taken
    :param value: The value of the measurement
    :param risk_score: Optional risk score associated with the measurement
    """
    timestamp: datetime
    value: float
    risk_score: Optional[int] = None

@dataclass
class Document:
    """
    Represents a document containing measurement data.

    :param user_id: Identifier for the user
    :param type: Type of the measurement
    :param device_id: Identifier for the device
    :param period: Sampling period
    :param day: Date of the measurements
    :param valueuom: Unit of measurement
    :param measurements: List of measurements
    :param summaries: Optional dictionary of summary data
    """
    user_id: UserID
    type: str
    device_id: DeviceID
    period: str
    day: datetime
    valueuom: str
    measurements: List[Measurement]
    summaries: Dict[str, Any] = field(default_factory=dict)

class Metadata(me.Document):
    """
    Mongoengine document class for metadata.
    """
    document_version = me.StringField(required=True)
    ontology_name = me.StringField(required=True)
    ontology_version = me.StringField(required=True)
    mappings = me.DictField(required=True)

# Additional constants
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