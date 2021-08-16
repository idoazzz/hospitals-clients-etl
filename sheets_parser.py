import abc
import os

import pandas as pd

from abstract_parser import AbstractDataParser


class AbstractPatientParser(AbstractDataParser):
    """Abstract Patient define the base property's each patient has  and rep of rest props.

    AbstractPatient holds processors. Each property processor can process property before
    we used it. Also, it possible to add functionality of conversion tables or classes for
    more accurate properties. e.g ISR => Israel if we use conversion in the processors.
    """
    BASE_PROPERTIES_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "number"},
            "first_name": {"type": "string"},
            "last_name": {"type": "string"},
            "mrn": {"type": "string"},
            "is_deceased": {"type": "boolean"},
            "gender": {"type": "string"},
            "sex": {"enum": ["Female", "Male"]},
            "city": {"type": ["string", "null"]},
            "address": {"type": "string"},
            "state": {"type": "string"},
            "zip": {"type": "string"},
        },
        "required": ["id"]
    }

    @property
    def base_properties_preprocessors(self):
        return {
            "id": self.process_id,
            "first_name": self.process_first_name,
            "last_name": self.process_last_name,
            "mrn": self.process_mrn,
            "is_deceased": self.process_is_deceased,
            "gender": self.process_gender,
            "sex": self.process_sex,
            "city": self.process_city,
            "address": self.process_address,
            "state": self.process_state,
            "zip": self.process_zip,
        }

    def process_id(self):
        return self.row[self.property_translations["id"]]

    def process_first_name(self):
        return self.row[self.property_translations["first_name"]]

    def process_last_name(self):
        return self.row[self.property_translations["last_name"]]

    def process_mrn(self):
        return self.row[self.property_translations["mrn"]]

    def process_is_deceased(self):
        return self.row[self.property_translations["is_deceased"]]

    def process_gender(self):
        return self.row[self.property_translations["gender"]]

    def process_sex(self):
        return self.row[self.property_translations["sex"]]

    def process_state(self):
        return self.row[self.property_translations["state"]]

    def process_address(self):
        return self.row[self.property_translations["address"]]

    def process_city(self):
        return self.row[self.property_translations["city"]]

    def process_zip(self):
        return self.row[self.property_translations["zip"]]


class AbstractTreatmentParser(AbstractDataParser):
    """Abstract Treatment define the base property's each patient has and rep of rest props."""

    BASE_PROPERTIES_SCHEMA = {
        "type": "object",
        "properties": {
            "id": {"type": "number"},
            "patient_id": {"type": "number"},
            "start_date": {"type": "string"},
            "end_date": {"type": "string"},
            "display_name": {"type": "string"},
            "diagnoses": {"type": "string"},
            "status": {"type": "string"},
        },
        "required": ["id"]
    }

    @property
    def base_properties_preprocessors(self):
        return {
            "id": self.process_id,
            "patient_id": self.process_patient_id,
            "start_date": self.process_start_date,
            "end_date": self.process_end_date,
            "display_name": self.process_display_name,
            "diagnoses": self.process_diagnoses,
            "status": self.process_status,
        }

    def process_id(self):
        return self.row[self.property_translations["id"]]

    def process_patient_id(self):
        return self.row[self.property_translations["patient_id"]]

    def process_start_date(self):
        return self.row[self.property_translations["start_date"]]

    def process_end_date(self):
        return self.row[self.property_translations["end_date"]]

    def process_display_name(self):
        return self.row[self.property_translations["display_name"]]

    def process_diagnoses(self):
        return self.row[self.property_translations["diagnoses"]]

    def process_status(self):
        return self.row[self.property_translations["status"]]


class AbstractSheetParser:
    """Loads hospital data into DB.

    Notes:
        DB Structure:
        * hospital 1
            ** patient 1
                **** treatment 1
                **** treatment 2
            ** patient 2

    Attributes:
        db_client (MongoClient): MongoDB client.
    """
    CHUNK_SIZE = 1024

    def __init__(self, db_client, hospital_name, patients_file_path, treatment_file_path):
        self.db_client = db_client
        self.hospital_name = hospital_name

        db_name = os.getenv("DB_NAME", default="tailormed")
        db = getattr(db_client, db_name)

        if hospital_name not in db.collection_names():
            db.create_collection(hospital_name)

        self.patients_collection = getattr(db, hospital_name)
        self.patients_file = patients_file_path
        self.treatments_file = treatment_file_path

    def import_sheet(self):
        """Import daily hospital sheets."""
        self.import_patients()
        self.import_treatments()

    def import_patients(self):
        """Import patients into DB."""
        for chunk in pd.read_csv(self.patients_file, chunksize=self.CHUNK_SIZE):
            for row in chunk.iterrows():
                patient = self.parse_patient(row[1])
                self.update_patient_document(patient)

    def import_treatments(self):
        """Import treatments into DB."""
        for chunk in pd.read_csv(self.treatments_file, chunksize=self.CHUNK_SIZE):
            for row in chunk.iterrows():
                treatment = self.parse_treatment(row[1])
                self.push_treatment_document(treatment)

    @abc.abstractmethod
    def parse_patient(self, row):
        """

        Args:
            row (DataFrame): Row that represent patient.

        Returns:
            dict. Unified representation of patient.
        """
        pass

    @abc.abstractmethod
    def parse_treatment(self, row):
        """

        Args:
            row (DataFrame): Row that represent treatment.

        Returns:
            dict. Unified representation of treatment.
        """
        pass

    def update_patient_document(self, patient):
        """Update specific patient document.

        patient (AbstractPatient): Specific patient object.
        """
        self.patients_collection.update_one({"patient_id": patient.id},
                                            {"$set": patient}, upsert=True)

    def push_treatment_document(self, treatment):
        """Update specific treatment document.

        treatment (AbstractTreatment): Specific treatment object.
        """
        self.patients_collection.update_one({"patient_id": treatment.patient_id},
                                            {
                                                "$push": {
                                                    "treatments": treatment
                                                }
                                            }, upsert=True)
