from sheets_parser import AbstractSheetParser, AbstractPatientParser, AbstractTreatmentParser


class HospitalOnePatientParser(AbstractPatientParser):
    @property
    def property_translations(self):
        return {
            "id": "PatientID",
            "first_name": "FirstName",
            "last_name": "LastName",
            "mrn": "MRN",
            "is_deceased": "IsDeceased",
            "gender": "Gender",
            "sex": "Sex",
            "city": "City",
            "address": "Address",
            "state": "State",
            "zip": "ZipCode",
        }

    def process_mrn(self):
        mrn_ = self.property_translations["mrn"]
        return str(self.row[mrn_])

    def process_is_deceased(self):
        is_deceased_ = self.property_translations["is_deceased"]
        return self.row[is_deceased_] == "Active"


class HospitalOneTreatmentParser(AbstractTreatmentParser):
    @property
    def property_translations(self):
        return {
            "id": "TreatmentID",
            "patient_id": "PatientID",
            "start_date": "StartDate",
            "end_date": "EndDate",
            "display_name": "DisplayName",
            "diagnoses": "Diagnoses",
            "status": "Active",
        }


class HospitalOneSheetParser(AbstractSheetParser):
    def parse_patient(self, row):
        return HospitalOnePatientParser(row).to_dict()

    def parse_treatment(self, row):
        return HospitalOneTreatmentParser(row).to_dict()
