import pymongo as pymongo

from parsers.hospital_1 import HospitalOneSheetParser


def main():
    db_client = pymongo.MongoClient("mongodb://localhost:27017/")
    parser = HospitalOneSheetParser(db_client,
                                    "hospital1",
                                    "./dataset/hospital_1_Patient.csv",
                                    "./dataset/hospital_1_Treatment.csv")
    parser.import_sheet()


if __name__ == '__main__':
    main()
