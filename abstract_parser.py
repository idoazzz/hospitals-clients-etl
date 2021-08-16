import abc

import numpy as np
from attrdict import AttrDict
from jsonschema import validate


class AbstractDataParser:
    """Parsing and converting data into a schema.

    * We validate our conversion with pre-defined schema.
    Each property processor can process property before we used it.
    Also, it possible to add functionality of conversion tables or classes for more accurate
    properties. e.g ISR => Israel if we use conversion in the processors.

    Attributes:
        row (DataFrame): Patient row from excel file.
    """
    BASE_PROPERTIES_SCHEMA = NotImplemented

    def __init__(self, row):
        self.row = row
        self.row = self.row.replace({np.NAN: None})
        self.unused_fields = self.row.copy()

    @property
    @abc.abstractmethod
    def property_translations(self):
        pass

    @property
    @abc.abstractmethod
    def base_properties_preprocessors(self):
        pass

    def to_dict(self):
        patient = {}
        # Insert only keys which has value.
        # e.g We have a patient without MRN, don't attach MRN to patient.
        for property, property_parser in self.base_properties_preprocessors.items():
            value = property_parser()

            try:
                self.unused_fields = self.unused_fields.drop(
                    self.property_translations[property]
                )

            except KeyError:
                pass

            if value is not None:
                patient[property] = value

        # Adding rest of the properties (not base).
        for field in self.unused_fields.keys():
            patient[field] = self.unused_fields[field]

        # Check that the patient holds schema requirements.
        validate(patient, schema=self.BASE_PROPERTIES_SCHEMA)
        return AttrDict(patient)