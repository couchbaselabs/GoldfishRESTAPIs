import csv
import json
import pandas as pd
import random
import string
import tempfile
from faker import Faker
from fastavro import writer, parse_schema


class s3Operations:
    def __init__(self):
        self.faker = Faker()

    def create_file_with_required_file_type(self, file_type, doc_size):
        if file_type == "json":
            return self.create_json_file(doc_size)
        elif file_type == "csv":
            return self.create_csv_file(doc_size)
        elif file_type == "tsv":
            return self.create_tsv_file(doc_size)
        elif file_type == "parquet":
            return self.create_parquet_file(doc_size)
        elif file_type == "avro":
            return self.create_avro_file(doc_size)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def create_unique_temporary_file(self, base_name, file_extension):
        unique_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        _, temp_path = tempfile.mkstemp(prefix=f"{base_name}_{unique_suffix}_", suffix=file_extension)
        return temp_path

    def create_json_file(self, doc_size):
        data = self._generate_data(doc_size)
        return json.dumps(data, indent=2)

    def create_csv_file(self, doc_size):
        data = self._generate_data(doc_size)
        return self._convert_to_csv(data)

    def create_tsv_file(self, doc_size):
        data = self._generate_data(doc_size)
        return self._convert_to_tsv(data)

    def create_parquet_file(self, doc_size):
        data = self._generate_data(doc_size)
        return self._convert_to_parquet(data)

    def _convert_to_avro_record(self, data, schema):
        record = {}
        for field in schema["fields"]:
            field_name = field["name"]
            if field_name in data:
                record[field_name] = data[field_name]
            else:
                record[field_name] = None
        return record

    def _convert_to_csv(self, data):
        output_csv = self.create_unique_temporary_file("output", '.csv')
        with open(output_csv, "w", newline="") as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=data.keys())
            csv_writer.writeheader()
            csv_writer.writerow(data)
        return output_csv

    def _convert_to_tsv(self, data):
        output_tsv = self.create_unique_temporary_file("output", '.tsv')
        with open(output_tsv, "w", newline="") as tsvfile:
            tsv_writer = csv.DictWriter(tsvfile, fieldnames=data.keys(), delimiter='\t')
            tsv_writer.writeheader()
            tsv_writer.writerow(data)
        return output_tsv

    def _convert_to_parquet(self, data):
        output_parquet = self.create_unique_temporary_file("output", '.parquet')
        table = pd.DataFrame([data])
        table.to_parquet(output_parquet, index=False, engine='pyarrow')
        return output_parquet

    def _generate_data(self, doc_size):
        data = {
            "address": self.faker.word(),
            "avgrating": round(random.uniform(1, 5), 2),
            "city": self.faker.word(),
            "country": self.faker.word(),
            "email": self.faker.email(),
            "freebreakfast": self.faker.boolean(),
            "freeparking": self.faker.boolean(),
            "name": self.faker.word(),
            "phone": self.faker.phone_number(),
            "price": round(random.uniform(100, 1000), 2),
            "publiclikes": [self.faker.word() for _ in range(5)],
            "reviews": [
                {"user": self.faker.user_name(), "comment": self.faker.sentence()} for _ in range(2)
            ],
            "type": self.faker.word(),
            "url": self.faker.url(),
        }

        # Ensure the doc size
        extra_size = doc_size - len(str(data))
        if extra_size > 0:
            data["extra"] = ''.join(random.choices(string.ascii_letters, k=extra_size))

        return data

    def create_avro_file(self, doc_size, avro_schema=None):
        data = self._generate_data(doc_size)

        if not avro_schema:
            # Avro schema definition
            avro_schema = {
                "type": "record",
                "name": "YourData",
                "fields": [
                    {"name": "address", "type": ["null", "string"], "default": None},
                    {"name": "avgrating", "type": ["null", "double"], "default": None},
                    {"name": "city", "type": ["null", "string"], "default": None},
                    {"name": "country", "type": ["null", "string"], "default": None},
                    {"name": "email", "type": ["null", "string"], "default": None},
                    {"name": "freebreakfast", "type": ["null", "boolean"], "default": None},
                    {"name": "freeparking", "type": ["null", "boolean"], "default": None},
                    {"name": "name", "type": ["null", "string"], "default": None},
                    {"name": "phone", "type": ["null", "string"], "default": None},
                    {"name": "price", "type": ["null", "double"], "default": None},
                    {"name": "publiclikes", "type": {"type": "array", "items": "string"}},
                    {"name": "reviews",
                     "type": {"type": "array", "items": {"type": "record", "name": "Review", "fields": [
                         {"name": "user", "type": ["null", "string"], "default": None},
                         {"name": "comment", "type": ["null", "string"], "default": None}
                     ]}}},
                    {"name": "type", "type": ["null", "string"], "default": None},
                    {"name": "url", "type": ["null", "string"], "default": None},
                    {"name": "extra", "type": ["null", "string"], "default": None}
                ]
            }

        # Convert data to Avro format
        avro_data = [self._convert_to_avro_record(data, avro_schema)]

        temp_path = tempfile.mktemp(suffix='.avro')
        with open(temp_path, "wb") as avro_file:
            writer(avro_file, parse_schema(avro_schema), avro_data)
        return temp_path
