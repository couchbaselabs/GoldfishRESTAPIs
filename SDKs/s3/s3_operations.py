import json
import csv
import pandas as pd
import random
import string
from faker import Faker
from fastavro import writer, parse_schema
from SDKs.s3.s3_config import s3Config


class s3Operations:
    def __init__(self, config):
        if not isinstance(config, s3Config):
            raise ValueError("config parameter must be an instance of s3Config class")

        self.spec_file = config
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
        elif file_type == "txt":
            return self.create_text_file(doc_size)
        elif file_type == "avro":
            return self.create_avro_file(doc_size)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def create_json_file(self, doc_size):
        data = self._generate_data(doc_size)
        return json.dumps(data, indent=2)

    def create_csv_file(self, doc_size):
        data = self._generate_data(doc_size)
        return self._convert_to_csv(data)

    def create_tsv_file(self, doc_size):
        data = self._generate_data(doc_size)
        return self._convert_to_tsv(data)

    def create_parquet_file(self, doc_size, output_path='output.parquet'):
        data = self._generate_data(doc_size)
        self._convert_to_parquet(data, output_path)
        return output_path

    def create_text_file(self, doc_size):
        data = self._generate_data(doc_size)
        return str(data)

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
        output_csv = "output.csv"
        with open(output_csv, "w", newline="") as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=data.keys())
            csv_writer.writeheader()
            csv_writer.writerow(data)
        return output_csv

    def _convert_to_tsv(self, data):
        output_tsv = "output.tsv"
        with open(output_tsv, "w", newline="") as tsvfile:
            tsv_writer = csv.DictWriter(tsvfile, fieldnames=data.keys(), delimiter='\t')
            tsv_writer.writeheader()
            tsv_writer.writerow(data)
        return output_tsv

    def _convert_to_parquet(self, data, output_path):
        table = pd.DataFrame([data])
        table.to_parquet(output_path, index=False, engine='pyarrow')

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

    def create_avro_file(self, doc_size, output_path='output.avro'):
        data = self._generate_data(doc_size)

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
                {"name": "reviews", "type": {"type": "array", "items": {"type": "record", "name": "Review", "fields": [
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

        # Write Avro file
        with open(output_path, "wb") as avro_file:
            writer(avro_file, parse_schema(avro_schema), avro_data)

        return output_path
