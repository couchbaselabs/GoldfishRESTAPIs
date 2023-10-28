import csv
import faker
import json
import pandas as pd
import random
import string
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from faker import Faker
from fastavro import writer, parse_schema

import Docloader.docgen_template as template


class s3Operations:
    def __init__(self):
        self.faker = Faker()

    def create_file_with_required_file_type(self, file_type, doc_size=1024, num_rows=1):
        if file_type == "json":
            return self.create_json_file(doc_size=doc_size, num_rows=num_rows)
        elif file_type == "csv":
            return self.create_csv_file(num_rows=num_rows, doc_size=doc_size)
        elif file_type == "tsv":
            return self.create_tsv_file(num_rows=num_rows, doc_size=doc_size)
        elif file_type == "parquet":
            return self.create_parquet_file(num_rows=num_rows, doc_size=doc_size)
        elif file_type == "avro":
            return self.create_avro_file(num_rows=num_rows)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    def create_unique_temporary_file(self, base_name, file_extension):
        unique_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
        _, temp_path = tempfile.mkstemp(prefix=f"{base_name}_{unique_suffix}_", suffix=file_extension)
        return temp_path

    def create_json_file(self, num_rows, doc_size):
        data = self._generate_data_multiple_rows(num_rows=num_rows, doc_size=doc_size)
        return json.dumps(data, indent=2)

    def create_csv_file(self, num_rows, doc_size):
        data = self._generate_data_multiple_rows(num_rows=num_rows, doc_size=doc_size)
        return self._convert_to_csv(data)

    def create_tsv_file(self, num_rows, doc_size):
        data = self._generate_data_multiple_rows(num_rows=num_rows, doc_size=doc_size)
        return self._convert_to_tsv(data)

    def create_parquet_file(self, num_rows, doc_size):
        data = self._generate_data_multiple_rows(num_rows=num_rows, doc_size=doc_size)
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
        fieldnames = data[0].keys()

        with open(output_csv, "w", newline="") as csvfile:
            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            csv_writer.writeheader()

            for row in data:
                csv_writer.writerow(row)

        return output_csv

    def _convert_to_tsv(self, data):
        output_tsv = self.create_unique_temporary_file("output", '.tsv')
        fieldnames = data[0].keys()

        with open(output_tsv, "w", newline="") as tsvfile:
            tsv_writer = csv.DictWriter(tsvfile, fieldnames=fieldnames, delimiter='\t')
            tsv_writer.writeheader()

            for row in data:
                tsv_writer.writerow(row)

        return output_tsv

    def _convert_to_parquet(self, data):
        output_parquet = self.create_unique_temporary_file("output", '.parquet')
        table = pd.DataFrame(data)
        table.to_parquet(output_parquet, index=False, engine='pyarrow')
        return output_parquet

    def _generate_data(self, doc_size=1024):
        data = {
            "address": self.faker.address(),
            "avg_ratings": self.faker.pyfloat(left_digits=1, right_digits=1, positive=True),
            "city": self.faker.city(),
            "country": self.faker.country(),
            "email": self.faker.email(),
            "free_breakfast": int(self.faker.boolean()),
            "free_parking": int(self.faker.boolean()),
            "name": self.faker.name(),
            "phone": self.faker.random_element(
                elements=(1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0,
                          7000.0, 8000.0, 9000.0, 10000.0)),
            "price": self.faker.random_element(
                elements=(1000.0, 2000.0, 3000.0, 4000.0, 5000.0, 6000.0,
                          7000.0, 8000.0, 9000.0, 10000.0)),
            "public_likes": [self.faker.word() for _ in range(5)],
            "reviews": [
                {
                    "date": self.faker.date_time_this_decade().isoformat(),
                    "author": self.faker.name(),
                    "rating": {
                        "value": random.randint(1, 10),
                        "cleanliness": random.randint(1, 10),
                        "overall": random.randint(1, 10)
                    }
                } for _ in range(2)
            ],
            "type": "Hotel",
            "url": self.faker.url()
        }

        # Ensure the doc size
        extra_size = doc_size - len(str(data))
        if extra_size > 0:
            data["extra"] = ''.join(random.choices(string.ascii_letters, k=extra_size))

        return data

    def _generate_data_multiple_rows(self, num_rows, doc_size=1024, num_workers=1):
        data_list = []
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Generate data concurrently
            futures = [executor.submit(self._generate_data, doc_size=doc_size) for _ in range(num_rows)]

            # Collect the results
            for future in futures:
                data_list.append(future.result())
        print(time.time() - start_time)
        return data_list

    def create_avro_file(self, num_rows, avro_schema=None):
        data = self._generate_data_multiple_rows(num_rows)

        if not avro_schema:
            # Avro schema definition
            avro_schema = {
                "type": "record",
                "name": "YourData",
                "fields": [
                    {"name": "address", "type": ["null", "string"], "default": None},
                    {"name": "avg_ratings", "type": ["null", "double"], "default": None},
                    {"name": "city", "type": ["null", "string"], "default": None},
                    {"name": "country", "type": ["null", "string"], "default": None},
                    {"name": "email", "type": ["null", "string"], "default": None},
                    {"name": "free_breakfast", "type": ["int"], "default": 0},
                    {"name": "free_parking", "type": ["int"], "default": 1},
                    {"name": "name", "type": ["null", "string"], "default": None},
                    {"name": "phone", "type": ["float"], "default": 0.0},
                    {"name": "price", "type": ["float"], "default": 0.0},
                    {"name": "public_likes", "type": {"type": "array", "items": "string"}},
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
        avro_data = [self._convert_to_avro_record(row, avro_schema) for row in data]

        temp_path = tempfile.mktemp(suffix='.avro')
        with open(temp_path, "wb") as avro_file:
            writer(avro_file, avro_schema, avro_data)

        return temp_path
