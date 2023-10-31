"""
Docloader to create and upload document to various sources for goldFish
Sources include mongoDB, dynamoDB, cassandra, etc..
"""
import concurrent
import concurrent.futures
import faker
import json
import logging
import os
import random
import string
import time
from concurrent.futures import ThreadPoolExecutor

import Docloader.docgen_template as template
import SDKs.DynamoDB.dynamo_sdk as dynamoSdk
from SDKs.DynamoDB.dynamo_sdk import DynamoDb
from SDKs.MongoDB.MongoConfig import MongoConfig
from SDKs.MongoDB.MongoSDK import MongoSDK
from SDKs.MySQL.MySqlSDK import MySQLSDK
from SDKs.s3.s3_SDK import s3SDK
from SDKs.s3.s3_config import s3Config
from SDKs.s3.s3_operations import s3Operations


class DocLoader:
    """
    DocLoader class has the function to generate document based on size and number.
    This class can be used to upload docs to mango, cassandra and dynamodb
    :params:
    -document_size: Default is 1024
    -no_of_docs: Default is 100
    """

    def __init__(self, document_size=1024, no_of_docs=100):
        self.document_size = document_size
        self.no_of_docs = no_of_docs
        self.index = 0
        self.stop_mongo_loader = False
        self.stop_dynamo_loader = False
        self.stop_s3_loader = False
        self.stop_mysql_loader = False
        self.stop_dynamo_loader = False

    def float_to_str(self, obj: any) -> any:
        """
        Change float values to str recursively in a given object.

        :param obj: The input object to process.
        :return: The modified object with float values converted to str.
        """
        if isinstance(obj, float):
            return str(obj)
        elif isinstance(obj, list):
            return [self.float_to_str(item) for item in obj]
        elif isinstance(obj, dict):
            return {key: self.float_to_str(value) for key, value in obj.items()}
        else:
            return obj

    def calculate_optimal_batch_size(self, target_docs, current_docs, max_batch_size, upper_factor=0.1,
                                     lower_factor=0.01):
        """
            Calculate the optimal batch size for CRUD operations based on the current and target number of documents.

            Parameters:
            - target_docs (int): The target number of documents to be inserted or updated.
            - current_docs (int): The current number of documents in the collection.
            - max_batch_size (int): The maximum allowed batch size for the operations.
            - upper_factor (float): The upper factor to adjust the batch size. Default is 0.1 (10% increase).
            - lower_factor (float): The lower factor to adjust the batch size. Default is 0.01 (1% decrease).

            Returns:
            - int: The calculated optimal batch size.
        """
        doc_difference = target_docs - current_docs
        initial_batch_size = int(doc_difference * upper_factor)
        initial_batch_size = min(max_batch_size, max(1, initial_batch_size))
        if initial_batch_size < doc_difference * lower_factor:
            initial_batch_size = int(doc_difference * lower_factor)
        return min(max_batch_size, max(1, initial_batch_size))

    def is_loader_running(self, db):
        """
           Check if the loader is currently running.

           Returns:
               bool: True if the loader is running, False otherwise.
       """
        if db == "mongo":
            return not self.stop_mongo_loader
        elif db == "s3":
            return not self.stop_s3_loader
        elif db == "mysql":
            return not self.stop_mysql_loader
        elif db == "dynamo":
            return not self.stop_dynamo_loader

    def stop_running_loader(self, db):
        """
             Stop the currently running loader.

             This method sets the `stop_loader` flag to True, indicating the loader to stop its operation.
         """
        if db == "mongo":
            self.stop_mongo_loader = True
        elif db == "s3":
            self.stop_s3_loader = True
        elif db == "mysql":
            self.stop_mysql_loader = True
        elif db == "dynamo":
            self.stop_dynamo_loader = True

    def start_running_loader(self, db):
        """
            Start the loader.

            This method sets the `stop_loader` flag to False, allowing the loader to
            continue or start its operation.
        """
        if db == "mongo":
            self.stop_mongo_loader = False
        elif db == "s3":
            self.stop_s3_loader = False
        elif db == "mysql":
            self.stop_mysql_loader = False
        elif db == "dynamo":
            self.stop_dynamo_loader = False

    def generate_docs(self, index=None):
        """
        Generates a single document
        :param index: unused here
        :return: a dictionary
        """
        faker_instance = faker.Faker()
        hotel = template.Hotel(faker_instance)
        hotel.generate_document(faker_instance, self.document_size, index)
        doc = json.loads(json.dumps(hotel, default=lambda o: o.__dict__))
        del hotel, faker_instance
        return doc

    def generate_fake_documents_concurrently(self, batch_size=25, num_workers=4):
        """
        Increase the document generation process
        :param batch_size: Default 25
        :param num_workers: Default 4
        :return: list of documents
        """
        documents = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_to_doc = {executor.submit(self.generate_docs): _ for _ in range(batch_size)}
            for future in concurrent.futures.as_completed(future_to_doc):
                try:
                    document = future.result()
                    documents.append(document)
                except Exception as err:
                    print(f"An error occurred: {err}")
        return documents

    # -- DYNAMODB --
    def load_doc_to_dynamo(self, access_key, secret_key, session_token=None, table=None, region_name=None,
                           batch_size=1000, max_concurrent_batches=3):
        """
        :param table: dynamoDb table name
        :param batch_size:
        :param max_concurrent_batches:
        :param region_name: Region in which dynamodb table is deployed
        Either region or url is required, table name is required if table already exist
        """
        start = time.time()
        dynamo_obj = dynamoSdk.DynamoDb(access_key=access_key, secret_key=secret_key, session_token=session_token,
                                        table=table, region=region_name)
        with concurrent.futures.ThreadPoolExecutor(max_concurrent_batches) as executor:
            futures = []
            for i in range(0, self.no_of_docs, batch_size):
                batch_start = i
                batch_end = min(i + batch_size, self.no_of_docs)
                futures.extend(executor.submit(self.generate_docs, index)
                               for index in range(batch_start, batch_end))

            for future in concurrent.futures.as_completed(futures):
                try:
                    document = future.result()
                    if document:
                        # converting float to str for dynamoDB
                        # does not support float type
                        for key in document:
                            document[key] = self.float_to_str(document[key])

                        # Maintaining the size of the document after converting float to str.
                        if len(str(document).encode("utf-8")) > 1024:
                            padding_size = max(len(str(document).encode("utf-8")) -
                                               self.document_size, 0)
                            document['padding'] = document['padding'][:-padding_size]
                        dynamo_obj.write_batch([document])  # Write each document separately
                except Exception as err:
                    print(f"An error occurred: {err}")

        end = time.time()
        time_spent = end - start
        logging.info(f"Took {time_spent} to insert docs")

    def delete_from_dynamodb(self, access_key, secret_key, item_key, session_token=None, table=None, region_name=None,
                             condition_expression=None,
                             expression_attribute_values=None):
        """

        Args:
            table: table name of dynamodb
            region_name: region in which dynamodb is deployed
            item_key: item_key to be deleted
            condition_expression: condition to delete object
            expression_attribute_values: attribute values
            table name is required if table already exist
        """
        dynamo_obj = dynamoSdk.DynamoDb(access_key=access_key, secret_key=secret_key, session_token=session_token,
                                        table=table, region=region_name)
        dynamo_obj.delete_item(item_key=item_key, condition_expression=condition_expression,
                               expression_attribute_values=expression_attribute_values)

    def update_in_dynamo(self, access_key, secret_key, item_key, changed_object_json, session_token=None, table=None,
                         region_name=None):
        """

        Args:
            item_key: item to change
            changed_object_json: keys to change
            table:
            region_name:
            table name is required if table already exist
        """
        dynamo_obj = dynamoSdk.DynamoDb(access_key=access_key, secret_key=secret_key, session_token=session_token,
                                        table=table, region=region_name)
        dynamo_obj.update_item(item_key, changed_object_json)

    def delete_random_in_dynamodb(self, access_key, secret_key, p_key, session_token=None, table=None,
                                  region_name=None):
        dynamo_obj = dynamoSdk.DynamoDb(access_key=access_key, secret_key=secret_key, session_token=session_token,
                                        table=table, region=region_name)
        response = dynamo_obj.scan_table()
        if len(response) > 0:
            random_item = random.choice(response)
            try:
                self.delete_from_dynamodb(access_key=access_key, secret_key=secret_key, session_token=session_token,
                                          table=table, region_name=region_name,
                                          item_key={p_key: random_item[p_key]})
                print("Document deleted successfully")
            except Exception as e:
                logging.info(f"Error : {str(e)}")

    def setup_inital_load_on_dynamo_db(self, access_key, secret_key, region_name, p_key, initial_doc_count, table,
                                       session_token=None):

        dynamo_object = DynamoDb(access_key=access_key, secret_key=secret_key, session_token=session_token,
                                 table=table, region=region_name)

        initial_doc_count = int(initial_doc_count)
        current_docs = dynamo_object.get_live_item_count()
        while current_docs < initial_doc_count:
            batch_size = self.calculate_optimal_batch_size(current_docs, current_docs, 10000)
            self.load_doc_to_dynamo(access_key=access_key, secret_key=secret_key,
                                    session_token=session_token, table=table, region_name=region_name,
                                    batch_size=batch_size)
            current_docs = dynamo_object.get_live_item_count()

        while current_docs > initial_doc_count:
            self.delete_random_in_dynamodb(access_key=access_key, secret_key=secret_key,
                                           session_token=session_token, region_name=region_name,
                                           p_key=p_key, table=table)
            current_docs = dynamo_object.get_live_item_count()

    def perform_crud_on_dynamodb(self, access_key, secret_key, region_name, p_key, session_token=None, table=None,
                                 num_buffer=0):
        try:
            dynamo_object = DynamoDb(access_key=access_key, secret_key=secret_key, session_token=session_token,
                                     table=table, region=region_name)

            start_docs = dynamo_object.get_live_item_count()
            if num_buffer == 0:
                max_files = float('inf')
                min_files = 0
            else:
                max_files = start_docs + num_buffer
                min_files = max(int(start_docs - num_buffer), 0)

            while True:
                while not self.stop_dynamo_loader:
                    current_docs = dynamo_object.get_live_item_count()
                    operation = random.choice(["insert", "delete"])
                    if operation == "insert" and current_docs < max_files:
                        self.load_doc_to_dynamo(access_key=access_key, secret_key=secret_key,
                                                session_token=session_token, table=table, region_name=region_name,
                                                batch_size=1)
                        print('document inserted successfully')
                    elif operation == "delete" and current_docs > min_files:
                        self.delete_random_in_dynamodb(access_key=access_key, secret_key=secret_key,
                                                       session_token=session_token, region_name=region_name,
                                                       p_key=p_key, table=table)
        except Exception as e:
            raise Exception(e)

    # -- MONGODB --
    def load_doc_to_mongo(self, mongoConfig, collection_name, num_docs, batch_size):
        """
            Insert documents into the MongoDB collection.

            :param mongoConfig: MongoDB configuration -> object of class MongoConfig
            :param collection_name: Name of the collection to insert documents into
            :param num_docs: Total number of documents to insert
            :param batch_size: Number of documents to insert per batch.
        """
        mongo_obj = MongoSDK(mongoConfig)

        total_documents = num_docs
        batch_size = batch_size
        max_concurrent_batches = 500
        start = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_concurrent_batches) as executor:
            for i in range(0, total_documents, batch_size):
                batch_start = i
                batch_end = min(i + batch_size, total_documents)
                data_to_insert = self.generate_fake_documents_concurrently(
                    batch_end - batch_start)
                if data_to_insert:
                    executor.submit(mongo_obj.insert_multiple_document, collection_name, data_to_insert)
        end = time.time()
        logging.info(f"Took {end - start} to insert docs")

    def update_in_mongo(self, mongo_config, collection_name, update_from, update_to):
        """
            Update documents in the MongoDB collection based on the update query
            :param mongo_config: MongoDB configuration -> object of class MongoConfig
            :param collection_name: Name of the collection to update documents in
            :param update_from: The query used to filter documents to be updated
            :param update_to: The update operation to apply to matching documents.
        """
        mongo_obj = MongoSDK(mongo_config)
        mongo_obj.update_document(collection_name, update_from, update_to)

    def delete_from_mongo(self, mongo_config, collection_name, delete_query):
        """
            Delete documents from the MongoDB collection based on the deletion query
            :param mongo_config: MongoDB configuration -> object of class MongoConfig
            :param collection_name: Name of the collection to delete documents from
            :param delete_query: The query used to filter and delete documents.
        """
        mongo_obj = MongoSDK(mongo_config)
        mongo_obj.delete_document(collection_name, delete_query)

    def perform_random_update(self, mongo_config, collection_name):
        """
            Perform a random update operation on a document in the MongoDB collection.

            Parameters:
            - mongo_config : Object of class SDKs.MongoDB.MongoConfig
            - collection_name (str): The name of the MongoDB collection to perform the update on.
        """
        mongo_obj = MongoSDK(mongo_config)
        random_doc = mongo_obj.get_random_doc(collection_name)
        if random_doc:
            updated_doc = self.generate_docs()
            updated_doc["_id"] = random_doc["_id"]

            mongo_obj.update_document(collection_name, {"_id": updated_doc["_id"]}, updated_doc)

    def delete_random_doc(self, mongo_config, collection_name):
        """
            Delete a random document from the MongoDB collection.

            Parameters:
            - mongo_config : Object of class SDKs.MongoDB.MongoConfig
            - collection_name (str): The name of the MongoDB collection to perform the deletion on.
        """
        mongo_obj = MongoSDK(mongo_config)
        total_documents = mongo_obj.get_current_doc_count(collection_name)
        if total_documents == 0:
            return

        random_document = mongo_obj.get_random_doc(collection_name)
        mongo_obj.delete_document(collection_name, {"_id": random_document["_id"]})

    def setup_initial_load_on_mongo(self, mongo_config, collection_name, initial_doc_count):
        if not isinstance(mongo_config, MongoConfig):
            raise ValueError("config parameter must be an instance of MongoConfig class")

        mongo_object = MongoSDK(mongo_config)

        if initial_doc_count:
            current_docs = mongo_object.get_current_doc_count(collection_name)
            while current_docs < initial_doc_count:
                batch_size = self.calculate_optimal_batch_size(initial_doc_count, current_docs, 10000)
                self.load_doc_to_mongo(mongo_config, collection_name, initial_doc_count - current_docs,
                                       batch_size)
                current_docs = mongo_object.get_current_doc_count(collection_name)

            while current_docs > initial_doc_count:
                self.delete_random_doc(mongo_object, collection_name)
                current_docs = mongo_object.get_current_doc_count(collection_name)

    def perform_crud_on_mongo(self, mongo_config, collection_name, num_buffer=0):
        """
            Perform CRUD operations on a MongoDB collection.

            Parameters:
            - mongo_config : Object of class SDKs.MongoDB.MongoConfig
            - collection_name (str): The name of the MongoDB collection to perform CRUD operations on.
        """
        if not isinstance(mongo_config, MongoConfig):
            raise ValueError("config parameter must be an instance of MongoConfig class")

        mongo_object = MongoSDK(mongo_config)
        start_docs = mongo_object.get_current_doc_count(collection_name)
        if num_buffer == 0:
            max_files = float('inf')
            min_files = 0
        else:
            max_files = start_docs + num_buffer
            min_files = max(int(start_docs - num_buffer), 0)
        while True:
            while not self.stop_mongo_loader:
                current_docs = mongo_object.get_current_doc_count(collection_name)
                operation = random.choice(["update", "insert", "delete"])
                if operation == "update":
                    self.perform_random_update(mongo_config, collection_name)
                elif operation == "insert" and current_docs < max_files:
                    mongo_object.insert_single_document(collection_name, self.generate_docs())
                elif operation == "delete" and current_docs > min_files:
                    self.delete_random_doc(mongo_config, collection_name)

    def rebalance_mongo_docs(self, mongo_config, collection_name, num_docs):
        mongo_object = MongoSDK(mongo_config)
        current_docs = mongo_object.get_current_doc_count(collection_name)
        while current_docs < num_docs:
            batch_size = self.calculate_optimal_batch_size(num_docs, current_docs, 10000)
            self.load_doc_to_mongo(mongo_config, collection_name, num_docs - current_docs, batch_size)
            current_docs = mongo_object.get_current_doc_count(collection_name)
        while current_docs > num_docs:
            self.delete_random_doc(mongo_object, collection_name)
            current_docs = mongo_object.get_current_doc_count(collection_name)

    # -- S3 --
    def generate_random_folder_path(self, num_folders, depth_lvl):
        """
        Generate a random folder path based on the specified number of folders and depth level.

        Parameters:
        - num_folders (int): Number of folders per level.
        - depth_lvl (int): Depth level.

        Returns:
        - str: Random folder path.
        """
        folder_path = ""
        for level in range(depth_lvl + 1):
            folder_path += f'Depth_{level}_Folder_{random.randint(0, num_folders - 1)}/'
        return folder_path

    def create_s3_using_specified_config(self, s3_config, skip_bucket_creation=False, bucket=[]):
        """
               Create an S3 client using the specified configuration.

               Parameters:
               - s3_config (dict): An object of SDK.s3.s3_config
        """
        if not isinstance(s3_config, s3Config):
            raise ValueError("config parameter must be an instance of s3Config class")

        print("Creating the required config. Please wait...")
        buckets = []
        if bucket:
            buckets = bucket
        s3 = s3SDK(s3_config.access_key, s3_config.secret_key)
        if not skip_bucket_creation:
            random_string = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))

            print(f"######## STEP 1/2  : CREATING {s3_config.num_buckets} BUCKETS ########")
            for i in range(s3_config.num_buckets):
                bucket, error = s3.create_bucket(f"goldfishxx{random_string}xx{i}{i}{i}", s3_config.region)
                if bucket:
                    buckets.append(f"goldfishxx{random_string}xx{i}{i}{i}")
                else:
                    return error

            print(f"######## STEP 1/2 COMPLETE : CREATED BUCKETS {buckets} ########")
        for bucket in buckets:
            print(f"######## STEP 2/2  : CREATE REQUIRED FILE STRUCTURE ########")
            s3.generate_and_upload_structure(s3_config, bucket, file_types=s3_config.file_format)

        print(f"######## STEP 2/2  COMPLETE: CREATED REQUIRED FILE STRUCTURE ########")
        return buckets

    def perform_crud_on_s3(self, config, buckets, max_files, min_files):
        """
        Perform CRUD operations on S3 for a specified duration.

        Parameters:
        - config (s3Config): An object of SDK.s3.s3_config.
        - buckets (list): A list of S3 bucket names.
        - duration_minutes (int): Duration for CRUD operations in minutes.
        - max_insertions (int): Maximum number of insertions allowed.
        - max_deletions (int): Maximum number of deletions allowed.
        """
        if not isinstance(config, s3Config):
            raise ValueError("config parameter must be an instance of s3Config class")

        s3 = s3SDK(config.access_key, config.secret_key)
        s3_config = s3Operations()

        with ThreadPoolExecutor(max_workers=len(buckets)) as executor:
            futures = []
            for bucket in buckets:
                futures.append(
                    executor.submit(self.crud_for_s3_bucket, config, s3, s3_config, bucket, max_files, min_files))

            # Wait for all tasks to complete
            for future in futures:
                future.result()

        print("All CRUD operations completed.")

    def crud_for_s3_bucket(self, config, s3, s3_config, bucket, max_files, min_files):
        self.print_s3_bucket_structure(s3, bucket)
        print_once = True
        while True:
            while not self.stop_s3_loader:
                print_once = True
                # Generate a random depth and folder path
                depth_lvl = random.randint(0, config.depth_level - 1)
                folder_path = self.generate_random_folder_path(config.num_folders_per_level, depth_lvl)

                # Check the total number of files in the folder
                existing_files = s3.list_files_in_folder(bucket, folder_path)

                # Randomly choose insert or delete operation
                operation = random.choice(["insert", "delete"])
                # Perform insert operation if there are fewer files than the target and insert operation is chosen
                if len(existing_files) < max_files and operation == "insert":
                    file_name = f"{random.randint(0, 100)}.{random.choice(config.file_format)}"
                    file_content = s3_config.create_file_with_required_file_type(random.choice(config.file_format),
                                                                                 config.file_size)
                    s3_object_key = os.path.join(folder_path, file_name)
                    print(f"inserting file {file_name} on path {s3_object_key}")
                    s3.upload_file_with_content(bucket, s3_object_key, file_content)
                # Perform delete operation if there are more files than the target and delete operation is chosen
                elif len(existing_files) > min_files and operation == "delete":
                    # Choose a random file from existing_files
                    file_path_in_folder = random.choice(existing_files)

                    # Extract the file name from the full path
                    file_name = os.path.basename(file_path_in_folder)

                    # Construct the S3 object key
                    s3_object_key = os.path.join(folder_path, file_name)

                    print(f"deleting file {file_name} on path {s3_object_key}")
                    s3.delete_file(bucket, s3_object_key)
            if print_once and not self.stop_s3_loader:
                self.print_s3_bucket_structure(s3, bucket)
                print_once = False

    def restore_s3(self, s3, bucket, config):
        s3.empty_bucket(bucket)
        self.create_s3_using_specified_config(config, skip_bucket_creation=True, bucket=[bucket])

    def print_s3_bucket_structure(self, s3, bucket):
        s3.print_bucket_structure(bucket)

    # -- MYSQL --
    def load_data_to_mysql(self, mysql_obj, table_name, table_columns, doc_count, record_values=None):
        for _ in range(doc_count):
            if not record_values:
                doc = self.generate_docs()
                record_values = [
                    doc["address"],
                    doc["avg_ratings"],
                    doc["city"],
                    doc["country"],
                    doc["email"],
                    doc["free_breakfast"],
                    doc["free_parking"],
                    doc["name"],
                    doc["phone"],
                    doc["price"],
                    json.dumps(doc["public_likes"]),
                    json.dumps(doc["reviews"]),
                    doc["type"],
                    doc["url"],
                ]
            table_columns_without_id = [col.split()[0] for col in table_columns.split(", ") if
                                        "AUTO_INCREMENT" not in col]
            mysql_obj.insert_record_using_columns(table_name, table_columns_without_id, record_values)

    def setup_inital_load_on_mysql(self, mysql_obj, table_name, table_columns, initial_doc_count):
        current_doc_count = mysql_obj.get_total_records_count(table_name)
        if current_doc_count < initial_doc_count:
            self.load_data_to_mysql(mysql_obj, table_name, table_columns, int(initial_doc_count-current_doc_count))
        else:
            for _ in range(initial_doc_count - current_doc_count):
                record_id = mysql_obj.get_random_record_id(table_name)
                if record_id is not None:
                    try:
                        mysql_obj.delete_record(table_name, f"id={record_id}")
                    except Exception as e:
                        print(f"Error during delete operation: {e}")

    def perform_crud_on_mysql(self, mysql_obj, table_name, table_columns, num_buffer=0):

        start_docs = mysql_obj.get_total_records_count(table_name)
        if num_buffer == 0:
            max_files = float('inf')
            min_files = 0
        else:
            max_files = start_docs + num_buffer
            min_files = max(int(start_docs - num_buffer), 0)

        while True:
            while not self.stop_mysql_loader:
                operation = random.choice(["create", "update", "delete"])
                current_records_count = mysql_obj.get_total_records_count(table_name)

                if operation == "create" and max_files > current_records_count:
                    doc = self.generate_docs()

                    table_columns_without_id = [col.split()[0] for col in table_columns.split(", ") if
                                                "AUTO_INCREMENT" not in col]
                    record_values = [
                        doc["address"],
                        doc["avg_ratings"],
                        doc["city"],
                        doc["country"],
                        doc["email"],
                        doc["free_breakfast"],
                        doc["free_parking"],
                        doc["name"],
                        doc["phone"],
                        doc["price"],
                        json.dumps(doc["public_likes"]),
                        json.dumps(doc["reviews"]),
                        doc["type"],
                        doc["url"],
                    ]
                    mysql_obj.insert_record_using_columns(table_name, table_columns_without_id, record_values)

                elif operation == "update":
                    record_id = mysql_obj.get_random_record_id(table_name)
                    if record_id is not None:
                        doc = self.generate_docs()

                        public_likes_json = json.dumps(doc["public_likes"])
                        reviews_json = json.dumps(doc["reviews"])

                        update_values = {
                            "address": doc["address"],
                            "avg_rating": doc["avg_ratings"],
                            "city": doc["city"],
                            "country": doc["country"],
                            "email": doc["email"],
                            "free_breakfast": doc["free_breakfast"],
                            "free_parking": doc["free_parking"],
                            "name": doc["name"],
                            "phone": doc["phone"],
                            "price": doc["price"],
                            "public_likes": public_likes_json,
                            "reviews": reviews_json,
                            "type": doc["type"],
                            "url": doc["url"],
                        }
                        update_query = f"UPDATE {table_name} SET " + ", ".join(
                            [f"{column} = %s" for column in update_values.keys()]) + f" WHERE id = {record_id}"

                        update_values_list = tuple(update_values.values())

                        try:
                            mysql_obj.update_using_given_query_and_value(update_query, update_values_list)
                            print(f"Record with ID {record_id} updated successfully.")
                        except Exception as e:
                            print(f"Error during update operation: {e}")
                    else:
                        print("No records found in the table.")

                elif operation == "delete" and min_files < current_records_count:
                    record_id = mysql_obj.get_random_record_id(table_name)

                    if record_id is not None:
                        try:
                            mysql_obj.delete_record(table_name, f"id={record_id}")
                        except Exception as e:
                            print(f"Error during delete operation: {e}")

    def rebalance_mysql_docs(self, doc_count, table_name, table_columns, mysql_obj=None, config=None,
                             database_name=None, record_values=None):
        if not mysql_obj:
            if config is None or database_name is None:
                raise Exception("MySQL config and Database name is required")
            else:
                mysql_obj = MySQLSDK(config)
        mysql_obj.use_database(database_name)
        current_records_count = mysql_obj.get_total_records_count(table_name)
        print(current_records_count)
        while current_records_count > doc_count and current_records_count >= 0:
            record_id = mysql_obj.get_random_record_id(table_name)
            if record_id is not None:
                try:
                    mysql_obj.delete_record(table_name, f"id={record_id}")
                    current_records_count -= 1
                except Exception as e:
                    print(f"Error during delete operation: {e}")

        while current_records_count < doc_count:
            doc = self.generate_docs()

            table_columns_without_id = [col.split()[0] for col in table_columns.split(", ") if
                                        "AUTO_INCREMENT" not in col]

            if not record_values:
                record_values = [
                    doc["address"],
                    doc["avg_ratings"],
                    doc["city"],
                    doc["country"],
                    doc["email"],
                    doc["free_breakfast"],
                    doc["free_parking"],
                    doc["name"],
                    doc["phone"],
                    doc["price"],
                    json.dumps(doc["public_likes"]),
                    json.dumps(doc["reviews"]),
                    doc["type"],
                    doc["url"],
                ]
            mysql_obj.insert_record_using_columns(table_name, table_columns_without_id, record_values)
            current_records_count += 1
