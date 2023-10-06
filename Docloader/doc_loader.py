"""
Docloader to create and upload document to various sources for goldFish
Sources include mongoDB, dynamoDB, cassandra, etc..
"""
import json
import concurrent
import concurrent.futures
import time
import logging
import faker
import random
import string
from concurrent.futures import ThreadPoolExecutor
import Docloader.docgen_template as template
import SDKs.DynamoDB.dynamo_sdk as dynamoSdk
from SDKs.MongoDB.MongoSDK import MongoSDK
from SDKs.MongoDB.MongoConfig import MongoConfig
from SDKs.s3.s3_SDK import s3SDK
from SDKs.s3.s3_config import s3Config
from SDKs.s3.s3_operations import s3Operations
import os


class DocLoader:
    """
    DocLoader class has the function to generate document based on size and number.
    This class can be used to upload docs to mango, cassandra and dynamodb
    :params:
    -document_size: Default is 1024
    -no_of_docs: Default is 100
    """

    def __init__(self, document_size=1024, no_of_docs=100, stop_loader=False):
        self.document_size = document_size
        self.no_of_docs = no_of_docs
        self.index = 0
        self.stop_loader = stop_loader

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

    def load_doc_to_dynamo(self, url=None, table=None, region_name=None, batch_size=1000, max_concurrent_batches=1000):
        """

        :param url: dynamoDB url
        :param table: dynamoDb table name
        :param batch_size:
        :param max_concurrent_batches:
        :param region_name: Region in which dynamodb table is deployed
        Either region or url is required, table name is required if table already exist
        """
        start = time.time()
        dynamo_obj = dynamoSdk.DynamoDb(endpoint_url=url, table=table, region=region_name)
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

    def delete_from_dynamodb(self, url=None, table=None, region_name=None, item_key=None, condition_expression=None,
                             expression_attribute_values=None, **params):
        """

        Args:
            url: Url of dynamodb
            table: table name of dynamodb
            region_name: region in which dynamodb is deployed
            item_key: item_key to be deleted
            condition_expression: condition to delete object
            expression_attribute_values: attribute values
            **params: extra parameters
        Either region or url is required, table name is required if table already exist
        """
        dynamo_obj = dynamoSdk.DynamoDb(endpoint_url=url, table=table, region=region_name)
        dynamo_obj.delete_item(item_key, condition_expression, expression_attribute_values, params)

    def update_in_dynamo(self, item_key, changed_object_json, url=None, table=None, region_name=None):
        """

        Args:
            item_key: item to change
            changed_object_json: keys to change
            url:
            table:
            region_name:
        Either region or url is required, table name is required if table already exist
        """
        dynamo_obj = dynamoSdk.DynamoDb(endpoint_url=url, table=table, region=region_name)
        dynamo_obj.update_item(item_key, changed_object_json)

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

    def perform_crud_on_mongo(self, mongo_config, collection_name, target_num_docs, time_for_crud_in_mins=None,
                              num_buffer=500):
        """
            Perform CRUD operations on a MongoDB collection.

            Parameters:
            - mongo_config : Object of class SDKs.MongoDB.MongoConfig
            - collection_name (str): The name of the MongoDB collection to perform CRUD operations on.
            - target_num_docs (int): The target number of documents to be inserted into the collection.
            - time_for_crud_in_mins (int): The total time (in minutes) allowed for performing CRUD operations.
            - num_buffer (int): The number of documents to buffer before inserting into the database. Default is 500.
        """

        if not isinstance(mongo_config, MongoConfig):
            raise ValueError("config parameter must be an instance of MongoConfig class")

        mongo_object = MongoSDK(mongo_config)
        time_for_crud = True
        if time_for_crud_in_mins is not None:
            start_time = time.time()
            time_for_crud = time.time() - start_time < time_for_crud_in_mins * 60

        while not self.stop_loader and time_for_crud:

            operation = random.choice(["update", "insert", "delete"])
            # Perform a random operation based on the selected type
            if operation == "update":
                self.perform_random_update(mongo_config, collection_name)
            elif operation == "insert":
                mongo_object.insert_single_document(collection_name, self.generate_docs())
            elif operation == "delete":
                self.delete_random_doc(mongo_config, collection_name)

            current_docs = mongo_object.get_current_doc_count(collection_name)

            if target_num_docs > num_buffer and current_docs < target_num_docs - num_buffer:
                while current_docs < target_num_docs:
                    batch_size = self.calculate_optimal_batch_size(target_num_docs, current_docs, 10000)
                    self.load_doc_to_mongo(mongo_config, collection_name, target_num_docs - current_docs, batch_size)
                    current_docs = mongo_object.get_current_doc_count(collection_name)

            elif current_docs > target_num_docs + num_buffer:
                while current_docs > target_num_docs:
                    self.delete_random_doc(mongo_object, collection_name)
                    current_docs = mongo_object.get_current_doc_count(collection_name)

        if not self.stop_loader:
            current_docs = mongo_object.get_current_doc_count(collection_name)
            while current_docs < target_num_docs:
                batch_size = self.calculate_optimal_batch_size(target_num_docs, current_docs, 10000)
                print(current_docs, target_num_docs, batch_size)
                self.load_doc_to_mongo(mongo_config, collection_name, target_num_docs - current_docs, batch_size)
                current_docs = mongo_object.get_current_doc_count(collection_name)
            while current_docs > target_num_docs:
                self.delete_random_doc(mongo_object, collection_name)
                current_docs = mongo_object.get_current_doc_count(collection_name)

    def is_loader_running(self):
        """
           Check if the loader is currently running.

           Returns:
               bool: True if the loader is running, False otherwise.
       """
        return not self.stop_loader

    def stop_running_loader(self):
        """
             Stop the currently running loader.

             This method sets the `stop_loader` flag to True, indicating the loader to stop its operation.
         """
        self.stop_loader = True

    def start_running_loader(self):
        """
            Start the loader.

            This method sets the `stop_loader` flag to False, allowing the loader to
            continue or start its operation.
        """
        self.stop_loader = False

    def create_s3_using_specified_config(self, s3_config, skip_bucket=False, bucket=[]):
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
        if not skip_bucket:
            random_string = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))

            print(f"######## STEP 1/2  : CREATING {s3_config.num_buckets} BUCKETS ########")
            for i in range(s3_config.num_buckets):
                bucket = s3.create_bucket(f"goldfishxx{random_string}xx{i}{i}{i}", s3_config.region)
                if bucket:
                    buckets.append(f"goldfishxx{random_string}xx{i}{i}{i}")

            print(f"######## STEP 1/2 COMPLETE : CREATED BUCKETS {buckets} ########")
        for bucket in buckets:
            print(f"######## STEP 2/2  : CREATE REQUIRED FILE STRUCTURE ########")
            s3.generate_and_upload_structure(s3_config, bucket, file_types=s3_config.file_format)

        print(f"######## STEP 2/2  COMPLETE: CREATED REQUIRED FILE STRUCTURE ########")
        return buckets

    def perform_crud_on_s3(self, config, buckets, duration_minutes, max_files, min_files):
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
                    executor.submit(self.crud_for_bucket, config, s3, s3_config, bucket, max_files, min_files,
                                    duration_minutes))

            # Wait for all tasks to complete
            for future in futures:
                future.result()

        print("All CRUD operations completed.")

    def crud_for_bucket(self, config, s3, s3_config, bucket, max_files, min_files, duration_minutes):
        self.print_s3_bucket_structure(s3, bucket)
        start_time = time.time()
        while time.time() - start_time < duration_minutes * 60:
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

        print("Crud complete")
        self.print_s3_bucket_structure(s3, bucket)

        self.rebalance_s3(s3, bucket, config)

        self.print_s3_bucket_structure(s3, bucket)

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

    def rebalance_s3(self, s3, bucket, config):
        s3.empty_bucket(bucket)
        self.create_s3_using_specified_config(config, skip_bucket=True, bucket=[bucket])

    def print_s3_bucket_structure(self, s3, bucket):
        s3.print_bucket_structure(bucket)
