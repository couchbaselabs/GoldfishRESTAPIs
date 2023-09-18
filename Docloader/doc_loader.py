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
import Docloader.docgen_template as template
import SDKs.DynamoDB.dynamo_sdk as dynamoSdk
from SDKs.MongoDB.MongoSDK import MongoSDK


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

    def load_doc_to_dynamo(self, url, table, batch_size=1000, max_concurrent_batches=1000):
        """

        :param url: dynamoDB url
        :param table: dynamoDb table name
        :param batch_size:
        :param max_concurrent_batches:
        """
        start = time.time()
        dynamo_obj = dynamoSdk.DynamoDb(url, table)
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

    def delete_from_mongo(self, mongoConfig, collection_name, delete_query):
        """
            Delete documents from the MongoDB collection based on the deletion query
            :param mongoConfig: MongoDB configuration -> object of class MongoConfig
            :param collection_name: Name of the collection to delete documents from
            :param delete_query: The query used to filter and delete documents.
        """
        mongo_obj = MongoSDK(mongoConfig)
        mongo_obj.delete_document(collection_name, delete_query)

    def update_in_mongo(self, mongoConfig, collection_name, updateFrom, updateTo):
        """
            Update documents in the MongoDB collection based on the update query
            :param mongoConfig: MongoDB configuration -> object of class MongoConfig
            :param collection_name: Name of the collection to update documents in
            :param updateFrom: The query used to filter documents to be updated
            :param updateTo: The update operation to apply to matching documents.
        """
        mongo_obj = MongoSDK(mongoConfig)
        mongo_obj.update_document(collection_name, updateFrom, updateTo)
