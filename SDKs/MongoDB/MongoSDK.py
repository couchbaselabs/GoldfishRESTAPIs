import logging
from pymongo import MongoClient

from .MongoConfig import MongoConfig


class MongoSDK(MongoConfig):
    """
     A MongoDB SDK class for performing database operations.

     This class provides methods for connecting to a MongoDB server, inserting, deleting, and updating documents.

     Parameters:
         config : A MongoConfig object containing MongoDB connection parameters.

     Attributes:
         mongo_ip (str): The IP address of the MongoDB server.
         port (int): The port number on which MongoDB is listening.
         username (str): The username for authentication (optional).
         password (str): The password for authentication (optional).
         database_name (str): The name of the MongoDB database to connect to.
     """

    def __init__(self, config):
        """
             Initialize a new MongoSDK instance with the provided connection settings.
        """
        self.mongo_ip = config.mongo_ip
        self.port = config.port
        self.username = config.username
        self.password = config.password
        self.database_name = config.database_name

        super().__init__(self.mongo_ip, self.port,
                         self.username, self.password, self.database_name)

        if config.atlas_url:
            self.client = MongoClient(config.atlas_url, tlsAllowInvalidCertificates=True)
        else:
            # create a mongoDB client
            if self.username and self.password:
                self.client = MongoClient(
                    f"mongodb://{self.username}:{self.password}@{self.mongo_ip}:{self.port}/{self.database_name}")
            else:
                self.client = MongoClient(f"mongodb://{self.mongo_ip}:{self.port}")

        self.db = self.client[self.database_name]

        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.INFO)
        if not self.log.handlers:
            # Create a handler to control where the log messages go (e.g., console or file)
            handler = logging.StreamHandler()  # Log to console
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.log.addHandler(handler)
            self.log.propagate = False

    def close_mongoDB_connection(self):
        """
            Close the MongoDB connection
        """
        self.client.close()

    def drop_database(self, database_name):
        self.client.drop_database(database_name)

    def drop_collection(self, database_name, collection_name):
        self.client[database_name][collection_name].drop()

    def insert_single_document(self, collection_name, data):
        """
            Insert a single document into the specified collection.

            Args:
                collection_name (str): The name of the collection to insert the document into.
                data (dict): The document data to insert.

            Returns:
                pymongo.results.InsertOneResult: The result of the insertion operation.
        """
        collection = self.db[collection_name]
        insert = collection.insert_one(data)
        self.log.info("Insertion result : Inserted data successfully")
        return insert

    def insert_multiple_document(self, collection_name, data_to_insert):
        """
            Insert multiple documents into the specified collection.

            Args:
                collection_name (str): The name of the collection to insert the documents into.
                data_to_insert (list): A list of document data to insert.

        """
        collection = self.db[collection_name]
        result = collection.insert_many(data_to_insert)
        self.log.info(
            f"Successfully inserted {len(result.inserted_ids)} documents.")

    def delete_document(self, collection_name, query):
        """
            Delete documents from the specified collection based on a query.

            Args:
                collection_name (str): The name of the collection to delete documents from.
                query (dict): The query to filter the documents to delete.
        """
        collection = self.db[collection_name]
        delete_result = collection.delete_many(query)
        self.log.info(
            f"Deletion result: {delete_result.deleted_count} documents deleted")

    def update_document(self, collection_name, query, update_data):
        """
            Update documents in the specified collection based on a query.

            Args:
                collection_name (str): The name of the collection to update documents in.
                query (dict): The query to filter the documents to update.
                update_data (dict): The data to update in the matching documents.
        """
        collection = self.db[collection_name]
        update_result = collection.update_many(query, {"$set": update_data})
        self.log.info(
            f"Update result: {update_result.modified_count} documents updated")

    def get_current_doc_count(self, collection_name):
        collection = self.db[collection_name]
        return collection.count_documents({})

    def get_random_doc(self, collection_name):
        collection = self.db[collection_name]
        return collection.find_one({})
