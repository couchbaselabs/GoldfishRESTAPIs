import logging
import mysql.connector
from mysql.connector import Error

from SDKs.MySQL.MySQL_config import MySQLConfig


class MySQLSDK:
    def __init__(self, config):
        if not isinstance(config, MySQLConfig):
            raise ValueError("config parameter must be an instance of MySQLConfig class")

        self.db_config = {
            'host': config.host,
            'port': config.port,
            'user': config.username,
            'password': config.password
        }
        self.connection = None
        self.cursor = None
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
        self.create_connection()

    def create_connection(self):
        try:
            self.connection = mysql.connector.connect(**self.db_config)

            if self.connection.is_connected():
                print(f"Connected to MySQL Server version {self.connection.get_server_info()}")
                self.cursor = self.connection.cursor()
        except Error as e:
            raise Exception(f"Error: {e}")

    def close_connection(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("Connection closed")

    def use_database(self, db_name):
        use_database_query = f"USE {db_name}"
        self.cursor.execute(use_database_query)

    def create_database(self, db_name):
        try:
            create_database_query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
            self.log.info(f"Executing query : {create_database_query}")
            self.cursor.execute(create_database_query)
        except mysql.connector.Error as err:
            self.log.error(f"Error during database creation: {err}")
            raise Exception(err)

    def create_table(self, table_name, columns):
        try:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
            self.log.info(f"Executing query : {create_table_query}")
            self.cursor.execute(create_table_query)
        except mysql.connector.Error as err:
            self.log.error(f"Error during table creation: {err}")
            raise Exception(err)

    def delete_database(self, db_name):
        try:
            delete_database_query = f"DROP DATABASE IF EXISTS {db_name}"
            self.log.info(f"Executing query : {delete_database_query}")
            self.cursor.execute(delete_database_query)
        except mysql.connector.Error as err:
            self.log.error(f"Error during database deletion: {err}")
            raise Exception(err)

    def delete_table(self, table_name):
        try:
            delete_table_query = f"DROP TABLE IF EXISTS {table_name}"
            self.log.info(f"Executing query : {delete_table_query}")
            self.cursor.execute(delete_table_query)
        except mysql.connector.Error as err:
            self.log.error(f"Error during table deletion: {err}")
            raise Exception(err)

    def insert_record(self, table_name, values):
        try:
            # values should be a tuple with the values for each column, e.g., ("John", 25)
            insert_query = f"INSERT INTO {table_name} VALUES {values}"
            self.log.info(f"Executing query : {insert_query}")
            self.cursor.execute(insert_query)
            self.connection.commit()
            self.log.info(f"Inserted record successfully")
        except mysql.connector.Error as err:
            self.connection.rollback()  # Rollback the transaction in case of an error
            self.log.error(f"Failed to Insert Record - Error during insert operation: {err}")
            raise Exception(err)

    def insert_record_using_columns(self, table_name, columns, values):
        try:
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(values))})"
            self.log.info(f"Executing query : {query} values = {values}")
            self.cursor.execute(query, values)
            self.connection.commit()
            print("Record inserted successfully.")
        except Exception as e:
            # Handle exceptions
            print(f"Error during insert operation: {e}")
            self.connection.rollback()
            raise Exception(err)

    def update_record(self, table_name, set_values, condition):
        try:
            # set_values should be a string defining the values to set, e.g., "name='John'"
            # condition should be a string defining the condition for the update, e.g., "id=1"
            update_query = f"UPDATE {table_name} SET {set_values} WHERE {condition}"
            self.log.info(f"Executing query : {update_query}")
            self.cursor.execute(update_query)
            self.connection.commit()
            self.log.info(f"Updated record successfully")
        except mysql.connector.Error as err:
            self.connection.rollback()  # Rollback the transaction in case of an error
            self.log.error(f"Error during update operation: {err}")
            raise Exception(err)

    def update_using_given_query_and_value(self, update_query, update_values):
        try:
            self.log.info(f"Executing query : {update_query} \n values = {update_values}")
            self.cursor.execute(update_query, update_values)
            self.connection.commit()
            self.log.info(f"Updated record successfully")
        except Exception as err:
            self.connection.rollback()
            self.log.error(f"Error during update operation: {err}")
            raise Exception(err)

    def delete_record(self, table_name, condition):
        try:
            # condition should be a string defining the condition for the delete, e.g., "id=1"
            delete_query = f"DELETE FROM {table_name} WHERE {condition}"
            self.log.info(f"Executing query : {delete_query}")
            self.cursor.execute(delete_query)
            self.connection.commit()
            self.log.info(f"Deleted record successfully")
        except mysql.connector.Error as err:
            self.connection.rollback()  # Rollback the transaction in case of an error
            self.log.error(f"Error during delete operation: {err}")
            raise Exception(err)

    def get_random_record_id(self, table_name):
        try:
            # Assuming 'id' is the primary key column
            query = f"SELECT id FROM {table_name} ORDER BY RAND() LIMIT 1;"
            # Execute the query
            self.cursor.execute(query)

            # Fetch the result set
            result = self.cursor.fetchone()
            if result:
                return result[0]  # Assuming the first column is the ID
            else:
                return None

        except Exception as e:
            print(f"Error during get_random_record_id operation: {e}")
            return None

    def get_total_records_count(self, table_name):
        try:
            query = f"SELECT COUNT(*) FROM {table_name}"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            return result[0] if result else 0
        except Exception as e:
            print(f"Error getting total records count: {e}")
            return 0
