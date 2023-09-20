from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import uuid


class CassandraSDK:
    """
    Cassandra sdk to handle cassandra entities
    """
    def __init__(self, contact_points, keyspace, table_name):
        self.cluster = Cluster(contact_points)
        self.session = self.cluster.connect(keyspace)
        self.table_name = table_name

    def create_keyspace(self, keyspace_name, replication_strategy, replication_factor):
        """
        Creates a keyspace for the cassandra database
        Args:
            keyspace_name:
            replication_strategy:
            replication_factor:
        """
        create_keyspace_query = (
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace_name} "
            f"WITH REPLICATION = {{ 'class': '{replication_strategy}', 'replication_factor': {replication_factor} }}"
        )
        self.session.execute(create_keyspace_query)

    def use_keyspace(self, keyspace_name):
        """
        Set keyspace for the cassandra database
        Args:
            keyspace_name:
        """
        self.session.set_keyspace(keyspace_name)

    def create_table(self, table_name, schema):
        """
        Create table in the cassandra database
        Args:
            table_name:
            schema:
        """
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} {schema}"
        self.session.execute(create_table_query)

    def insert_data(self, table_name, data):
        """
        Insert data in a table in the database
        Args:
            table_name:
            data:
        """
        insert_query = f"INSERT INTO {table_name} {data}"
        self.session.execute(insert_query)

    def execute_query(self, query):
        """
        Runs query on cassandra. This will be used to delete and update
        data in cassandra
        Args:
            query:

        Returns:

        """
        return self.session.execute(query)

    def close_connections(self):
        """
        Close the cassandra session and the cassandra cluster
        """
        self.session.shutdown()
        self.cluster.shutdown()

    def insert_into_batch(self, items):
        """
        Write data in batches in cassandra
        Args:
            items:
        """
        batch = BatchStatement()
        for data in items:
            insert_query_template = f"INSERT INTO {self.table_name} " \
                                    f"({', '.join(items[0].keys())}) VALUES " \
                                    f"({', '.join(['?'] * len(items[0]))})"
            insert_query = self.session.prepare(insert_query_template)
            # Add the insert queries to the batch
            batch.add(insert_query, tuple(data.values()))
        # Execute the batch insert
        self.session.execute(batch)
