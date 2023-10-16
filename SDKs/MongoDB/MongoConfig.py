class MongoConfig:
    """
        A configuration class for MongoDB connection settings.

        This class stores the configuration settings required to establish a connection to a MongoDB server.

        Parameters:
            mongo_ip (str): The IP address of the MongoDB server.
            port (int): The port number on which MongoDB is listening.
            username (str): The username for authentication (optional).
            password (str): The password for authentication (optional).
            database_name (str): The name of the MongoDB database to connect to.
            atlas_url (str) : If you want to run using MongoDB Atlas
    """

    def __init__(self, mongo_ip, port, username, password, database_name, atlas_url=None):
        """
        Initialize a new MongoConfig instance with the provided connection settings.
        """
        self.mongo_ip = mongo_ip
        self.port = port
        self.username = username
        self.password = password
        self.database_name = database_name
        self.atlas_url = atlas_url
