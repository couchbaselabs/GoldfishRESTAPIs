# Docloading Server

The Docloading server is a Flask application designed to manage and control data loading processes, such as loading data into MongoDB, S3, or MySQL databases. This server allows users to start, stop, and monitor loader processes through a RESTful API.

It is purpose-built for efficiently loading documents into database. The "Start Loader" initiates CRUD operations, while the "Stop Loader" gracefully halts the ongoing CRUD processes, providing seamless control over data loading into MongoDB collections.

## Prerequisites

Before running the server, make sure you have the following prerequisites installed:

Install the dependencies using:

```
pip3 install -r requirements.txt
```
-----

You can get the postman collection by importing
```
https://api.postman.com/collections/23120857-39d26b69-12db-423d-8484-73de6a03146a?access_key=PMAT-01HCKYQ5RSBN16PPGXBDGKF07A
```

## Getting Started

1. Clone the repository:
```
git clone https://github.com/couchbaselabs/GoldfishRESTAPIs.git
```
2. Navigate to the project directory:
```
cd Server
```
3. Run the Flask application:
```
python3 dataloading_server.py
```

The server should start running on http://127.0.0.1:5000/
-----

## API Endpoints

### MongoDB Loader

  1. Start MongoDB Loader

      + Endpoint: /mongo/start_loader
      + Method: POST
      + Request Body:
        ```
          {
            "ip": "MongoDB_IP",
            "port": "MongoDB_Port",
            "username": "MongoDB_Username",
            "password": "MongoDB_Password",
            "database_name": "Database_Name, This would create the database if it doesn't exists",
            "collection_name": "Collection_Name, This would create the collection if it doesn't exists",
            "target_num_docs": "num_docs arount which you want the CRUD to run. It will load/delete docs till this value is attained and then start crud"
          }
        ```
      + Optional Parameters
           ```
             {
               "atlas_url": "This will override all the request body parameters and start running loader on mongodb atlas"
               "loader_id": "This can be used to restart a loader",
               "time_for_crud_in_mins": "If you want the crud to run for some time you can add this to your body. By default loader runs infinitely."
               "num_buffer": "Buffer number of documents. By default it is 500. It won't let your num_docs go below num_docs-num_buffer nor go above num_docs+num_buffer.
             }
           ```   
      + Response: JSON with loader information
           ```
             {
               "response": f"Loader {loader_id} started successfully",
               "loader_id": "loader_id",
               "database": "database_name",
               "collection": "collection_name",
               "status": "running/ failed"
             }
            ```
   2. Stop MongoDB Loader

      + Endpoint: /mongo/stop_loader
      + Method: POST
      + Request Body:
         ```
            {
              "loader_id": "Loader_ID"
            }
         ```
      + Response: JSON with loader status
         ```
            {
                "collection": "collection_name",
                "database": "database_name",
                "loader_id": "loader_id",
                "response": "Loader loader_id stopped successfully",
                "status": "stopped"
            }
         ```
   3. Count Documents in MongoDB Collection
        + Endpoint: /mongo/count
        + Method: GET
        + Request Body:
          ```
            {
              "ip": "MongoDB_IP",
              "port": "MongoDB_Port",
              "username": "MongoDB_Username",
              "password": "MongoDB_Password",
              "database_name": "Database_Name, This would create the database if it doesn't exists",
              "collection_name": "Collection_Name, This would create the collection if it doesn't exists"
            }
          ```
         + Response: JSON with count status
             ```
                {
                    "count": X
                }
             ```
   4. Delete MongoDB Database
       + Endpoint: /mongo/delete_database
       + Method: DELETE
       + Request Body:
          ```
            {
              "ip": "MongoDB_IP",
              "port": "MongoDB_Port",
              "username": "MongoDB_Username",
              "password": "MongoDB_Password",
              "database_name": "Database_Name, This would create the database if it doesn't exists",
              "collection_name": "Collection_Name, This would create the collection if it doesn't exists"
            }
          ```
       + Response: JSON with loader status
           ```
              {
                  "response": "SUCCESS" 
              }
           ```     
  5. Delete MongoDB Collection
       + Endpoint: /mongo/delete_collection
       + Method: DELETE
       + Request Body:
          ```
            {
              "ip": "MongoDB_IP",
              "port": "MongoDB_Port",
              "username": "MongoDB_Username",
              "password": "MongoDB_Password",
              "database_name": "Database_Name, This would create the database if it doesn't exists",
              "collection_name": "Collection_Name, This would create the collection if it doesn't exists"
            }
          ```
       + Response: JSON with loader status
           ```
              {
                  "response": "SUCCESS" 
              }
           ```

---

### S3 Loader
   1. Start S3 Loader
      + Endpoint: /s3/start_loader
      + Method: POST
      + Request Body:
         ```
            {
              "access_key": "Your_AWS_Access_Key",
              "secret_key": "Your_AWS_Secret_Key",
              "region": "AWS_Region",
              "num_buckets": 5,
              "depth_level": 3,
              "num_folders_per_level": 2,
              "num_files_per_level": 10
            }
         ```
      + Optional Parameters
        ```
          {
            "session_token": "This can be used for temp aws credentials",
            "file_size": "The size of document"
            "file_format": "Format of file you wish to create. Currently supported are ['json', 'csv', 'tsv', 'avro', 'parquet']"
          }
        ```   
      + Response: JSON with loader information
        ```
           {
                "bucket": [
                    "bucket_name1","bucket_name2"
                ],
                "loader_id": "loader_id",
                "response": "Loader "loader_id" restarted successfully",
                "status": "running/ failed"
            }
        ```
   2. Stop S3 Loader

      + Endpoint: /s3/stop_loader
      + Method: POST
      + Request Body:
         ```
            {
              "loader_id": "Loader_ID"
            }
         ```
      + Response: JSON with loader status
        ```
           {
                "bucket": [
                    "bucket_name1","bucket_name2"
                ],
                "loader_id": "loader_id",
                "response": "Loader "loader_id" stopped successfully",
                "status": "stopped"
            }
        ```
  3. Delete S3 Bucket
      + Endpoint: /s3/stop_loader
      + Method: DELETE
      + Request Body:
         ```
            "access_key": "Your_AWS_Access_Key",
            "secret_key": "Your_AWS_Secret_Key",
            "bucket_name": "bucket_name"
         ```
      + Response: JSON with loader status
        ```
           {
              "response": "SUCCESS"
           }
        ```
  4. Restore S3 to some config
      + Endpoint: /s3/restore
      + Method: POST
      + Request Body:
        ```
            {
              "host": "MySQL_Host",
              "port": "MySQL_Port",
              "username": "MySQL_Username",
              "password": "MySQL_Password",
              "database_name": "Database_Name",
              "table_name": "Table_Name",
              "table_columns": "coloumn details, Ex: id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), avg_rating FLOAT, city VARCHAR(255), country VARCHAR(255)",
              "bucket_name" : bucket name
            }
         ```
      + Response: JSON with loader status
        ```
           {
              "response": "SUCCESS"
           }
        ```

---

## MySQL Loaders
   1. Start MySQL Loader

      + Endpoint: /mysql/start_loader
      + Method: POST
      + Request Body:
         ```
            {
              "host": "MySQL_Host",
              "port": "MySQL_Port",
              "username": "MySQL_Username",
              "password": "MySQL_Password",
              "database_name": "Database_Name",
              "table_name": "Table_Name",
              "table_columns": "coloumn details, Ex: id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), avg_rating FLOAT, city VARCHAR(255), country VARCHAR(255)"
            }
         ```
       + Optional Parameters
           ```
             {
               "init_config": "If passed as true, it would create database and table from scratch, else would expect the db and table already exists."
             }
           ```  
      + Response: JSON with loader information
   
   2. Stop MySQL Loader

      + Endpoint: /mysql/stop_loader
      + Method: POST
      + Request Body:
         ```
            {
              "loader_id": "Loader_ID"
            }
         ```
      + Response: JSON with loader status

  3. Count Records in MySQL Table

      + Endpoint: /mysql/count
      + Method: GET
      + Request Body:
         ```
            {
              "host": "MySQL_Host",
              "port": "MySQL_Port",
              "username": "MySQL_Username",
              "password": "MySQL_Password",
              "database_name": "Database_Name",
              "table_name": "Table_Name"
            }
         ``` 
      + Response: JSON with count information
           ```
              {
                  "count": 50
              }
           ```
   4. Delete MySQL Database
      + Endpoint: /s3/delete_database
      + Method: DELETE
      + Request Body:
         ```
           {
              "host": "MySQL_Host",
              "port": "MySQL_Port",
              "username": "MySQL_Username",
              "password": "MySQL_Password",
              "database_name": "Database_Name"
           }
         ```
      + Response: JSON with loader status
        ```
           {
              "response": "SUCCESS"
           }
        ```
   5. Delete MySQL Table
      + Endpoint: /s3/delete_table
      + Method: DELETE
      + Request Body:
         ```
            {
              "host": "MySQL_Host",
              "port": "MySQL_Port",
              "username": "MySQL_Username",
              "password": "MySQL_Password",
              "database_name": "Database_Name",
              "table_name": "Table_Name"
            }
         ```
      + Response: JSON with loader status
        ```
           {
              "response": "SUCCESS"
           }
        ```
  6. Restore MySQL to some config
      + Endpoint: /mysql/restore
      + Method: POST
      + Request Body:
        ```
            {
              "host": "MySQL_Host",
              "port": "MySQL_Port",
              "username": "MySQL_Username",
              "password": "MySQL_Password",
              "database_name": "Database_Name",
              "table_name": "Table_Name",
              "table_columns": "coloumn details, Ex: id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, address VARCHAR(255), avg_rating FLOAT, city VARCHAR(255), country VARCHAR(255)"
              "doc_count": doc count to which you want to restore.
            }
         ```
      + Response: JSON with loader status
        ```
           {
              "response": "SUCCESS"
           }
        ```
---

## GENERIC LOADER ENDPOINTS
   1. Get All Loaders for all DBs

      + Endpoint: /loaders
      + Method: GET
      + Response: JSON array with information about all loaders
        ```
           [
              {
                    "collection": "collection_name / bucket_name",
                    "database": "database_bane / bucket_name",
                    "loader_id": "loader_id",
                    "status": "running/stopped"
              }
           ]
        ```

   2. Get Loader Information

      + Endpoint: /loaders/{loader_id}
      + Method: GET
      + Response: JSON with information about the specified loader
        ```
           {
              "collection": "collection_name / bucket_name",
              "database": "database_bane / bucket_name",
              "loader_id": "loader_id",
              "status": "running/stopped"
           }
        ```