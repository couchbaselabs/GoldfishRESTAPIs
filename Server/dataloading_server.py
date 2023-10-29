import boto3
import os
import threading
import uuid
from flask import Flask, request, jsonify

from Docloader.doc_loader import DocLoader
from SDKs.DynamoDB.dynamo_sdk import DynamoDb
from SDKs.MongoDB.MongoConfig import MongoConfig
from SDKs.MongoDB.MongoSDK import MongoSDK
from SDKs.MySQL.MySQL_config import MySQLConfig
from SDKs.MySQL.MySqlSDK import MySQLSDK
from SDKs.s3.s3_SDK import s3SDK
from SDKs.s3.s3_config import s3Config

app = Flask(__name__)

# These are the details of DB in which all loader Data is stored
loader_config = MongoConfig("172.23.105.114", 27017, "", "", "loaderDB")
loader_sdk = MongoSDK(loader_config)
loader_collection_name = str("loaderCollection")
loader_collection = loader_sdk.db[loader_collection_name]

loaderIdvsDocobject = {}


def check_request_body(params, checklist):
    for check in checklist:
        if check not in params:
            rv = {
                "List of required parameters": checklist,
                "Response": f"{check} is a required parameter"

            }
            return jsonify(rv), 422
    return "", 200


@app.route('/mongo/start_loader', methods=['POST'])
def start_mongo_loader():
    params = request.json
    checklist = ["ip", "port", "username", "password", "database_name", "collection_name",
                 "target_num_docs"]

    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        loaders = loader_collection.find({})

        # check if there is a loader already running on same db and collection
        for loader in loaders:
            if loader['database'] == params['database_name'] and loader['collection'] == params['collection_name'] and \
                    loader['status'] == "running":
                rv = {
                    "ERROR": f"There is already a loader running on {params['database_name']} and {params['collection_name']}. You can poll for the loader to be stopped",
                    "loader_id": loader['loader_id'],
                    "database": loader['database'],
                    "collection": loader['collection'],
                    "status": "failed"
                }
                return jsonify(rv), 409

        # Run a loader whose loader ID is known
        if "loader_id" in params:
            result = loader_collection.find_one({"loader_id": params['loader_id']})
            if not result:
                rv = {
                    "ERROR": f"No loader found for loader_id {params['loader_id']}",
                    "status": "failed"
                }
                return jsonify(rv), 409
            loader_id = params['loader_id']
            loader_obj = loaderIdvsDocobject[loader_id]
            loader_status = loader_obj.is_loader_running(db="mongo")
            if loader_status:
                rv = {
                    "response": f"Loader {loader_id} is already running",
                    "loader_id": loader_id,
                    "database": params['database_name'],
                    "collection": params['collection_name'],
                    "status": "failed"
                }
                return jsonify(rv), 200
            else:
                loader_obj.start_running_loader(db="mongo")
                rv = {
                    "response": f"Loader {loader_id} restarted successfully",
                    "loader_id": loader_id,
                    "database": params['database_name'],
                    "collection": params['collection_name'],
                    "status": "running"
                }
                loader_sdk.update_document(loader_collection_name, {"loader_id": loader_id},
                                           {"status": "running"})
                return jsonify(rv), 200
        else:
            # Start a new loader
            loader_id = str(uuid.uuid4())

            loader_data = {"loader_id": loader_id, "docloader": DocLoader(), "status": "running",
                           "database": params['database_name'], "collection": params['collection_name']}

            if params['atlas_url']:
                mongo_config = MongoConfig(params['ip'], params['port'], params['username'], params['password'],
                                           params['database_name'], params['atlas_url'])
            else:
                mongo_config = MongoConfig(params['ip'], params['port'], params['username'], params['password'],
                                           params['database_name'])
            thread1 = threading.Thread(target=loader_data['docloader'].perform_crud_on_mongo,
                                       args=(mongo_config, params['collection_name'], params['target_num_docs'],
                                             params.get('time_for_crud_in_mins', float('inf')),
                                             params.get('num_buffer', 500)))
            thread1.start()

            loaderIdvsDocobject[loader_id] = loader_data['docloader']

            # since threads would be deleted automatically so removing this field as of now.
            # We would need this docloader object to stop the loader (update the class field)
            del loader_data['docloader']

            loader_sdk.insert_single_document(loader_collection_name, loader_data)

            del loader_data['_id']
            return jsonify(loader_data), 200

    else:
        return params_check


@app.route('/mongo/stop_loader', methods=['POST'])
def stop_mongo_loader():
    params = request.json
    checklist = ["loader_id"]
    params_check = check_request_body(params, checklist)

    if params_check:
        loader_id = request.json.get("loader_id")

        loaders = loader_collection.find({})
        for loader in loaders:
            if loader_id == loader['loader_id']:
                if loader['status'] == "running":
                    loaderIdvsDocobject[loader_id].stop_running_loader(db="mongo")
                    loader_sdk.update_document(loader_collection_name, {"loader_id": loader['loader_id']},
                                               {"status": "stopped"})
                    loader['status'] = "stopped"
                    rv = {
                        "response": f"Loader {loader_id} stopped successfully",
                        "loader_id": loader_id,
                        "database": loader['database'],
                        "collection": loader['collection'],
                        "status": loader['status']
                    }
                    return jsonify(rv), 200
                else:
                    return jsonify({"response": f"Loader {loader_id} is not running"}), 200

        return jsonify({"response": f"No loader found with ID {loader_id}"}), 200
    else:
        return params_check


@app.route('/mongo/count', methods=['GET'])
def get_docs_in_mongo():
    params = request.json
    checklist = ["ip", "port", "username", "password", "database_name", "collection_name"]
    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        mongo_config = MongoConfig(params['ip'], params['port'], params['username'], params['password'],
                                   params['database_name'], params.get("atlas_url", None))
        mongo_sdk = MongoSDK(mongo_config)
        try:
            count = mongo_sdk.get_current_doc_count(params['collection_name'])
            rv = {
                "count": count
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check


@app.route('/mongo/delete_database', methods=['DELETE'])
def drop_mongo_database():
    params = request.json
    checklist = ["ip", "port", "username", "password", "database_name"]
    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        mongo_config = MongoConfig(params['ip'], params['port'], params['username'], params['password'],
                                   params['database_name'], params.get("atlas_url", None))
        mongo_sdk = MongoSDK(mongo_config)
        try:
            mongo_sdk.drop_database(params['database_name'])
            rv = {
                "response": "SUCCESS"
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "Error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check


@app.route('/mongo/delete_collection', methods=['DELETE'])
def drop_mongo_collection():
    params = request.json
    checklist = ["ip", "port", "username", "password", "database_name", "collection_name"]
    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        mongo_config = MongoConfig(params['ip'], params['port'], params['username'], params['password'],
                                   params['database_name'], params.get("atlas_url", None))
        mongo_sdk = MongoSDK(mongo_config)
        try:
            mongo_sdk.drop_collection(params['database_name'], params['collection_name'])
            rv = {
                "response": "SUCCESS"
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check


# -- Dynamo --`
def init_table(dynamo_object, table_name, primary_key):
    KeySchema = {f'{primary_key}': 'HASH', }  # Partition key
    AttributeDefinitions = {f'{primary_key}': 'S', }
    dynamo_object.create_table(table_name, KeySchema, AttributeDefinitions,
                               {'ReadCapacityUnits': 10000, 'WriteCapacityUnits': 10000})
    dynamo_object.enable_image_streaming()


def check_aws_credentials(access_key, secret_key, region, session_token=None):
    try:
        # Create an IAM client
        iam = boto3.client('iam', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region,
                           aws_session_token=session_token)

        # Call a simple IAM operation to check if the credentials are valid
        iam.get_user()

        return True, "AWS credentials are valid."

    except Exception as e:
        return False, f"An error occurred: {e}"


@app.route('/dynamo/start_loader', methods=['POST'])
def start_dynamo_loader():
    params = request.json
    checklist = ["access_key", "secret_key", "region", "primary_key_field", "table_name", "target_num_docs"]
    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        loaders = loader_collection.find({})
        # check if there is a loader already running on same db and collection
        for loader in loaders:
            if loader['database'] == params['table_name'] and loader['collection'] == params['table_name'] and \
                    loader['status'] == "running":
                rv = {
                    "ERROR": f"There is already a loader running on {params['table_name']}. You can poll for the loader to be stopped",
                    "loader_id": loader['loader_id'],
                    "database": loader['database'],
                    "status": "failed"
                }
                return jsonify(rv), 409

        try:
            dynamo_obj = DynamoDb(params.get('url', None), params['table_name'], params['region'])
            init_table(dynamo_obj, params['table_name'], params['primary_key_field'])
        except:
            pass

        if "loader_id" in params:
            loader_id = params['loader_id']
            result = loader_collection.find_one({"loader_id": params['loader_id']})
            if not result:
                rv = {
                    "ERROR": f"No loader found for loader_id {params['loader_id']}",
                    "status": "failed"
                }
                return jsonify(rv), 409
            loader_obj = loaderIdvsDocobject[loader_id]
            loader_status = loader_obj.is_loader_running(db="dynamo")

            if loader_status:
                rv = {
                    "response": f"Loader {loader_id} is already running",
                    "loader_id": loader_id,
                    "table": result['database'],
                    "status": "failed"
                }
                return jsonify(rv), 200
            else:
                loader_obj.start_running_loader(db="dynamo")
                rv = {
                    "response": f"Loader {loader_id} restarted successfully",
                    "loader_id": loader_id,
                    "table": result['database'],
                    "status": "running"
                }
                loader_sdk.update_document(loader_collection_name, {"loader_id": loader_id},
                                           {"status": "running"})
                return jsonify(rv), 200
        else:
            resp, err = check_aws_credentials(params['access_key'], params['secret_key'], params['region'])
            if not resp:
                rv = {
                    "ERROR": f"{resp}",
                    "status": "failed"
                }
                return jsonify(rv), 409
            loader_id = str(uuid.uuid4())

            loader_data = {"loader_id": loader_id, "docloader": DocLoader(no_of_docs=1), "status": "running",
                           "database": params['table_name'], "collection": params['table_name']}

            thread1 = threading.Thread(target=loader_data['docloader'].perform_crud_on_dynamodb,
                                       args=(params['access_key'], params['secret_key'], params['region'],
                                             params['primary_key_field'],
                                             int(params['target_num_docs']),
                                             params.get('session_token', None),
                                             params['table_name'],
                                             params.get('time_for_crud_in_mins', None), params.get('num_buffer', 500),
                                             params.get("initial_load", False)))
            thread1.start()

            loaderIdvsDocobject[loader_id] = loader_data['docloader']

            # since threads would be deleted automatically so removing this field as of now.
            # We would need this docloader object to stop the loader (update the class field)
            del loader_data['docloader']

            loader_sdk.insert_single_document(loader_collection_name, loader_data)

            del loader_data['_id']
            return jsonify(loader_data), 200
    else:
        return params_check


@app.route('/dynamo/stop_loader', methods=['POST'])
def stop_dynamo_loader():
    params = request.json
    checklist = ["loader_id"]
    params_check = check_request_body(params, checklist)

    if params_check:
        loader_id = request.json.get("loader_id")

        loaders = loader_collection.find({})
        for loader in loaders:
            if loader_id == loader['loader_id']:
                if loader['status'] == "running":
                    loaderIdvsDocobject[loader_id].stop_running_loader(db="dynamo")
                    loader_sdk.update_document(loader_collection_name, {"loader_id": loader['loader_id']},
                                               {"status": "stopped"})
                    loader['status'] = "stopped"
                    rv = {
                        "response": f"Loader {loader_id} stopped successfully",
                        "loader_id": loader_id,
                        "table": loader['database'],
                        "status": loader['status']
                    }
                    return jsonify(rv), 200
                else:
                    return jsonify({"response": f"Loader {loader_id} is not running"}), 200

        return jsonify({"response": f"No loader found with ID {loader_id}"}), 200
    else:
        return params_check


@app.route('/dynamo/count', methods=['GET'])
def get_docs_in_dynamo():
    params = request.json
    checklist = ["access_key", "secret_key", "region", "table_name", "region"]
    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        try:

            dynamo_object = DynamoDb(params['access_key'], params['secret_key'], params['region'],
                                     params.get("session_token", None), params["table_name"])
            count = dynamo_object.get_live_item_count()
            rv = {
                "count": count
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check


@app.route('/dynamo/delete_table', methods=['DELETE'])
def drop_dynamo_database():
    params = request.json
    checklist = ["access_key", "secret_key", "region", "table_name"]
    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        try:
            dynamo_object = DynamoDb(params['access_key'], params['secret_key'], params['region'],
                                     params.get("session_token", None), params["table_name"])
            dynamo_object.delete_table()
            rv = {
                "response": f"SUCCESS, table {params['table_name']} deleted successfully"
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "Error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check


# -- s3 --
@app.route('/s3/start_loader', methods=['POST'])
def start_s3_loader():
    params = request.json
    checklist = ["access_key", "secret_key", "region", "num_buckets", "depth_level", "num_folders_per_level",
                 "num_files_per_level"]

    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        if "loader_id" in params:
            loader_id = params['loader_id']
            result = loader_collection.find_one({"loader_id": params['loader_id']})
            if not result:
                rv = {
                    "ERROR": f"No loader found for loader_id {params['loader_id']}",
                    "status": "failed"
                }
                return jsonify(rv), 409
            loader_obj = loaderIdvsDocobject[loader_id]
            loader_status = loader_obj.is_loader_running(db="s3")

            if loader_status:
                rv = {
                    "response": f"Loader {loader_id} is already running",
                    "loader_id": loader_id,
                    "bucket": result['database'],
                    "status": "failed"
                }
                return jsonify(rv), 200
            else:
                loader_obj.start_running_loader(db="s3")
                rv = {
                    "response": f"Loader {loader_id} restarted successfully",
                    "loader_id": loader_id,
                    "bucket": result['database'],
                    "status": "running"
                }
                loader_sdk.update_document(loader_collection_name, {"loader_id": loader_id},
                                           {"status": "running"})
                return jsonify(rv), 200
        else:
            loader_id = str(uuid.uuid4())

            loader_data = {"loader_id": loader_id, "docloader": DocLoader(), "status": "running",
                           "database": "", "collection": ""}

            s3_config = s3Config(params['access_key'], params['secret_key'], params['region'], params['num_buckets'],
                                 params['depth_level'], params['num_folders_per_level'],
                                 params['num_files_per_level'], params.get('session_token', None),
                                 params.get('file_size', 1024), params.get('max_file_size', 10240),
                                 params.get('file_format', ['json', 'csv', 'tsv']))

            buckets = loader_data["docloader"].create_s3_using_specified_config(s3_config)
            if isinstance(buckets, str):
                rv = {
                    "response": f"Failed to create bucket",
                    "error": buckets
                }
                return jsonify(rv), 200
            loader_data["database"] = buckets
            loader_data["collection"] = buckets
            thread1 = threading.Thread(target=loader_data['docloader'].perform_crud_on_s3,
                                       args=(s3_config, buckets, params.get('duration_minutes', float('inf')),
                                             params.get('max_files', params['num_files_per_level'] + 10),
                                             params.get('min_files', 1)))
            thread1.start()

            loaderIdvsDocobject[loader_id] = loader_data['docloader']

            # since threads would be deleted automatically so removing this field as of now.
            # We would need this docloader object to stop the loader (update the class field)
            del loader_data['docloader']

            loader_sdk.insert_single_document(loader_collection_name, loader_data)

            del loader_data['_id']
            return jsonify(loader_data), 200

    else:
        return params_check


@app.route('/s3/stop_loader', methods=['POST'])
def stop_s3_loader():
    params = request.json
    checklist = ["loader_id"]
    params_check = check_request_body(params, checklist)

    if params_check:
        loader_id = request.json.get("loader_id")

        loaders = loader_collection.find({})
        for loader in loaders:
            if loader_id == loader['loader_id']:
                if loader['status'] == "running":
                    loaderIdvsDocobject[loader_id].stop_running_loader(db="s3")
                    loader_sdk.update_document(loader_collection_name, {"loader_id": loader['loader_id']},
                                               {"status": "stopped"})
                    loader['status'] = "stopped"
                    rv = {
                        "response": f"Loader {loader_id} stopped successfully",
                        "loader_id": loader_id,
                        "bucket": loader['database'],
                        "status": loader['status']
                    }
                    return jsonify(rv), 200
                else:
                    return jsonify({"response": f"Loader {loader_id} is not running"}), 200

        return jsonify({"response": f"No loader found with ID {loader_id}"}), 200
    else:
        return params_check


@app.route('/s3/delete_bucket', methods=['DELETE'])
def drop_s3_bucket():
    params = request.json
    checklist = ["access_key", "secret_key", "bucket_name"]

    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        s3_sdk = s3SDK(params['access_key'], params['secret_key'], params.get('session_token', None))
        try:
            s3_sdk.delete_bucket(params['bucket_name'])
            rv = {
                "response": f"SUCCESS dropped bucket {params['bucket_name']}"
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check


@app.route('/s3/restore', methods=['POST'])
def restore_s3_bucket():
    params = request.json
    checklist = ["access_key", "secret_key", "region", "num_buckets", "depth_level", "num_folders_per_level",
                 "num_files_per_level", "bucket_name"]

    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        try:
            s3_config = s3Config(params['access_key'], params['secret_key'], params['region'], params['num_buckets'],
                                 params['depth_level'], params['num_folders_per_level'],
                                 params['num_files_per_level'], params.get('session_token', None),
                                 params.get('file_size', 1024), params.get('max_file_size', 10240),
                                 params.get('file_format', ['json', 'csv', 'tsv']))

            DocLoader().restore_s3(
                s3SDK(params['access_key'], params['secret_key'], params.get('session_token', None)),
                params['bucket_name'], s3_config)
            rv = {
                "response": f"Success, restore started successfully"
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check


def init_mysql_setup(config, database_name, table_columns, table_name):
    mysql = MySQLSDK(config)
    mysql.create_database(database_name)
    mysql.use_database(database_name)
    mysql.create_table(table_name, table_columns)
    return mysql


@app.route('/mysql/start_loader', methods=['POST'])
def start_mysql_loader():
    params = request.json
    checklist = ["host", "port", "username", "password", "database_name", "table_name", "table_columns"]

    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        loaders = loader_collection.find({})
        # check if there is a loader already running on same db and collection
        for loader in loaders:
            if loader['database'] == params['database_name'] and loader['collection'] == params['table_name'] and \
                    loader['status'] == "running":
                rv = {
                    "ERROR": f"There is already a loader running on {params['database_name']} and {params['table_name']}. You can poll for the loader to be stopped",
                    "loader_id": loader['loader_id'],
                    "database": loader['database'],
                    "table": loader['collection'],
                    "status": "failed"
                }
                return jsonify(rv), 409

        if "loader_id" in params:
            params['init_config'] = False
            result = loader_collection.find_one({"loader_id": params['loader_id']})
            if not result:
                rv = {
                    "ERROR": f"No loader found for loader_id {params['loader_id']}",
                    "status": "failed"
                }
                return jsonify(rv), 409
            loader_id = params['loader_id']
            loader_obj = loaderIdvsDocobject[loader_id]
            loader_status = loader_obj.is_loader_running(db="mysql")
            if loader_status:
                rv = {
                    "response": f"Loader {loader_id} is already running",
                    "loader_id": loader_id,
                    "database": params['database_name'],
                    "table": params['table_name'],
                    "status": "failed"
                }
                return jsonify(rv), 200
            else:
                loader_obj.start_running_loader(db="mysql")
                rv = {
                    "response": f"Loader {loader_id} restarted successfully",
                    "loader_id": loader_id,
                    "database": params['database_name'],
                    "table": params['table_name'],
                    "status": "running"
                }
                loader_sdk.update_document(loader_collection_name, {"loader_id": loader_id},
                                           {"status": "running"})
                return jsonify(rv), 200
        else:
            loader_id = str(uuid.uuid4())

            loader_data = {"loader_id": loader_id, "docloader": DocLoader(), "status": "running",
                           "database": params['database_name'], "collection": params['table_name']}

            mysql_config = MySQLConfig(host=params['host'], port=params['port'], username=params['username'],
                                       password=params['password'])
            table_columns = (params['table_columns'])
            if params['init_config']:
                mysql_obj = init_mysql_setup(mysql_config, params['database_name'], table_columns, params['table_name'])
            else:
                mysql_obj = MySQLSDK(mysql_config)

            thread1 = threading.Thread(target=loader_data['docloader'].perform_crud_on_mysql,
                                       args=(mysql_obj, params['table_name'], table_columns,
                                             params.get('duration_minutes', float('inf')),
                                             params.get('max_files_extra', 50), params.get('atleast_min_files', None)))
            thread1.start()

            loaderIdvsDocobject[loader_id] = loader_data['docloader']

            # since threads would be deleted automatically so removing this field as of now.
            # We would need this docloader object to stop the loader (update the class field)
            del loader_data['docloader']

            loader_sdk.insert_single_document(loader_collection_name, loader_data)

            del loader_data['_id']
            return jsonify(loader_data), 200

    else:
        return params_check


@app.route('/mysql/stop_loader', methods=['POST'])
def stop_mysql_loader():
    params = request.json
    checklist = ["loader_id"]
    params_check = check_request_body(params, checklist)

    if params_check:
        loader_id = request.json.get("loader_id")

        loaders = loader_collection.find({})
        for loader in loaders:
            if loader_id == loader['loader_id']:
                if loader['status'] == "running":
                    loaderIdvsDocobject[loader_id].stop_running_loader(db="mysql")
                    loader_sdk.update_document(loader_collection_name, {"loader_id": loader['loader_id']},
                                               {"status": "stopped"})
                    loader['status'] = "stopped"
                    rv = {
                        "response": f"Loader {loader_id} stopped successfully",
                        "loader_id": loader_id,
                        "database": loader['database'],
                        "collection": loader['collection'],
                        "status": loader['status']
                    }
                    return jsonify(rv), 200
                else:
                    return jsonify({"response": f"Loader {loader_id} is not running"}), 200

        return jsonify({"response": f"No loader found with ID {loader_id}"}), 200
    else:
        return params_check


@app.route('/mysql/count', methods=['GET'])
def get_docs_in_mysql():
    params = request.json
    checklist = ["host", "port", "username", "password", "database_name", "table_name"]

    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        mysql_config = MySQLConfig(host=params['host'], port=params['port'], username=params['username'],
                                   password=params['password'])
        mysql_sdk = MySQLSDK(mysql_config)
        try:
            mysql_sdk.use_database(params['database_name'])
            count = mysql_sdk.get_total_records_count(params['table_name'])
            rv = {
                "count": count
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check


@app.route('/mysql/delete_database', methods=['DELETE'])
def delete_mysql_database():
    params = request.json
    checklist = ["host", "port", "username", "password", "database_name"]
    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        mysql_config = MySQLConfig(host=params['host'], port=params['port'], username=params['username'],
                                   password=params['password'])
        mysql_sdk = MySQLSDK(mysql_config)
        try:
            mysql_sdk.use_database(params['database_name'])
            mysql_sdk.delete_database(params["database_name"])
            rv = {
                "response": "SUCCESS"
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "error": str(e)
            }
            return jsonify(rv), 200
    else:
        return params_check


@app.route('/mysql/delete_table', methods=['DELETE'])
def delete_mysql_table():
    params = request.json
    checklist = ["host", "port", "username", "password", "database_name", "table_name"]
    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        mysql_config = MySQLConfig(host=params['host'], port=params['port'], username=params['username'],
                                   password=params['password'])
        mysql_sdk = MySQLSDK(mysql_config)
        try:
            mysql_sdk.use_database(params['database_name'])
            mysql_sdk.delete_table(params["table_name"])
            rv = {
                "response": "SUCCESS"
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check

@app.route('/mysql/restore', methods=['POST'])
def restore_mysql_table():
    params = request.json
    checklist = ["host", "port", "username", "password", "doc_count", "database_name", "table_name", "table_columns"]
    params_check = check_request_body(params, checklist)
    if params_check[1] != 422:
        mysql_config = MySQLConfig(host=params['host'], port=params['port'], username=params['username'],
                                   password=params['password'])
        try:
            DocLoader().rebalance_mysql_docs(doc_count=params['doc_count'], table_name=params['table_name'], table_columns=params['table_columns'], config=mysql_config, database_name=params['database_name'])
            rv = {
                "response": "SUCCESS"
            }
            return jsonify(rv), 200
        except Exception as e:
            rv = {
                "error": str(e)
            }
            return jsonify(rv), 200

    else:
        return params_check


@app.route('/loaders', methods=['GET'])
def get_all_loaders():
    rv = []
    loaders = loader_collection.find({})
    for loader in loaders:
        del loader['_id']
        rv.append(loader)
    return jsonify(rv), 200


@app.route('/loaders/<loader_id>', methods=['GET'])
def get_loader_info(loader_id):
    loaders = loader_collection.find({})
    for loader in loaders:
        if loader['loader_id'] == loader_id:
            del loader['_id']
            return jsonify(loader), 200

    return jsonify({"response": f"No loader found with ID {loader_id}"}), 200


@app.route('/', methods=['GET'])
def welcome():
    return 'Welcome to Docloading server'


if __name__ == '__main__':
    app.run(debug=False)
