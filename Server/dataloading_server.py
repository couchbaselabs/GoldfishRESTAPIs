from flask import Flask, request, jsonify
from Docloader.doc_loader import DocLoader
from SDKs.MongoDB.MongoConfig import MongoConfig
from SDKs.MongoDB.MongoSDK import MongoSDK
import uuid
import threading

app = Flask(__name__)

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


@app.route('/start_loader', methods=['POST'])
def start_loader():
    params = request.json
    checklist = ["ip", "port", "username", "password", "database_name", "collection_name",
                 "target_num_docs",
                 "time_for_crud_in_mins", "num_buffer"]

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

        if "loader_id" in params:
            loader_id = params['loader_id']
            loader_obj = loaderIdvsDocobject[loader_id]
            loader_status = loader_obj.is_loader_running()
            if loader_status:
                rv = {
                    "response": f"Loader {loader_id} is already running",
                    "loader_id": loader_id,
                    "database": params['database_name'],
                    "collection": params['collection_name'],
                    "status": "running"
                }
                return jsonify(rv), 200
            else:
                loader_obj.start_running_loader()
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
            loader_id = str(uuid.uuid4())

            loader_data = {"loader_id": loader_id, "docloader": DocLoader(), "status": "running",
                           "database": params['database_name'], "collection": params['collection_name']}

            mongo_config = MongoConfig(params['ip'], params['port'], params['username'], params['password'],
                                       params['database_name'])
            thread1 = threading.Thread(target=loader_data['docloader'].perform_crud_on_mongo,
                                       args=(mongo_config, params['collection_name'], params['target_num_docs'],
                                             params['time_for_crud_in_mins'], params['num_buffer']))
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


@app.route('/stop_loader', methods=['POST'])
def stop_loader():
    params = request.json
    checklist = ["loader_id"]
    params_check = check_request_body(params, checklist)

    if params_check:
        loader_id = request.json.get("loader_id")

        loaders = loader_collection.find({})
        for loader in loaders:
            if loader_id == loader['loader_id']:
                if loader['status'] == "running":
                    loaderIdvsDocobject[loader_id].stop_running_loader()
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
