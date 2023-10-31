import requests
import json


class DocloadingAPIs:
    def __init__(self, server_ip, server_port):
        self.server_ip = server_ip
        self.server_port = server_port
        self.url = "http://{}:{}/".format(server_ip, server_port)
        self.headers = {
            'Content-Type': 'application/json'
        }

    # mongo
    def start_mongo_loader(self, ip, database_name, collection_name, atlas_url=None, port=27017,
                           username="", password="", headers=None,
                           num_buffer=500, initial_doc_count=None, loader_id=None):
        url = self.url + "mongo/start_loader"
        if not headers:
            headers = self.headers

        payload = json.dumps({
            "ip": ip,
            "port": port,
            "username": username,
            "password": password,
            "database_name": database_name,
            "collection_name": collection_name,
            "atlas_url": atlas_url,
            'num_buffer': num_buffer,
            'initial_doc_count': initial_doc_count,
            'loader_id': loader_id
        })

        response = requests.post(url, headers=headers, data=payload)
        return response

    def stop_crud_on_mongo(self, loader_id, headers=None):
        url = self.url + "mongo/stop_loader"
        if not headers:
            headers = self.headers

        payload = json.dumps({
            "loader_id": loader_id
        })

        return requests.post(url, headers=headers, data=payload)

    def get_mongo_doc_count(self, headers, ip, database_name, collection_name, username=None, password=None,
                            atlas_url=None, port=27017):
        url = self.url + "mongo/start_loader"
        if not headers:
            headers = self.headers

        payload = json.dumps({
            "ip": ip,
            "port": port,
            "username": username,
            "password": password,
            "database_name": database_name,
            "collection_name": collection_name,
            "atlas_url": atlas_url
        })

        response = requests.get(url, headers=headers, data=payload)
        return response

    def drop_mongo_database(self, ip, database_name, collection_name, username=None, password=None, headers=None,
                            port=27017):
        url = self.url + "mongo/delete_database"
        if not headers:
            headers = self.headers

        payload = json.dumps({
            "ip": ip,
            "port": port,
            "username": username,
            "password": password,
            "database_name": database_name,
            "collection_name": collection_name,
        })

        return requests.delete(url, headers=headers, data=payload)

    def delete_mongo_collection(self, ip, port, database_name, collection_name, username=None, password=None,
                                headers=None):
        url = self.url + "mongo/delete_collection"

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "ip": ip,
            "port": port,
            "username": username,
            "password": password,
            "database_name": database_name,
            "collection_name": collection_name,
        })

        return requests.delete(url, headers=headers, data=payload)

    # Dynamo
    def start_dynamo_loader(self, access_key, secret_key, primary_key_field, table_name, region, initial_doc_count,
                            num_buffer=500,
                            headers=None, session_token=None, loader_id=None):
        endpoint_url = "{}dynamo/start_loader".format(self.url)
        if not headers:
            headers = self.headers

        payload = json.dumps({
            "access_key": access_key,
            "secret_key": secret_key,
            "primary_key_field": primary_key_field,
            "table_name": table_name,
            "region": region,
            "num_buffer": num_buffer,
            "initial_doc_count": initial_doc_count,
            "session_token": session_token,
            'loader_id': loader_id
        })

        response = requests.post(endpoint_url, headers=headers, data=payload)
        return response

    def stop_dynamo_loader(self, loader_id, headers=None):
        endpoint_url = "{}dynamo/stop_loader".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "loader_id": loader_id
        })

        response = requests.post(endpoint_url, headers=headers, data=payload)
        return response

    def count_dynamo_documents(self, access_key, secret_key, region, table_name, headers=None):
        endpoint_url = "{}dynamo/count".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "access_key": access_key,
            "secret_key": secret_key,
            "region": region,
            "table_name": table_name
        })

        response = requests.get(endpoint_url, headers=headers, data=payload)
        return response

    def delete_dynamo_table(self, table_name, access_key, secret_key, region, headers=None):
        endpoint_url = "{}dynamo/delete_table".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "table_name": table_name,
            "access_key": access_key,
            "secret_key": secret_key,
            "region": region
        })

        response = requests.delete(endpoint_url, headers=headers, data=payload)
        return response

    # MySQL
    def start_mysql_loader(self, host, port, username, password, database_name, table_name, table_columns, init_config,
                           num_buffer=500, initial_doc_count=None, headers=None, loader_id=None):
        endpoint_url = "{}mysql/start_loader".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database_name": database_name,
            "table_name": table_name,
            "table_columns": table_columns,
            "init_config": init_config,
            "num_buffer": num_buffer,
            "initial_doc_count": initial_doc_count,
            "loader_id": loader_id
        })

        response = requests.post(endpoint_url, headers=headers, data=payload)
        return response

    def stop_mysql_loader(self, loader_id, headers=None):
        endpoint_url = "{}mysql/stop_loader".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "loader_id": loader_id
        })

        response = requests.post(endpoint_url, headers=headers, data=payload)
        return response

    def count_mysql_documents(self, host, port, username, password, database_name, table_name, headers=None):
        endpoint_url = "{}mysql/count".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database_name": database_name,
            "table_name": table_name
        })

        response = requests.get(endpoint_url, headers=headers, data=payload)
        return response

    def delete_mysql_database(self, host, port, username, password, database_name, table_name, headers=None):
        endpoint_url = "{}mysql/delete_database".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database_name": database_name,
            "table_name": table_name
        })

        response = requests.delete(endpoint_url, headers=headers, data=payload)
        return response

    def delete_mysql_table(self, host, port, username, password, database_name, table_name, headers=None):
        endpoint_url = "{}mysql/delete_table".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database_name": database_name,
            "table_name": table_name
        })

        response = requests.delete(endpoint_url, headers=headers, data=payload)
        return response

    def restore_mysql_database(self, host, port, username, password, database_name, table_name, table_columns,
                               doc_count, headers=None):
        endpoint_url = "{}mysql/restore".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "host": host,
            "port": port,
            "username": username,
            "password": password,
            "database_name": database_name,
            "table_name": table_name,
            "table_columns": table_columns,
            "doc_count": doc_count
        })

        response = requests.post(endpoint_url, headers=headers, data=payload)
        return response

    # s3
    def start_s3_loader(self, access_key, secret_key, region, num_buckets, depth_level, num_folders_per_level,
                        num_files_per_level, file_format, headers=None, session_token=None, file_size=1024,
                        loader_id=None):
        endpoint_url = "{}s3/start_loader".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "access_key": access_key,
            "secret_key": secret_key,
            "region": region,
            "num_buckets": num_buckets,
            "depth_level": depth_level,
            "num_folders_per_level": num_folders_per_level,
            "num_files_per_level": num_files_per_level,
            "file_format": file_format,
            "session_token": session_token,
            "file_size": file_size,
            "loader_id": loader_id
        })

        response = requests.post(endpoint_url, headers=headers, data=payload)
        return response

    def stop_s3_loader(self, loader_id, headers=None):
        endpoint_url = "{}s3/stop_loader".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "loader_id": loader_id
        })

        response = requests.post(endpoint_url, headers=headers, data=payload)
        return response

    def restore_s3_bucket(self, access_key, secret_key, region, num_buckets, depth_level, num_folders_per_level,
                          num_files_per_level, file_format, bucket_name, headers=None):
        endpoint_url = "{}s3/restore".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "access_key": access_key,
            "secret_key": secret_key,
            "region": region,
            "num_buckets": num_buckets,
            "depth_level": depth_level,
            "num_folders_per_level": num_folders_per_level,
            "num_files_per_level": num_files_per_level,
            "file_format": file_format,
            "bucket_name": bucket_name
        })

        response = requests.post(endpoint_url, headers=headers, data=payload)
        return response

    def delete_s3_bucket(self, access_key, secret_key, region, bucket_name, headers=None):
        endpoint_url = "{}s3/delete_bucket".format(self.url)

        if not headers:
            headers = self.headers

        payload = json.dumps({
            "access_key": access_key,
            "secret_key": secret_key,
            "region": region,
            "bucket_name": bucket_name
        })

        response = requests.delete(endpoint_url, headers=headers, data=payload)
        return response

    # Generic
    def get_all_loaders(self, headers=None):
        endpoint_url = "{}loaders".format(self.url)

        if not headers:
            headers = self.headers

        response = requests.get(endpoint_url, headers=headers)
        return response

    def get_specific_loader(self, loader_id, headers=None):
        endpoint_url = "{}loaders/{}".format(self.url, loader_id)

        if not headers:
            headers = self.headers

        response = requests.get(endpoint_url, headers=headers)
        return response
