import boto3
import concurrent.futures
import logging
import math
import os
import time
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError
from prettytable import PrettyTable

from SDKs.s3.s3_operations import s3Operations


class s3SDK:
    def __init__(self, access_key, secret_key, session_token=None):
        logging.basicConfig()
        self.logger = logging.getLogger("AWS_Util")
        self.create_session(access_key, secret_key, session_token)
        self.s3_client = self.create_service_client(service_name="s3")
        self.s3_resource = self.create_service_resource(resource_name="s3")

    def create_session(self, access_key, secret_key, session_token=None):
        """
        Create a session to AWS using the credentials provided.
        If no credentials are provided then, credentials are read from aws_config.json file.

        :param access_key: access key for the IAM user
        :param secret_key: secret key for the IAM user
        :param access_token:
        """
        try:
            if session_token:
                self.aws_session = boto3.Session(aws_access_key_id=access_key,
                                                 aws_secret_access_key=secret_key,
                                                 aws_session_token=session_token)
            else:
                self.aws_session = boto3.Session(aws_access_key_id=access_key,
                                                 aws_secret_access_key=secret_key)
        except Exception as e:
            self.logger.error(e)

    def create_service_client(self, service_name, region=None):
        """
        Create a low level client for the service specified.
        If a region is not specified, the client is created in the default region (us-east-1).
        :param service_name: name of the service for which the client has to be created
        :param region: region in which the client has to created.
        """
        try:
            if region is None:
                return self.aws_session.client(service_name)
            else:
                return self.aws_session.client(service_name, region_name=region)
        except ClientError as e:
            self.logger.error(e)

    def create_service_resource(self, resource_name):
        """
        Create a service resource object, to access resources related to service.
        """
        try:
            return self.aws_session.resource(resource_name)
        except Exception as e:
            self.logger.error(e)

    def get_region_list(self):
        # Retrieves all regions/endpoints
        ec2 = self.create_service_client(service_name="ec2", region="us-west-1")
        response = ec2.describe_regions()
        if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return [region['RegionName'] for region in response["Regions"]]
        else:
            return []

    def create_bucket(self, bucket_name, region):
        """
        Create an S3 bucket in a specified region
        If a region is not specified, the bucket is created in the S3 default region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """
        # Create bucket
        try:
            location = {'LocationConstraint': region}
            response = self.s3_resource.Bucket(bucket_name).create(CreateBucketConfiguration=location)
            if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
                return True, None
            else:
                return False, None
        except Exception as e:
            self.logger.error(e)
            return False, str(e)

    def delete_bucket(self, bucket_name, max_retry=5, retry_attempt=0):
        """
        Deletes a bucket
        :param bucket_name: Bucket to delete
        """
        try:
            bucket_deleted = False
            if retry_attempt < max_retry:
                if bucket_name in self.list_existing_buckets():
                    if self.empty_bucket(bucket_name):
                        response = self.s3_resource.Bucket(bucket_name).delete()
                        if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                            bucket_deleted = True
                    if not bucket_deleted:
                        self.delete_bucket(bucket_name, max_retry, retry_attempt + 1)
                    else:
                        return bucket_deleted
                else:
                    return False
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def delete_file(self, bucket_name, file_path, version_id=None):
        """
        Deletes a single file/object, identified by object name.
        :param bucket_name: Bucket whose objects are to be deleted.
        :param file_path: path of the file, relative to S3 bucket
        :param version_id: to delete a specific version of an object.
        """
        try:
            s3_object = self.s3_resource.Object(bucket_name, file_path)
            if version_id:
                response = s3_object.delete(VersionId=version_id)
            else:
                response = s3_object.delete()
            if response["ResponseMetadata"]["HTTPStatusCode"] == 204:
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def delete_folder(self, bucket_name, folder_path):
        try:
            bucket_resource = self.s3_resource.Bucket(bucket_name)
            for obj in bucket_resource.objects.filter(Prefix="{}/".format(folder_path)):
                self.s3_resource.Object(bucket_resource.name, obj.key).delete()
            return True
        except Exception as e:
            self.logger.error(e)
            return False

    def empty_bucket(self, bucket_name):
        """
        Deletes all the objects in the bucket.
        :param bucket_name: Bucket whose objects are to be deleted.
        """
        try:
            # Checking whether versioning is enabled on the bucket or not
            response = self.s3_resource.BucketVersioning(bucket_name).status
            if not response:
                versioning = True
            else:
                versioning = False

            # Create a bucket resource object
            bucket_resource = self.s3_resource.Bucket(bucket_name)

            # Empty the bucket before deleting it.
            # If versioning is enabled delete all versions of all the objects, otherwise delete all the objects.
            if versioning:
                response = bucket_resource.object_versions.all().delete()
            else:
                response = bucket_resource.objects.all().delete()
            status = True
            for item in response:
                if item["ResponseMetadata"]["HTTPStatusCode"] != 200:
                    status = status and False
            return status
        except Exception as e:
            self.logger.error(e)
            return False

    def list_existing_buckets(self):
        """
        List all the S3 buckets.
        """
        try:
            response = self.s3_client.list_buckets()
            if response:
                return [x["Name"] for x in response['Buckets']]
            else:
                return []
        except Exception as e:
            self.logger.error(e)
            return []

    def get_region_wise_buckets(self):
        """
        Fetch all buckets in all regions.
        """
        try:
            buckets = self.list_existing_buckets()
            if buckets:
                region_wise_bucket = {}
                for bucket in buckets:
                    region = self.get_bucket_region(bucket)
                    if region in region_wise_bucket:
                        region_wise_bucket[region].append(bucket)
                    else:
                        region_wise_bucket[region] = [bucket]
                return region_wise_bucket
            else:
                return {}
        except Exception as e:
            self.logger.error(e)
            return {}

    def get_bucket_region(self, bucket_name):
        """
        Gets the region where the bucket is located
        """
        try:
            response = self.s3_client.list_buckets(Bucket=bucket_name)
            if response:
                if response["LocationConstraint"] == None:
                    return "us-east-1"
                else:
                    return response["LocationConstraint"]
            else:
                return ""
        except Exception as e:
            self.logger.error(e)
            return ""

    def upload_file(self, bucket_name, source_path, destination_path):
        """
        Uploads a file to bucket specified.
        :param bucket_name: name of the bucket where file has to be uploaded.
        :param source_path: path of the file to be uploaded.
        :param destination_path: path relative to aws bucket. If only file name is specified
        then file will be loaded in root folder relative to bucket.
        :return: True/False
        """
        try:
            response = self.s3_resource.Bucket(bucket_name).upload_file(source_path, destination_path)
            if not response:
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def upload_file_with_content(self, bucket_name, object_key, content):
        try:
            self.s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=content)
            try:
                os.remove(content)
            except:
                pass
        except Exception as e:
            self.logger.error(f"Could not upload file in {bucket_name} with path {object_key} : {str(e)}")
            self.logger.error(e)

    def upload_large_file(self, bucket_name, source_path, destination_path,
                          multipart_threshold=1024 * 1024 * 8, max_concurrency=10,
                          multipart_chunksize=1024 * 1024 * 8, use_threads=True):
        """
        Uploads a large file to bucket specified.
        :param bucket_name: name of the bucket where file has to be uploaded.
        :param source_path: path of the file to be uploaded.
        :param destination_path: path relative to aws bucket. If only file name is specified
        then file will be loaded in root folder relative to bucket.
        :param multipart_threshold: The transfer size threshold.
        :param max_concurrency: The maximum number of threads that will be
            making requests to perform a transfer. If ``use_threads`` is
            set to ``False``, the value provided is ignored as the transfer
            will only ever use the main thread.
        :param multipart_chunksize: The partition size of each part for a
            multipart transfer.
        :param use_threads: If True, threads will be used when performing
            S3 transfers. If False, no threads will be used in
            performing transfers
        :return: True/False
        """
        """
        WARNING : Please use this function if you want to upload a large file only (ex - file above 10 MB, 
        again this value is only subjective), as this API call to AWS is charged extra.
        """
        try:
            config = TransferConfig(multipart_threshold=multipart_threshold, max_concurrency=max_concurrency,
                                    multipart_chunksize=multipart_chunksize, use_threads=use_threads)
            response = self.s3_resource.Bucket(bucket_name).upload_file(source_path, destination_path,
                                                                        Config=config)
            if not response:
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def download_file(self, bucket_name, filename, dest_path):
        try:
            response = self.s3_resource.Bucket(bucket_name).download_file(filename, dest_path)
            if not response:
                return True
            else:
                return False
        except Exception as e:
            self.logger.error(e)
            return False

    def generate_and_upload_structure(self, config, bucket_name, base_path='', depth_lvl=0, file_types=['json']):
        s3_op = s3Operations()
        if depth_lvl >= config.depth_level:
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            futures = []

            for folder_num in range(config.num_folders_per_level):
                folder_name = f'Depth_{depth_lvl}_Folder_{folder_num}/'

                for count in range(int(math.ceil(config.num_files_per_level / config.num_folders_per_level))):
                    file_type = file_types[count % len(file_types)]
                    file_name = f"{count}.{file_type}"
                    file_content = s3_op.create_file_with_required_file_type(file_type, config.file_size,
                                                                             config.num_rows_per_file)
                    if file_type != "json":
                        with open(file_content, 'rb') as file:
                            file_content = file.read()
                    s3_object_key = os.path.join(base_path, folder_name, file_name)

                    # Submit the task to the executor
                    future = executor.submit(
                        self.upload_file_with_content, bucket_name, s3_object_key, file_content
                    )
                    futures.append(future)

            # Wait for all submitted tasks to complete
            concurrent.futures.wait(futures)

            # Recursively create nested folders
            for folder_num in range(config.num_folders_per_level):
                folder_name = f'Depth_{depth_lvl}_Folder_{folder_num}/'
                self.generate_and_upload_structure(
                    config,
                    bucket_name,
                    base_path=os.path.join(base_path, folder_name),
                    depth_lvl=depth_lvl + 1,
                    file_types=file_types
                )

    def list_files_in_folder(self, bucket, folder_path):
        """
        List files in a specific folder within an S3 bucket.

        Parameters:
        - bucket (str): The name of the S3 bucket.
        - folder_path (str): The path of the folder within the bucket.

        Returns:
        - List[str]: A list of file names in the specified folder.
        """
        # Add a trailing slash to the folder path if it doesn't exist
        if not folder_path.endswith('/'):
            folder_path += '/'

        try:
            # Use the list_objects_v2 API to get a list of objects in the folder
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=folder_path
            )

            files = [obj['Key'] for obj in response.get('Contents', [])]

            return files
        except Exception as e:
            print(f"Error listing files in folder {folder_path}: {e}")
            return []

    def print_bucket_structure(self, bucket):
        my_bucket = self.s3_resource.Bucket(bucket)

        table = PrettyTable()
        table.field_names = ["Bucket", "Key"]

        for my_bucket_object in my_bucket.objects.all():
            table.add_row([my_bucket_object.bucket_name, my_bucket_object.key])

        print(table)
