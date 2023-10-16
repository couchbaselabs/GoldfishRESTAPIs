"""
SDK for dynamoDB
You likely have your AWS credentials already configured on your machine before using this class
AWS Access Key ID: "yourAccessKeyId"
AWS Secret Access Key: "yourAccessKey"
Default region name : "yourRegionName"
"""
import boto3
import logging
from botocore.exceptions import ClientError


class DynamoDb:
    """
    SDK class for dynamoDb, Helps in managing dynamoDB entities
    """

    def __init__(self, endpoint_url=None, table=None, region=None):
        if endpoint_url is None and region is None:
            raise ValueError("DynamoDB URL or region is required")
        if endpoint_url is None:
            self.dyn_resource = boto3.resource('dynamodb', region_name=region)
        else:
            self.dyn_resource = boto3.resource(
                'dynamodb', endpoint_url=endpoint_url)
        self.table = self.dyn_resource.Table(name=table) if table else None
        self.table_name = table
        self.region = region

    def create_table(self, table_name, key_schema,
                     attribute_definitions, read_write_capacity_units, **params):
        """
        Create table in dynamoDb
        :param read_write_capacity_units: {'ReadCapacityUnits': read_capacity_units,
                                                     'WriteCapacityUnits': write_capacity_units}
        :param table_name:
        :param key_schema: Provide key value pair in format: {key : keyType}
        :param attribute_definitions: Provide key value pair in format: {key : AttributeType}
        :param params: Any other arguments
        :return: table object
        """
        table_params = {
            'TableName': table_name,
            'KeySchema': [{'AttributeName': key, 'KeyType': value}
                          for key, value in key_schema.items()],
            'AttributeDefinitions': [{'AttributeName': key, 'AttributeType': value}
                                     for key, value in attribute_definitions.items()]
        }
        if read_write_capacity_units:
            table_params['ProvisionedThroughput'] = read_write_capacity_units
        for key, value in params:
            table_params[key] = value

        try:
            self.table = self.dyn_resource.create_table(**table_params)
            self.table.wait_until_exists()
            return self.table
        except ClientError as err:
            logging.error("Couldn't create table %s. Here's why: %s: %s", table_name,
                          err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def delete_table(self):
        """
        Delete the table present in dynamoDB object
        """
        try:
            self.table.delete()
            self.table = None
        except ClientError as err:
            logging.error(
                "Couldn't delete table. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def delete_item(self, item_key, condition_expression=None,
                    expression_attribute_values=None):
        """
        Delete an item in inside the dynamoDB tables
        :param item_key:
        :param condition_expression:
        :param expression_attribute_values:
        :param params:
        """
        delete_params = {}
        if item_key:
            delete_params['Key'] = item_key
        if condition_expression:
            delete_params['ConditionExpression'] = condition_expression
        if expression_attribute_values:
            delete_params['ExpressionAttributeValues'] = expression_attribute_values
        try:
            self.table.delete_item(**delete_params)
        except ClientError as err:
            logging.error(
                "Couldn't delete item with key %s. Here's why: %s: %s", item_key,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def get_item(self, item_key):
        """
        Retrieve an item form the dynamoDB table
        :param item_key:
        :return:
        """
        try:
            response = self.table.get_item(Key=item_key)
            return response['Item']
        except ClientError as err:
            logging.error(
                "Couldn't get item %s from table %s. Here's why: %s: %s",
                item_key, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def add_item(self, item):
        """
        Put an item in the dynamoDB table
        :param item:
        """
        try:
            self.table.put_item(Item=item)
        except ClientError as err:
            logging.error(
                "Couldn't add item: Error: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def update_item(self, item_key, changed_object_json):
        """
        Update an item already present in the dynamoDB table
        :param item_key:
        :param changed_object_json:
        :return:
        """
        update_params = {
            'Key': item_key
        }
        update_expression = "set "
        expression_attribute_values = {}
        for key, value in changed_object_json.items():
            update_expression = update_expression + f"info.{key}=:{key},"
            expression_attribute_values[':' + key] = value
        update_params['UpdateExpression'] = update_expression
        update_params['ExpressionAttributeValues'] = expression_attribute_values
        try:
            response = self.table.update_tem(**update_params)
            return response['Attributes']
        except ClientError as err:
            logging.error(
                "Couldn't update item %s in table %s. Here's why: %s: %s",
                item_key, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def list_tables(self):
        """
        List all the tables present in dynamoDB
        :return:
        """
        try:
            tables = []
            for table in self.dyn_resource.tables.all():
                tables.append(table)
            return tables
        except ClientError as err:
            logging.error(
                "Couldn't list tables. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def query_table(self, key_condition_expression, projection_expression=None,
                    expression_attribute_names=None):
        """
        Query a dynamoDB table
        :param key_condition_expression:
        :param projection_expression:
        :param expression_attribute_names:
        :return:
        """
        query_params = {'KeyConditionExpression': key_condition_expression}
        if projection_expression:
            query_params['ProjectionExpression'] = projection_expression
        if expression_attribute_names:
            query_params['ExpressionAttributeNames'] = expression_attribute_names
        try:
            response = self.table.query(**query_params)
        except ClientError as err:
            if err.response['Error']['Code'] == "ValidationException":
                logging.warning(
                    "There's a validation error. Here's the message: %s: %s",
                    err.response['Error']['Code'], err.response['Error']['Message'])
            else:
                logging.error(
                    "Couldn't query for item. Here's why: %s: %s",
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        return response['Items']

    def scan_table(self, filter_expression=None, projection_expression=None,
                   expression_attribute_names=None, count=False,
                   **params):
        """
        Scan through all the document in a table
        :param filter_expression:
        :param projection_expression:
        :param expression_attribute_names:
        :param count: If true return the count(integer)
        :param params:
        :return:
        """
        items = []
        scan_kwargs = {}
        if count:
            scan_kwargs['Select'] = 'COUNT'
        if filter_expression:
            scan_kwargs['FilterExpression'] = filter_expression
        if projection_expression:
            scan_kwargs['ProjectionExpression'] = projection_expression
        if expression_attribute_names:
            scan_kwargs['ExpressionAttributeNames'] = expression_attribute_names
        for key, value in params:
            scan_kwargs[key] = value
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs['ExclusiveStartKey'] = start_key
                response = self.table.scan(**scan_kwargs)
                if count:
                    return response['Count']
                items.extend(response.get('Items', []))
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None
        except ClientError as err:
            logging.error(
                "Couldn't scan for items. Here's why: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message']
            )
            raise
        return items

    def run_partiql(self, statement, params):
        """
        Run dynamoDB partiql queries
        :param statement:
        :param params:
        :return:
        """
        try:
            output = self.dyn_resource.meta.client.execute_statement(
                Statement=statement, Parameters=params)
            return output
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                logging.error(
                    "Couldn't execute PartiQL '%s' because the table does not exist.",
                    statement)
            else:
                logging.error(
                    "Couldn't execute PartiQL '%s'. Here's why: %s: %s", statement,
                    err.response['Error']['Code'], err.response['Error']['Message'])
            raise

    def write_batch(self, items):
        """
        Write to dynamoDB in batches
        :param items:
        """
        try:
            with self.table.batch_writer() as writer:
                for item in items:
                    writer.put_item(Item=item)
        except ClientError as err:
            logging.error(
                "Couldn't load data into table %s. Here's why: %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
