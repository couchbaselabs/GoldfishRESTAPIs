import logging
import json
from ..lib.GoldfishRequests import GoldfishRequests

class GoldfishAPI(GoldfishRequests):
    """
        GoldfishAPI class to support Goldfish / Serverless Analytics endpoints and operations.

        Parameters:
            url (str): The base URL of the Goldfish API.
            secret (str): The secret key for authentication.
            access (str): The access key for authentication.
            user (str): The username for authentication.
            pwd (str): The password for authentication.
            TOKEN_FOR_INTERNAL_SUPPORT (str, optional): An optional token for internal support.
    """
    def __init__(self, url, secret, access, user, pwd, TOKEN_FOR_INTERNAL_SUPPORT=None):
        super(GoldfishAPI, self).__init__(url, secret, access)
        self.user = user
        self.pwd = pwd
        self.internal_url = url.replace("https://cloud", "https://", 1)
        self._log = logging.getLogger(__name__)
        self.TOKEN_FOR_INTERNAL_SUPPORT = TOKEN_FOR_INTERNAL_SUPPORT
        self.cbc_api_request_headers = {
            'Authorization': 'Bearer %s' % self.TOKEN_FOR_INTERNAL_SUPPORT,
            'Content-Type': 'application/json'
        }
        self.basic_headers = {
            'Content-Type': 'application/json'
        }

    def get_goldfish_services(self, headers=None):
        """
            Retrieve a list of all Goldfish services.
        """
        url = "{}/internal/support/serverlessanalytics/service".format(self.internal_url)
        if headers:
            resp = self._urllib_request(url, "GET", headers=self.cbc_api_request_headers)
        else:
            resp = self._urllib_request(url, "GET", headers=self.cbc_api_request_headers)
        return resp

    def get_specific_goldfish_service(self, serviceId, headers=None):
        """
            Retrieve a specific Goldfish service.

            Parameters:
                serviceId : ID of the service whose service needs to be retrieved
        """
        url = '{}/internal/support/serverlessanalytics/service/{}'.format(self.internal_url, serviceId)
        if headers:
            resp = self._urllib_request(url, "GET", headers=self.cbc_api_request_headers)
        else:
            resp = self._urllib_request(url, "GET", headers=self.cbc_api_request_headers)
        return resp

    def create_goldfish_instance(self, tenant_id, project_id, name, description, provider, region, nodes, headers=None, **kwargs):
        """
            Create a new Goldfish instance within a specified project.

            Parameters:
                tenant_id (str): The ID of the tenant under which the instance will be created.
                project_id (str): The ID of the project where the instance will be added.
                name (str): The name of the Goldfish instance.
                description (str): A description for the Goldfish instance.
                provider (str): The provider or cloud platform for the instance (e.g., AWS, Azure).
                region (str): The region or location where the instance will be deployed.
                nodes (int): The number of nodes to allocate for the instance.
                headers (dict, optional): Additional headers to include in the request. Default is None.
                **kwargs: Additional keyword arguments to pass to the API request.

        """

        url = "{}/v2/organizations/{}/projects/{}/instance".format(self.internal_url, tenant_id, project_id)
        body = {
            "name": name,
            "description": description,
            "provider": provider,
            "region": region,
            "nodes": nodes
        }
        for key, value in kwargs.items():
            body[key] = value

        if headers:
            resp = self.do_internal_request(url, method="POST",
                                            params=json.dumps(body),
                                            headers=headers)
        else:
            resp = self.do_internal_request(url, method="POST",
                                            params=json.dumps(body),
                                            headers=self.basic_headers)
        return resp

    def get_goldfish_instances(self, tenant_id, project_id, headers=None):
        """
            Retrieve a list of Goldfish instances within a specified project.

            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project for which instances will be retrieved.
                headers (dict, optional): Additional headers to include in the request. Default is None.

        """
        url = "{}/v2/organizations/{}/projects/{}/instance".format(self.internal_url, tenant_id, project_id)
        if headers:
            resp = self.do_internal_request(url, method="GET", headers=self.basic_headers)
        else:
            resp = self.do_internal_request(url, method="GET", headers=self.basic_headers)
        return resp

    def get_specific_goldfish_instance(self, tenant_id, project_id, instance_id, headers=None):
        """
            Retrieve information about a specific Goldfish instance within a project.

            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Goldfish instance to retrieve information for.
                headers (dict, optional): Additional headers to include in the request. Default is None.
        """
        url = "{}/v2/organizations/{}/projects/{}/instance/{}".format(self.internal_url, tenant_id, project_id,
                                                                      instance_id)
        if headers:
            resp = self.do_internal_request(url, method="GET", headers=self.basic_headers)
        else:
            resp = self.do_internal_request(url, method="GET", headers=self.basic_headers)
        return resp

    def delete_goldfish_instance(self, tenant_id, project_id, instance_id, headers=None):
        """
            Delete a specific Goldfish instance within a project.

            Parameters:
                tenant_id (str): The ID of the tenant associated with the project.
                project_id (str): The ID of the project where the instance is located.
                instance_id (str): The ID of the Goldfish instance to delete.
                headers (dict, optional): Additional headers to include in the request. Default is None.
        """

        url = "{}/v2/organizations/{}/projects/{}/instance/{}".format(self.internal_url, tenant_id, project_id,
                                                                      instance_id)
        if headers:
            resp = self.do_internal_request(url, method="DELETE", headers=headers)
        else:
            resp = self.do_internal_request(url, method="DELETE", headers=self.basic_headers)
        return resp
