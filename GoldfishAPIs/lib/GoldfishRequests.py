import base64
import json
import logging
import pprint
import requests
from threading import Lock

from .GoldfishAuth import GoldfishAuth
from .GoldfishExceptions import (
    MissingAccessKeyError,
    MissingSecretKeyError,
    GenericHTTPError,
    CbcAPIError
)


class GoldfishRequests(object):
    """
        A Python class for interacting with REST APIs.

        This class allows you to make GET, POST, PUT, DELETE requests
        by providing the base URL and optional authentication parameters.

        Parameters:
            url (str): The base URL of the REST API.
            secret (str, optional): The secret key for authentication. Default is None.
            access (str, optional): The access key for authentication. Default is None.
            token (str, optional): The API token for authentication. Default is None.
    """

    def __init__(self, url, secret=None, access=None, token=None):
        # handles http requests - GET , PUT, POST, DELETE
        # Read the values from the environmental variables
        self.API_BASE_URL = url
        self.SECRET = secret
        self.ACCESS = access
        self.bearer_token = token

        self._log = logging.getLogger(__name__)

        # We will re-use the first session we setup to avoid
        # the overhead of creating new sessions for each request
        self.network_session = requests.Session()
        self.jwt = None
        self.lock = Lock()

    def set_logging_level(self, level):
        self._log.setLevel(level)

    def get_authorization_internal(self):
        """
            Generate a JSON Web Token (JWT) and return it as an authorization header.

            This method generates a JWT and returns an authorization header for use in API requests.

            Returns:
                dict: A dictionary containing the Authorization header and Content-Type.
        """
        if self.jwt is None:
            self.lock.acquire()
            if self.jwt is None:
                self._log.debug("refreshing token")
                basic = base64.b64encode('{}:{}'.format(self.user, self.pwd).encode()).decode()
                header = {'Authorization': 'Basic %s' % basic}
                resp = self._urllib_request(
                    "{}/sessions".format(self.internal_url), method="POST",
                    headers=header)
                self.jwt = json.loads(resp.content).get("jwt")
            self.lock.release()
        cbc_api_request_headers = {
            'Authorization': 'Bearer %s' % self.jwt,
            'Content-Type': 'application/json'
        }
        return cbc_api_request_headers

    def do_internal_request(self, url, method, params='', headers={}):
        """
            Make a REST API request with JWT-based authentication.

            This method sends a REST API request with JWT-based authentication.
            It allows you to specify the HTTP method (GET, POST, etc.), the API
            endpoint, optional request data, and query parameters.

            Parameters:
                url (str): The base URL of the REST API.
                method (str): The HTTP method for the request (e.g., 'GET', 'POST', 'PUT').
                params (dict, optional): Query parameters to include in the request URL. Default is ''.
                headers (dict, optional): headers to include in the request URL. Default is {}.
            Returns:
                dict or None: The JSON response from the API or None in case of an error.

        """
        goldfish_header = self.get_authorization_internal()
        goldfish_header.update(headers)
        resp = self._urllib_request(
            url, method, params=params, headers=goldfish_header)
        if resp.status_code == 401:
            self.jwt = None
            return self.do_internal_request(url, method, params)
        return resp

    def goldfish_api_get(self, api_endpoint, params=None, headers=None):
        """
            Make a GET request to the REST API using access and secret keys for authentication.

            Parameters:
                api_endpoint (str): The endpoint to access within the API.
                params (dict, optional): Query parameters to include in the request URL. Default is None.
                headers (dict, optional): headers to include in the request URL. Default is None.

            Returns:
                dict or None: The JSON response from the API or None in case of an error.
        """
        cbc_api_response = None
        self._log.info(api_endpoint)
        try:
            if headers and "Authorization" in headers:
                cbc_api_response = self.network_session.get(
                    self.API_BASE_URL + api_endpoint,
                    params=params,
                    verify=False, headers=headers)
            else:
                cbc_api_response = self.network_session.get(
                    self.API_BASE_URL + api_endpoint,
                    auth=GoldfishAuth(
                        self.SECRET, self.ACCESS, self.bearer_token),
                    params=params,
                    verify=False, headers=headers)
            self._log.info(cbc_api_response.content)

        except requests.exceptions.HTTPError:
            error = pprint.pformat(cbc_api_response.json())
            raise GenericHTTPError(error)

        except MissingAccessKeyError:
            self._log.debug("Missing Access Key environment variable")
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            self._log.debug("Missing Access Key environment variable")
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return (cbc_api_response)

    def goldfish_api_post(self, api_endpoint, request_body, headers=None):
        """
            Make a POST request to the REST API using access and secret keys for authentication.

            Parameters:
                api_endpoint (str): The endpoint to access within the API.
                request_body (dict, optional): Request Body to include in the request API. Default is None.
                headers (dict, optional): headers to include in the request URL. Default is None.

            Returns:
                dict or None: The JSON response from the API or None in case of an error.
        """
        cbc_api_response = None

        self._log.info(api_endpoint)
        self._log.debug("Request body: " + str(request_body))

        try:
            if headers and "Authorization" in headers:
                cbc_api_response = self.network_session.post(
                    self.API_BASE_URL + api_endpoint,
                    json=request_body,
                    verify=False, headers=headers)
            else:
                cbc_api_response = self.network_session.post(
                    self.API_BASE_URL + api_endpoint,
                    json=request_body,
                    auth=GoldfishAuth(
                        self.SECRET, self.ACCESS, self.bearer_token),
                    verify=False, headers=headers)
            self._log.debug(cbc_api_response.content)

        except requests.exceptions.HTTPError:
            error = pprint.pformat(cbc_api_response.json())
            raise GenericHTTPError(error)

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return (cbc_api_response)

    def goldfish_api_put(self, api_endpoint, request_body, headers=None):
        """
            Make a PUT request to the REST API using access and secret keys for authentication.

            Parameters:
                api_endpoint (str): The endpoint to access within the API.
                request_body (dict, optional): Request Body to include in the request API. Default is None.
                headers (dict, optional): headers to include in the request URL. Default is None.

            Returns:
                dict or None: The JSON response from the API or None in case of an error.
        """
        cbc_api_response = None

        self._log.info(api_endpoint)
        self._log.debug("Request body: " + str(request_body))

        try:
            if headers and "Authorization" in headers:
                cbc_api_response = self.network_session.put(
                    self.API_BASE_URL + api_endpoint,
                    json=request_body,
                    verify=False, headers=headers)
            else:
                cbc_api_response = self.network_session.put(
                    self.API_BASE_URL + api_endpoint,
                    json=request_body,
                    auth=GoldfishAuth(
                        self.SECRET, self.ACCESS, self.bearer_token),
                    verify=False, headers=headers)
            self._log.debug(cbc_api_response.content)

        except requests.exceptions.HTTPError:
            error = pprint.pformat(cbc_api_response.json())
            raise GenericHTTPError(error)

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        return (cbc_api_response)

    def goldfish_api_patch(self, api_endpoint, request_body, headers=None):
        """
            Make a PATCH request to the REST API using access and secret keys for authentication.

            Parameters:
                api_endpoint (str): The endpoint to access within the API.
                request_body (dict, optional): Request Body to include in the request API. Default is None.
                headers (dict, optional): headers to include in the request URL. Default is None.

            Returns:
                dict or None: The JSON response from the API or None in case of an error.
        """
        cbc_api_response = None

        self._log.info(api_endpoint)
        self._log.debug("Request body: " + str(request_body))

        try:
            if headers and "Authorization" in headers:
                cbc_api_response = self.network_session.patch(
                    self.API_BASE_URL + api_endpoint,
                    json=request_body,
                    verify=False, headers=headers)
            else:
                cbc_api_response = self.network_session.patch(
                    self.API_BASE_URL + api_endpoint,
                    json=request_body,
                    auth=GoldfishAuth(
                        self.SECRET, self.ACCESS, self.bearer_token),
                    verify=False, headers=headers)
            self._log.debug(cbc_api_response.content)

        except requests.exceptions.HTTPError:
            error = pprint.pformat(cbc_api_response.json())
            raise GenericHTTPError(error)

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        return (cbc_api_response)

    def goldfish_api_del(self, api_endpoint, request_body=None, headers=None):
        """
            Make a DELETE request to the REST API using access and secret keys for authentication.

            Parameters:
                api_endpoint (str): The endpoint to access within the API.
                request_body (dict, optional): Request body to include in the API request. Default is None.
                headers (dict, optional): headers to include in the request URL. Default is None.

            Returns:
                dict or None: The JSON response from the API or None in case of an error.
        """
        cbc_api_response = None

        self._log.info(api_endpoint)
        self._log.debug("Request body: " + str(request_body))

        try:
            if headers and "Authorization" in headers:
                if request_body is None:
                    cbc_api_response = self.network_session.delete(
                        self.API_BASE_URL + api_endpoint,
                        verify=False, headers=headers)
                else:
                    cbc_api_response = self.network_session.delete(
                        self.API_BASE_URL + api_endpoint,
                        json=request_body,
                        verify=False, headers=headers)
            else:
                if request_body is None:
                    cbc_api_response = self.network_session.delete(
                        self.API_BASE_URL + api_endpoint,
                        auth=GoldfishAuth(
                            self.SECRET, self.ACCESS, self.bearer_token),
                        verify=False, headers=headers)
                else:
                    cbc_api_response = self.network_session.delete(
                        self.API_BASE_URL + api_endpoint,
                        json=request_body,
                        auth=GoldfishAuth(
                            self.SECRET, self.ACCESS, self.bearer_token),
                        verify=False, headers=headers)

            self._log.debug(cbc_api_response.content)

        except requests.exceptions.HTTPError:
            error = pprint.pformat(cbc_api_response.json())
            raise GenericHTTPError(error)

        except MissingAccessKeyError:
            print("Missing Access Key environment variable")

        except MissingSecretKeyError:
            print("Missing Access Key environment variable")

        # Grab any other exception and send to our generic exception
        # handler
        except Exception as e:
            raise CbcAPIError(e)

        return (cbc_api_response)

    def _urllib_request(self, api, method='GET', headers=None,
                        params='', timeout=300, verify=False):
        session = requests.Session()
        resp = None
        try:
            if method == "GET":
                resp = session.get(api, params=params, headers=headers,
                                   timeout=timeout, verify=verify)
            elif method == "POST":
                resp = session.post(api, data=params, headers=headers,
                                    timeout=timeout, verify=verify)
            elif method == "DELETE":
                resp = session.delete(api, data=params, headers=headers,
                                      timeout=timeout, verify=verify)
            elif method == "PUT":
                resp = session.put(api, data=params, headers=headers,
                                   timeout=timeout, verify=verify)
            elif method == "PATCH":
                resp = session.patch(api, data=params, headers=headers,
                                     timeout=timeout, verify=verify)
            return resp
        except requests.exceptions.HTTPError as errh:
            self._log.error("HTTP Error {0}".format(errh))
        except requests.exceptions.ConnectionError as errc:
            self._log.error("Error Connecting {0}".format(errc))
        except requests.exceptions.Timeout as errt:
            self._log.error("Timeout Error: {0}".format(errt))
        except requests.exceptions.RequestException as err:
            self._log.error("Something else: {0}".format(err))
