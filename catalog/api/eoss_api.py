import logging
import urlparse

import grequests
import requests
from catalog.api import deserialize, serialize
from catalog.utilities import with_metaclass, Singleton, read_OS_var
from passlib.apps import custom_app_context as pwd_context

logger = logging.getLogger(__name__)


@with_metaclass(Singleton)
class Api(object):
    """
    API wrapper to EOSS catalog web API

    A simple wrapper which hides web API and allows the use of pythonic function exeution and plain object use
    """

    def __init__(self, url="http://localhost:8000", user=None, password=None, token=None):
        self.url = url
        self.token = token
        if user is None:
            self.user = read_OS_var('API_USER', mandatory=False)
        else:
            self.user = user
        if user is None:
            self.password = read_OS_var('API_PASSWORD', mandatory=False)
        else:
            self.password = password

    def auth(self, user, password):
        """
        Controls authentification for the requests
        :param user:
        :param password:
        :return:
        """
        logger.info("Using %s for auth" % password)
        payload = {'User-ID': user, "Authorization": pwd_context.encrypt(password)}
        req = requests.get(urlparse.urljoin(self.url, "/auth"), headers=payload)
        if req.status_code == requests.codes.ok:
            self.token = req.json()["token"]
            self.user = user
            self.password = password
            return True
        elif req.status_code == requests.codes.unauthorized:
            raise Exception("Cannot login to system...")

    def __get_resource__(self, url):
        if self.token is None:
            self.auth(self.user, self.password)
        payload = {"Authorization": self.token, "User-ID": self.user}
        url = urlparse.urljoin(self.url, url)
        req = requests.get(url, headers=payload)

        if req.status_code == requests.codes.ok:
            if len(req.text) > 0:
                return req.json()
        elif req.status_code == requests.codes.not_found:
            raise Exception("Cannot find url %s" % urlparse.urljoin(self.url, url))
        elif req.status_code == requests.codes.server_error:
            raise Exception("Server error url %s" % urlparse.urljoin(self.url, url))

        return req.status_code, req.text

    def __put_resource__(self, url, body):
        if self.token is None:
            self.auth(self.user, self.password)
        payload = {"Authorization": self.token, "User-ID": self.user}
        url = urlparse.urljoin(self.url, url)
        req = requests.put(url, headers=payload, json=body)

        if req.status_code in (requests.codes.ok, requests.codes.created):
            if len(req.text) > 0:
                return req.json()
        elif req.status_code == requests.codes.not_found:
            raise Exception("Cannot find url %s" % urlparse.urljoin(self.url, url))
        elif req.status_code == requests.codes.server_error:
            raise Exception("Server error url %s" % urlparse.urljoin(self.url, url))

        return req.text

    def __del_resource__(self, url):
        if self.token is None:
            self.auth(self.user, self.password)
        payload = {"Authorization": self.token, "User-ID": self.user}
        url = urlparse.urljoin(self.url, url)
        req = requests.delete(url, headers=payload)

        if req.status_code in (requests.codes.ok, requests.codes.created):
            if len(req.text) > 0:
                return req.json()

        return req.text

    def __get_resource_list__(self, urls, pool_size=12):
        if self.token is None:
            self.auth(self.user, self.password)
        payload = {"Authorization": self.token, "User-ID": self.user}
        urls = [urlparse.urljoin(self.url, u) for u in urls]

        rs = (grequests.get(u, headers=payload) for u in urls)
        reqs = grequests.map(rs, size=pool_size, gtimeout=2)
        results = list()

        for req in reqs:
            if req != None:
                if req.status_code == requests.codes.ok:
                    results.append(req.json())
                elif req.status_code == requests.codes.not_found:
                    raise Exception("Cannot find url %s" % urlparse.urljoin(self.url, req.url))
                elif req.status_code == requests.codes.server_error:
                    raise Exception("Server error url %s" % urlparse.urljoin(self.url, req.url))

        return results

    def catalog_get(self, entity_id):
        if not type(entity_id) is list:
            status, obj_json = self.__get_resource__("/catalog/{0}.json".format(entity_id))
            if len(obj_json) > 0:
                return deserialize(obj_json, False)
            else:
                return None

    def catalog_put(self, ds_obj):
        obj = serialize(ds_obj, as_json=False)
        req = self.__put_resource__("/catalog/{0}.json".format(ds_obj.entity_id), obj)
        return req

    def catalog_delete(self, entity_id):
        if type(entity_id) is str:
            entity_id = [entity_id]
        for id in entity_id:
            req = self.__del_resource__("/catalog/{0}.json".format(entity_id))
        return req

    def catalog_query(self, **kwargs):
        pass
