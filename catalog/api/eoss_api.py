import logging
import urlparse

import grequests
import requests
from toolz.curried import operator

from api import deserialize, serialize
from utilities import with_metaclass, Singleton, read_OS_var
from passlib.apps import custom_app_context as pwd_context
from shapely.geometry import Polygon
from shapely.wkt import dumps as wkt_dumps

logger = logging.getLogger(__name__)


class ApiOverHttp(object):
    def __init__(self, url="http://localhost:8000", user=None, password='', token=None):
        self.url = url
        self.token = token

        self.headers = {
            'User-Agent': 'EOSS API 1.0',
            'Accept-Encoding': 'identity, gzip',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Serialization':'General_Structure'
        }

        if user is None:
            self.user = read_OS_var('API_USER', mandatory=False)
        else:
            self.user = user
        if password is None:
            self.password = read_OS_var('API_PASSWORD', mandatory=False)
        else:
            self.password = password

        #self.auth(self.user, self.password)
        #if not self.token is None:
        #    payload = {'User-ID': user, "Authorization": pwd_context.encrypt(password)}
        #    self.headers.update(self.payload)

    def auth(self, user, password):
        """
        Controls authentification for the requests
        :param user:
        :param password:
        :return:
        """
        logger.info("Using %s for auth" % password)

        req = requests.get(urlparse.urljoin(self.url, "/auth"), headers=self.headers)
        if req.status_code == requests.codes.ok:
            self.token = req.json()["token"]
            self.user = user
            self.password = password
            return True
        elif req.status_code == requests.codes.unauthorized:
            raise Exception("Cannot login to system...")

    def __get_resource__(self, url):
        url = urlparse.urljoin(self.url, url)
        req = requests.get(url, headers=self.headers)

        if req.status_code == requests.codes.ok:
            if len(req.text) > 0:
                return req.status_code, req.json()
        elif req.status_code == requests.codes.not_found:
            raise Exception("Cannot find url %s" % urlparse.urljoin(self.url, url))
        elif req.status_code == requests.codes.server_error:
            raise Exception("Server error url %s" % urlparse.urljoin(self.url, url))

        return req.status_code, req.text

    def __put_resource__(self, url, body):
        url = urlparse.urljoin(self.url, url)
        req = requests.put(url, headers=self.headers, json=body)

        if req.status_code in (requests.codes.ok, requests.codes.created):
            if len(req.text) > 0:
                return req.json()
        elif req.status_code == requests.codes.not_found:
            raise Exception("Cannot find url %s" % urlparse.urljoin(self.url, url))
        elif req.status_code == requests.codes.server_error:
            raise Exception("Server error url %s" % urlparse.urljoin(self.url, url))

        return req.text

    def __post_resource__(self, url, body):
        url = urlparse.urljoin(self.url, url)
        req = requests.post(url, headers=self.headers, json=body)

        if req.status_code in (requests.codes.ok, requests.codes.created):
            if len(req.text) > 0:
                return req.json()
        elif req.status_code == requests.codes.not_found:
            raise Exception("Cannot find url %s" % urlparse.urljoin(self.url, url))
        elif req.status_code == requests.codes.server_error:
            raise Exception("Server error url %s" % urlparse.urljoin(self.url, url))

        return req.text

    def __del_resource__(self, url):
        url = urlparse.urljoin(self.url, url)
        req = requests.delete(url, headers=self.headers)

        if req.status_code in (requests.codes.ok, requests.codes.created):
            if len(req.text) > 0:
                return req.json()

        return req.text

    def __get_resource_list__(self, urls, pool_size=12):
        urls = [urlparse.urljoin(self.url, u) for u in urls]

        rs = (grequests.get(u, headers=self.headers) for u in urls)
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

@with_metaclass(Singleton)
class Api(ApiOverHttp):
    """
    API wrapper to EOSS catalog web API

    A simple wrapper which hides web API and allows the use of pythonic function exeution and plain object use
    """

    def __init__(self, url="http://localhost:8000", user=None, password='', token=None):
        ApiOverHttp.__init__(self, url="http://localhost:8000", user=None, password='', token=None)


    def get_dataset(self, entity_id):
        if not type(entity_id) is list:
            status, obj_json = self.__get_resource__("/dataset/{0}.json".format(entity_id))
            if len(obj_json) > 0 and status == requests.codes.ok:
                return deserialize(obj_json, False)
            else:
                return None

    def create_dataset(self, ds_obj):
        obj = serialize(ds_obj, as_json=False)
        req = self.__put_resource__("/dataset/{0}.json".format(ds_obj.entity_id), obj)
        return req

    def delete_dataset(self, entity_id):
        if type(entity_id) is str:
            entity_id = [entity_id]
        for id in entity_id:
            req = self.__del_resource__("/dataset/{0}.json".format(entity_id))
        return req

    def search_dataset(self, aoi, cloud_ratio, date_start, date_stop, platform, full_objects=False, **kwargs):
        geometry = wkt_dumps(Polygon(aoi))

        params = dict()
        params['clouds'] = int(100 * cloud_ratio)
        dates = dict()
        dates['start_date'] = date_start.strftime('%m/%d/%Y')
        dates['end_date'] = date_stop.strftime('%m/%d/%Y')
        params['daterange'] = [dates]
        params['sensors'] = [{'name': platform}]
        params['areas'] = [{'aoi': geometry}]
        results = list()
        response = self.__post_resource__("catalog/search/result.json", params)
        id_list = set()
        for obj in response['found_dataset']:
            id_list.add(obj['entity_id'])
        if full_objects:
            for id in id_list:
                results.append(reduce(operator.concat, self.get_dataset(id)))
        else:
            results = response['found_dataset']
        return results
