#-*- coding: utf-8 -*-

""" EOSS catalog API module
accessing EOSS catalog functionality via python
direct communication with API right now doesnt need authentification; later API_USER and API_PASSWORD variables are used to provide these credentials
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import operator
import ujson
import urlparse

import grequests
import requests
from shapely.geometry import Polygon
from shapely.wkt import dumps as wkt_dumps

from api import deserialize, serialize, load_json
from general.catalog_exception import ApiException
from general.catalog_logger import notificator
from utilities import with_metaclass, Singleton, read_OS_var
from api_logging import logger


API_VERSION = 'v1'


class ApiOverHttp(object):
    def __init__(self, url, user, password, token):
        logger.info('Using endpoint: %s to connect to EOSS api' % url)
        self.url = url
        self.token = token

        self.headers = {
            'User-Agent': 'EOSS API client %s' % API_VERSION,
            'Accept-Encoding': 'identity, gzip',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Serialization': 'General_Structure'
        }
        try:
            r = requests.head(url, headers=self.headers)
        except requests.exceptions.ConnectionError, e:
            logger.error('Cannot connect to API endpoint %s' % url)
            raise
        if r.status_code == requests.codes.ok:
            if r.headers.get('api-version') != API_VERSION:
                raise ApiException('Different API version %s - needs %s' % (r.headers.get('api-version'), API_VERSION))
            notificator.info('Connection to %s established' % url)
        else:
            raise ApiException('Cannot connect to API endpoint.')

        if user is None:
            self.user = read_OS_var('API_USER', mandatory=False)
        else:
            self.user = user
        if password is None:
            self.password = read_OS_var('API_PASSWORD', mandatory=False)
        else:
            self.password = password

        # self.auth(self.user, self.password)
        # if not self.token is None:
        #    payload = {'User-ID': user, "Authorization": pwd_context.encrypt(password)}
        #    self.headers.update(self.payload)

    def auth(self, user, password):
        """
        Controls authentification for the requests
        :param user:
        :param password:
        :return:
        """
        logger.info("Login as %s for auth" % user)

        req = requests.get(urlparse.urljoin(self.url, "/auth"), headers=self.headers)
        if req.status_code == requests.codes.ok:
            self.token = req.json()["token"]
            self.user = user
            self.password = password
            return True
        elif req.status_code == requests.codes.unauthorized:
            raise ApiException("Cannot login to system...")

    def __get_resource__(self, url):
        url = urlparse.urljoin(self.url, url)
        req = requests.get(url, headers=self.headers)
        logger.info('[%s:%d] %s' % ('GET', req.status_code, url))
        if req.status_code == requests.codes.ok:
            if len(req.text) > 0:
                return req.json()
            else:
                logger.warn('No content received for endpoint %s' % url)
        elif req.status_code == requests.codes.not_found:
            logger.warn("Cannot find url %s" % urlparse.urljoin(self.url, url))
            return None
        elif req.status_code == requests.codes.server_error:
            raise ApiException("Server error url %s" % urlparse.urljoin(self.url, url))
        else:
            logger.warn('Problem occured: %s' % req.text)
            raise ApiException("General error url %s ()" % (urlparse.urljoin(self.url, url)), req.status_code)

    def __put_resource__(self, url, body):
        url = urlparse.urljoin(self.url, url)
        req = requests.put(url, headers=self.headers, json=body)
        logger.info('[%s:%d] %s' % ('PUT', req.status_code, url))
        if req.status_code in (requests.codes.ok, requests.codes.created):
            if len(req.text) > 0:
                return req.json()
        elif req.status_code == requests.codes.not_found:
            logger.error("Cannot find url %s" % urlparse.urljoin(self.url, url))
        elif req.status_code == requests.codes.server_error:
            logger.error(req.text)
            raise ApiException("Server error url %s (%d)" % (urlparse.urljoin(self.url, url), req.status_code))
        else:
            logger.warn('[%d]: %s' % (req.status_code, str(ujson.loads(req.text)['description'])))
            print '##', req
            raise ApiException("Server error url %s (%d)" % (urlparse.urljoin(self.url, url), req.status_code))

        return None

    def __post_resource__(self, url, body):
        url = urlparse.urljoin(self.url, url)
        req = requests.post(url, headers=self.headers, json=body)
        logger.info('[%s:%d] %s' % ('POST', req.status_code, url))
        if req.status_code in (requests.codes.ok, requests.codes.created):
            if len(req.text) > 0:
                return req.json()
        elif req.status_code == requests.codes.not_found:
            raise ApiException("Cannot find url %s" % urlparse.urljoin(self.url, url))
        elif req.status_code == requests.codes.server_error:
            raise ApiException("Server error url %s" % urlparse.urljoin(self.url, url))

        return req.text

    def __del_resource__(self, url):
        url = urlparse.urljoin(self.url, url)
        req = requests.delete(url, headers=self.headers)
        logger.info('[%s:%d] %s' % ('DELETE', req.status_code, url))
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
                    raise ApiException("Cannot find url %s" % urlparse.urljoin(self.url, req.url))
                elif req.status_code == requests.codes.server_error:
                    raise ApiException("Server error url %s" % urlparse.urljoin(self.url, req.url))

        return results


@with_metaclass(Singleton)
class Api(ApiOverHttp):
    """
    API wrapper to EOSS catalog web API

    A simple wrapper which hides web API and allows the use of pythonic function exeution and plain object use
    """

    def __init__(self, url="http://localhost:8000", user=None, password='', token=None):
        ApiOverHttp.__init__(self, url=url, user=None, password='', token=None)

    def get_dataset(self, entity_id):
        """
        Get Dataset from catalog via its entity_id

        :param entity_id: string
        :return: list of datasets
        """
        logger.info('Accesing dataset %s' % entity_id)
        try:
            obj_json = self.__get_resource__("/dataset/{0}.json".format(entity_id))

            return deserialize(obj_json, False)
        except ApiException, e:
            logger.exception('An error occurred during dataset request %s' % entity_id)

    def create_dataset(self, ds_obj):
        """
        Register dataset in catalog
        :param ds_obj: model.plain_models.CatalogDataset object
        :return: json structure with its entitiy_id or None on error
        """
        obj = serialize(ds_obj, as_json=False)
        try:
            req = self.__put_resource__("/dataset/{0}.json".format(ds_obj.entity_id), obj)
            if req != None:
                logger.info('Creating dataset %s' % ds_obj.entity_id)
            return load_json(req)
        except ApiException, e:
            logger.exception('An error occurred during dataset creation [%s]' % str(ds_obj))

    def delete_dataset(self, entity_id):
        """
        Delete (deregister) datasets in catalog
        :param entity_id: string or list of strings
        :return:
        """
        if type(entity_id) is str:
            entity_id = [entity_id]
        for id in entity_id:
            try:
                req = self.__del_resource__("/dataset/{0}.json".format(entity_id))
                logger.info('Deleting dataset %s' % entity_id)
            except ApiException, e:
                logger.exception('An error occurred during deletion of dataset [%s]' % entity_id)
        return req

    def search_dataset(self, aoi, cloud_ratio, date_start, date_stop, platform, full_objects=False):
        """
        Search datasets with different filters
        :param aoi: list of lat/lon coordinates describing and area of interest in EPSG4326
        :param cloud_ratio: float between 0 and 1
        :param date_start: datetime object
        :param date_stop: datetime object
        :param platform: string
        :param full_objects: if True returns CatalogObject instances, otherwise simple dictionary structure
        :return: resultset as list of CatalogObject instances or dicts
        """
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
        try:
            response = self.__post_resource__("catalog/search/result.json", params)
            logger.info('Searching datasets', extra=params)
        except ApiException, e:
            logger.exception('An error occurred during dataset search [%s]' % str(params))

        id_list = set()
        try:
            for obj in response['found_dataset']:
                id_list.add(obj['entity_id'])
        except TypeError, e:
            logger.error(e)
            print response
        if full_objects:
            for id in id_list:
                results.append(reduce(operator.concat, self.get_dataset(id)))
        else:
            results = response['found_dataset']

        notificator.info('Searching datasets [%d] - %s' % (len(results), str(params)))
        return results
