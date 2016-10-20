#-*- coding: utf-8 -*-

""" EOSS catalog system
Implementation of ESA sentinel1/2 catalog access
(https://scihub.copernicus.eu)
Users need to register at the scihub page to get access to their catalog system. These credentials are set by SENTINEL_USER and SENTINEL_PASSWORD

"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import requests
from manage import ICatalog
from model.plain_models import CopernicusSciHubContainer, S3PublicContainer, Catalog_Dataset
from utilities import read_OS_var
from utilities.web_utils import public_key_exists
from utilities.web_utils import remote_file_exists
from shapely.geometry import Polygon
from shapely.wkt import dumps as wkt_dumps

SENTINEL_S3_HTTP_ZIP_BASEURL = 'http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/zips/'
SENTINEL_S3_HTTP_BASEURL = 'http://sentinel-s2-l1c.s3-website.eu-central-1.amazonaws.com/'
SENTINEL_S3_BUCKET = 'sentinel-s2-l1c'


class SentinelCatalog(ICatalog):
    """
    SentinelCatalog class
    needs OS vars for copernicus service authentification: SENTINEL_USER, SENTINEL_PASSWORD
    """

    sensors = ['sentinel1', 'sentinel2']
    url = 'https://scihub.copernicus.eu/apihub/search?format=%s&rows=%d' % ('json', 15000)

    def __init__(self):
        self.user = read_OS_var('SENTINEL_USER', mandatory=True)
        self.pwd = read_OS_var('SENTINEL_PASSWORD', mandatory=True)

    def find(self, provider, aoi, date_start, date_stop, clouds=None):
        session = requests.Session()
        session.auth = (self.user, self.pwd)
        session.stream = True

        acquisition_date = '(beginPosition:[%s TO %s])' % (
            date_start.strftime('%Y-%m-%dT%H:%M:%SZ'),
            date_stop.strftime('%Y-%m-%dT%H:%M:%SZ')
        )

        poly = Polygon(aoi)
        geometry = wkt_dumps(poly)

        query_area = ' AND (footprint:"Intersects(%s)")' % geometry
        query = ''.join([acquisition_date, query_area])

        response = requests.post(self.url, dict(q=query), auth=session.auth)
        assert response.status_code == requests.codes.ok, 'Connection to copernicus server went wrong [%d]. Please check %s. \\n%s' % \
                                                          (response.status_code, self.url, response.text)
        products = response.json()['feed']['entry']
        datasets = set()

        for p in products:
            ds = Catalog_Dataset()
            ds.entity_id = p['title']
            ds.acq_time = next(x for x in p["date"] if x["name"] == "beginposition")["content"]
            ds.sensor = next(x for x in p["str"] if x["name"] == "platformname")["content"]
            resource_url = next(x for x in p["link"] if len(x.keys()) == 1)["href"]

            if ds.sensor == 'Sentinel-2':
                # ds.tile_identifier = r['tile_identifier']
                ds.clouds = p['double']['content']
                ds.level = next(x for x in p["str"] if x["name"] == "processinglevel")["content"]

                daynight = 'day'
                if next(x for x in p["str"] if x["name"] == "orbitdirection")["content"] != 'DESCENDING':
                    daynight = 'night'
                ds.daynight = daynight

                cop = CopernicusSciHubContainer()
                cop.http = resource_url
                container = cop.to_dict()

                s3 = S3PublicContainer()
                if remote_file_exists(SENTINEL_S3_HTTP_ZIP_BASEURL + ds.entity_id + '.zip'):
                    s3.http = SENTINEL_S3_HTTP_ZIP_BASEURL + ds.entity_id + '.zip'
                if public_key_exists('sentinel-s2-l1c', 'zips/%s.zip' % ds.entity_id):
                    s3.bucket = SENTINEL_S3_BUCKET
                    s3.prefix = 'zips/%s.zip' % ds.entity_id
                if s3.http != None or s3.bucket != None:
                    container.update(s3.to_dict())
                    # print s3.to_dict()
                ds.container = container

                datasets.add(ds)

        return datasets

    def register(self, ds):
        raise Exception('Cannot register dataset in repository %s' % self.url)
