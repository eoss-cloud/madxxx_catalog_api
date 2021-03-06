#-*- coding: utf-8 -*-

""" EOSS catalog system
Implementation of EOSS catalog access
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import ujson
import requests
from manage import ICatalog
from model.plain_models import Catalog_Dataset
from shapely.geometry import Polygon
from shapely.wkt import dumps as wkt_dumps


class EOSSCatalog(ICatalog):
    """
    EOSS Catalog interface
    """
    def __init__(self):
        self.url = 'http://api.eoss.cloud/catalog/search/result.json'
        self.headers = {'content-type': 'application/json'}

    def find(self, platform, aoi, date_start, date_stop, cloud_ratio=1.0):

        session = requests.Session()
        session.auth = (None, None)
        session.stream = True

        headers = {'content-type': 'application/json'}

        poly = Polygon(aoi)
        geometry = wkt_dumps(poly)

        params = dict()
        params['clouds'] = int(100 * cloud_ratio)
        dates = dict()
        dates['start_date'] = date_start.strftime('%m/%d/%Y')
        dates['end_date'] = date_stop.strftime('%m/%d/%Y')
        params['daterange'] = [dates]
        params['sensors'] = [{'name': platform}]

        params['areas'] = [{'aoi': geometry}]

        response = requests.post(self.url, json=ujson.loads(ujson.dumps(params)), auth=session.auth, headers=headers)

        datasets = set()
        if response.status_code == requests.codes.ok:
            result = response.json()['found_dataset']

            for r in result:
                ds = Catalog_Dataset()
                ds.entity_id = r['entity_id']
                ds.acq_time = r['acq_time']
                ds.sensor = r['sensor']
                ds.tile_identifier = r['tile_identifier']
                ds.clouds = r['clouds']
                ds.level = r['level']
                ds.daynight = r['daynight']

                datasets.add(ds)
        else:
            print response.text

        return datasets

    def register(self, ds):
        raise Exception('Registering datasets in EOSSCatalog not implemented!!!')
