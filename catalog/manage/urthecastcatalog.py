#-*- coding: utf-8 -*-

""" EOSS catalog system
Implementation of urthecast catalog access
https://developers.urthecast.com/sign-in
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
from model.plain_models import Catalog_Dataset
from shapely.geometry import Polygon
from shapely.wkt import dumps as wkt_dumps


class UrthecastCatalog(ICatalog):
    def __init__(self):
        # api_key = os.environ['UC_API_KEY']
        # api_secret = os.environ['UC_API_SECRET']
        self.api_key = 'B47EAFC6559748D4AD62'
        self.api_secret = 'D796AF0410DB4580876C66B72F790192'
        self.url = 'https://api.urthecast.com/v1/archive/scenes'

    def find(self, platforms, aoi, date_start, date_stop, cloud_ratio=0.2):

        url = self.url
        poly = Polygon(aoi)
        geometry = wkt_dumps(poly)

        params = {
            'api_key': self.api_key,
            'api_secret': self.api_secret,
            'cloud_coverage_lte': cloud_ratio,
            'acquired_gte': date_start.isoformat(),
            'acquired_lte': date_stop.isoformat(),
            'geometry_intersects': geometry,
            # 'sensor_platform': 'deimos-1,deimos-2,theia'
            'sensor_platform': ",".join(platforms)
        }

        result = requests.get(url, params=params)

        datasets = set()
        for r in result.json()['payload']:
            ds = Catalog_Dataset()
            ds.entity_id = r['owner_scene_id']
            ds.acq_time = r['acquired']
            ds.sensor = r['sensor_platform']
            # ds.tile_identifier = r['tile_identifier']
            ds.clouds = r['cloud_coverage']
            # ds.level = r['level']
            if int(ds.clouds) > 0:
                ds.daynight = 'day'
            else:
                ds.daynight = 'night'

            datasets.add(ds)

        return datasets

    def register(self, ds):
        raise Exception('Cannot register dataset in repository %s' % self.__class__.__name__)
