#-*- coding: utf-8 -*-

""" EOSS catalog system
Implementation of Planet catalog access
(https://www.planet.com/docs/)
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import urllib
from datetime import datetime
from uuid import uuid4

import geojson
import ujson
import requests
from manage import ICatalog
# from model import Dataset, PlanetContainer
from geoalchemy2.elements import WKTElement
from pytz import UTC
from requests.auth import HTTPBasicAuth
from shapely.geometry import Polygon, shape
from shapely.wkt import dumps as wkt_dumps


class PlanetDataCatalog(ICatalog):
    def __init__(self):
        self.authkey = "7be82d074a904f35b60ceae1807cafab"
        self.url = 'https://api.planet.com/data/v1/quick-search'

    def find(self, provider, aoi, date_start, date_stop, cloud_ratio=0.2):
        # filter for items the overlap with our chosen geometry
        # ["REOrthoTile"] ["PSOrthoTile"]

        poly = Polygon(aoi)
        g2 = geojson.Feature(geometry=poly, properties={})
        geo_json_geometry = g2.geometry

        geometry_filter = {
            "type": "GeometryFilter",
            "field_name": "geometry",
            "config": geo_json_geometry
        }

        # filter images acquired in a certain date range
        date_range_filter = {
            "type": "DateRangeFilter",
            "field_name": "acquired",
            "config": {
                "gte": date_start.isoformat(),
                "lte": date_stop.isoformat()
            }
        }

        # filter any images which are more than 50% clouds
        cloud_cover_filter = {
            "type": "RangeFilter",
            "field_name": "cloud_cover",
            "config": {
                "lte": cloud_ratio
            }
        }

        # create a filter that combines our geo and date filters
        # could also use an "OrFilter"
        query_filter = {
            "type": "AndFilter",
            "config": [geometry_filter, date_range_filter, cloud_cover_filter]
        }

        # Search API request object
        search_endpoint_request = {
            "item_types": provider,
            "filter": query_filter
        }



        result = \
            requests.post(
                'https://api.planet.com/data/v1/quick-search',
                auth=HTTPBasicAuth(self.authkey, ''),
                json=search_endpoint_request)

        scenes_data = result.json()["features"]
        for scene in scenes_data:
            print ujson.dumps(scene)

        return scenes_data

    def register(self, ds):
        raise Exception('Cannot register dataset in repository %s' % self.__class__.__name__)



class PlanetCatalog(ICatalog):
    sensors = ["planetscope", "rapideye", "landsat", "sentinel"]

    def __init__(self):
        self.authkey = "7be82d074a904f35b60ceae1807cafab"
        self.catalog_name = 'grid-utm-25km'
        self.utm_25_url = 'https://api.planet.com/v1/catalogs/{}/items/'.format(self.catalog_name)

    def find(self, provider, aoi, date_start, date_stop, cloud_ratio=0.2, black_fill=0.0):
        '''
        :param provider: string ("planetscope", "rapideye", "landsat", "sentinel")
        :param aoi: geojson polygon
        :param date_start: timestamp
        :param date_stop: timestamp
        :param cloud_ratio: number (0 - 1)
        :return:
        '''

        session = requests.Session()
        session.auth = (self.authkey, '')

        poly = Polygon(aoi)
        geometry = wkt_dumps(poly)

        initial_filters = {
            'catalog::provider': provider,
            'geometry': geometry,
            'catalog::acquired': '[{start}:{end}]'.format(
                start=date_start.isoformat(), end=date_stop.isoformat()),
            'catalog::cloud_cover': '[:{}]'.format(cloud_ratio),
            'catalog::black_fill': '[:{}]'.format(black_fill),
        }

        next_url = self.utm_25_url + '?' + urllib.urlencode(initial_filters)

        datasets = set()
        # Go through each page of results so long as there is a `next` URL returned
        while next_url:
            data = session.get(next_url)
            data.raise_for_status()
            scenes_data = data.json()
            # there will be one entry in 'features' per result
            for s in scenes_data['features']:
                ds = Dataset()
                ds.identifier = s['id']
                ds.uuid = uuid4()
                ds.time_created = s['properties']['catalog::acquired']
                # ds.extent = s['geometry']
                g1 = geojson.loads(ujson.dumps(s['geometry']))
                g2 = shape(g1)
                ds.extent = WKTElement(g2.wkt, srid=4326)
                ds.properties = s['properties']
                datasets.add(ds)

                # TODO add asset url for ordering, activating and downloading

            # Get the URL for the next page of results
            next_url = scenes_data['_links'].get('_next')

        return datasets

    def register(self, ds):
        raise Exception('Cannot register dataset in repository %s' % self.__class__.__name__)






if __name__ == '__main__':
    provider = 'rapideye'

    provider = ["REOrthoTile"]
    max_cloud_ratio = 0.0
    max_black_fill = 0.1
    ag_season_start = datetime(2016, 1, 1, tzinfo=UTC)
    ag_season_end = datetime(2017, 8, 15, tzinfo=UTC)
    aoi_nw = (-91.6959003976, 17.0346713486)
    aoi_se = (-90.4831139337, 15.9802308919)
    aoi_ne = (aoi_se[0], aoi_nw[1])
    aoi_sw = (aoi_nw[0], aoi_se[1])
    aoi = [aoi_nw, aoi_ne, aoi_se, aoi_sw, aoi_nw]

    cat = PlanetDataCatalog()
    res = cat.find(provider, aoi, ag_season_start, ag_season_end, max_cloud_ratio)
    print len(res)
