#-*- coding: utf-8 -*-

""" EOSS catalog system
 functionality for the catalog status endpoint
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import logging
import falcon
import ujson

from api import max_body
from client.services.db_calls import Persistance
from client.services.static_maps import j2_env
from client.services.tools import can_zip_response, compress_body, make_GeoJson
from api_logging import logger


class CatalogStatus(object):
    """
       EOSS catalog class from web API
       """

    def __init__(self):
        self.logger = logging.getLogger('eoss.' + __name__)

    @falcon.before(max_body(64 * 1024))  # max 64kB request size
    def on_get(self, req, resp, sensor):
        logger.info('[GET] /catalog/status/count/%s' % (sensor))
        results = dict()

        minx,maxx, miny, maxy = -180,180,-90,90
        if 'last_days' in req.params:
            last_days = int(req.params['last_days'])
        else:
            last_days = 4

        global_extent = [[miny, minx], [maxy, maxx]]
        res = Persistance().get_observation_coverage(int(sensor), last_days=last_days)
        results['geojson'] = make_GeoJson(res['geojson'], res['attr'])

        content_type = 'text/html'
        results = j2_env.get_template('leaflet_map.html').render(title='Reference object: %s' % sensor, center='[%f, %f]' % (21.5, -102),
                                                            zoomlevel=5, geojson=ujson.dumps(results['geojson']),
                                                                 label_attribute=None,
                                                            extent=ujson.dumps(global_extent))

        if can_zip_response(req.headers):
            resp.set_header('Content-Type', content_type)
            resp.set_header('Content-Encoding', 'gzip')
            if content_type == 'application/json':
                resp.body = compress_body(ujson.dumps(results))
            else:
                resp.body = compress_body(results)
        else:
            resp.set_header('Content-Type', content_type)
            if content_type == 'application/json':
                resp.body = ujson.dumps(results)
            else:
                resp.body = results
        resp.status = falcon.HTTP_200