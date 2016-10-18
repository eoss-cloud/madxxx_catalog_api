import logging
import falcon
import ujson

from api import max_body
from client.services.static_maps import j2_env


class CatalogStatus(object):
    """
       EOSS catalog class from web API
       """

    def __init__(self):
        self.logger = logging.getLogger('eoss.' + __name__)

    @falcon.before(max_body(64 * 1024))  # max 64kB request size
    def on_get(self, req, resp, sensor):
        results = dict()
        minx,maxx, miny, maxy = -180,180,-90,90

        global_extent = [[miny, minx], [maxy, maxx]]


        content_type = 'text/html'
        results = j2_env.get_template('leaflet_map.html').render(title='Reference object: %s' % sensor, center='[%f, %f]' % (21.5, -102),
                                                            zoomlevel=5, geojson=ujson.dumps(results['geojson']),
                                                            extent=ujson.dumps(global_extent))