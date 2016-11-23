#-*- coding: utf-8 -*-

""" EOSS catalog system
 functionality for the sensors endpoint
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
import logging

import falcon

from api import General_Structure
from client.services.root_service import struct
from api_logging import logger
from .db_calls import Persistance
from .tools import can_zip_response, compress_body



class Sensors:
    def __init__(self):
        self.logger = logging.getLogger('eoss.' + __name__)
        self.default_status = falcon.HTTP_200
        self.default_content_type = 'application/json'
        self.headers = {'api-version': struct['version'],
                        'Content-Type': self.default_content_type}

    def on_get(self, req, resp, group=None):
        """Handles GET requests
        http://localhost:8000/sensors
        http://localhost:8000/sensors/platform (sensor_level, mission, platform)
        """
        if group:
            logger.info('[GET] /sensors/%s' % group)
        else:
            logger.info('[GET] /sensors/')
        for key, value in self.headers.iteritems():
            resp.set_header(key, value)

        # set default group to sensor_level
        if not group:
            group = 'sensor_level'
        results = list()

        for counter, result in enumerate(Persistance().get_sensors(group)):
            values = {'sensor_name': result[1], 'proc_level': result[2], 'id': counter,
                      'label': '%s' % result[0], 'type': group}
            types = {'sensor_name': str, 'proc_level': str, 'id': int, 'label': str, 'type': str}
            x = General_Structure(values, types)
            x.__class__.__name__ = 'Sensor'
            results.append(x.__dict__)

        if len(results) == 0:
            raise falcon.HTTPNotFound()

        resp.status = self.default_status
        if can_zip_response(req.headers):
            resp.set_header('Content-Encoding', 'gzip')
            resp.body = compress_body(ujson.dumps(results))
        else:
            resp.body = ujson.dumps(results)
