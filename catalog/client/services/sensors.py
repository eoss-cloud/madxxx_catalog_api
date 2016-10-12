#!/usr/bin/env python
# coding: utf8
import json
import logging

import falcon

from api import General_Structure
from model.orm import Context
from .db_calls import Persistance
from .tools import can_zip_response, compress_body

logger = logging.getLogger(__name__)


class Sensors:
    def __init__(self):
        self.logger = logging.getLogger('eoss.' + __name__)
        self.session = Context().getSession()

    def on_get(self, req, resp, group=None):
        """Handles GET requests
        http://localhost:8000/sensors
        http://localhost:8000/sensors/platform (sensor_level, mission, platform)
        """

        if group is None:
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
            description = 'Group %s not found.' % group
            raise falcon.HTTPNotFound()
        # print results

        resp.status = falcon.HTTP_200
        if can_zip_response(req.headers):
            resp.set_header('Content-Type', 'application/json')
            resp.set_header('Content-Encoding', 'gzip')
            resp.body = compress_body(json.dumps(results))
        else:
            resp.set_header('Content-Type', 'application/json')
            resp.body = json.dumps(results)
