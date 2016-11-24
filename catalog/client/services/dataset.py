#-*- coding: utf-8 -*-

""" EOSS catalog system
 functionality for the dataset endpoint
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import cStringIO
import ujson
import logging

import falcon
from sqlalchemy.exc import TimeoutError

from api import General_Structure, max_body, serialize, deserialize
from .root_service import struct
from client.services.tools import can_zip_response, compress_body
from model import Context
from model.orm import Catalog_Dataset
from .db_calls import Persistance
from api_logging import logger


class Dataset:
    def __init__(self):
        self.logger = logging.getLogger('eoss.' + __name__)
        self.default_status = falcon.HTTP_200
        self.default_content_type = 'application/json'
        self.headers = {'api-version': struct['version'],
                        'Content-Type': self.default_content_type}

    @falcon.before(max_body(64 * 1024))  # max 64kB request size
    def on_delete(self, req, resp, entity_id):
        logger.info('[DEL] /dataset/%s.json' % (entity_id))
        results = Persistance().delete_dataset(entity_id)
        resp.status = falcon.HTTP_200
        resp.set_header('Content-Type', 'application/json')
        resp.body = ujson.dumps({'action': 'delete', 'status': 'OK', "entity_id": entity_id})

    def _get_dataset_(self, entity_id):
        """
        Query dataset from catalog and convert orm object into serializable object
        :param entity_id:
        :return:
        """
        results = Persistance().get_dataset(entity_id)

        values = dict()
        types = dict()
        result_set = list()
        for ds in results:
            for k, v in ds.__dict__.iteritems():
                if '_' != k[0]:
                    values[k] = v
                    types[k] = type(v)
            x = General_Structure(values, types)
            x.__class__.__name__ = 'Catalog_Dataset'
            result_set.append(x)
        del results
        return result_set

    @falcon.before(max_body(64 * 1024))  # max 64kB request size
    def on_get(self, req, resp, entity_id):
        """Handles GET requests
        Returns list of datasets found with this identifier

        http://localhost:8000/dataset/LC81920272016240LGN00.json

        """
        logger.info('[GET] /dataset/%s.json' % (entity_id))
        for key, value in self.headers.iteritems():
            resp.set_header(key, value)

        results = list()
        result_set = self._get_dataset_(entity_id)
        if len(result_set) == 0:
            resp.status = falcon.HTTP_404
        else:
            if req.get_header('Serialization') != 'General_Structure':
                # Return simple dict structure for web client
                for obj in result_set:
                    print obj
                    results.append(serialize(obj, as_json=False)['data'])
            else:
                for obj in result_set:
                    results.append(serialize(obj, as_json=False))
            resp.status = self.default_status

        if can_zip_response(req.headers):
            resp.set_header('Content-Encoding', 'gzip')
            resp.body = compress_body(ujson.dumps(results))
        else:
            resp.body = ujson.dumps(results)

    @falcon.before(max_body(64 * 1024))  # max 64kB request size
    def on_put(self, req, resp, entity_id):
        #logger.info('[PUT] /dataset/%s.json' % (entity_id))
        for key, value in self.headers.iteritems():
            resp.set_header(key, value)

        output = cStringIO.StringIO()
        while True:
            chunk = req.stream.read(4096)
            if not chunk:
                break
            output.write(chunk)

        json_string = output.getvalue()
        output.close()
        obj_list = deserialize(json_string)

        for obj in obj_list:
            try:
                new_dataset = Persistance().add_dataset(obj)
                if new_dataset:
                    logger.info('Register new dataset: %s' % (obj.entity_id))
                    resp.body = ujson.dumps({'status': 'OK', "new_obj_id": obj.entity_id})
                    resp.status = falcon.HTTP_201
                else:
                    logger.warn('Dataset (%s/%s/%s) already exists' % (obj.entity_id, obj.tile_identifier, obj.acq_time))
                    description = 'Dataset (%s/%s/%s already exists' % (obj.entity_id, obj.tile_identifier, obj.acq_time)
                    raise falcon.HTTPConflict('Dataset already exists', description,
                                              href='http://docs.example.com/auth')
            except TimeoutError, e:
                resp.body = ujson.dumps({'status': 'ERROR', "errorcode": str(e)})
                resp.status = falcon.HTTP_408


