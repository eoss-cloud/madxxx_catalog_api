#!/usr/bin/env python
# coding: utf8
import cStringIO
import ujson
import logging

import falcon

from api import General_Structure, max_body, serialize, deserialize
from client.services.tools import can_zip_response, compress_body
from model import Context
from model.orm import Catalog_Dataset
from .db_calls import Persistance

logger = logging.getLogger(__name__)

class Dataset:
    def __init__(self):
        self.logger = logging.getLogger('eoss.' + __name__)

    @falcon.before(max_body(64 * 1024))  # max 64kB request size
    def on_delete(self, req, resp, entity_id):
        results = Persistance().delete_dataset(entity_id)
        resp.status = falcon.HTTP_200
        resp.set_header('Content-Type', 'application/json')
        resp.body = ujson.dumps({'action': 'delete', 'status': 'OK', "entity_id": entity_id})

    @falcon.before(max_body(64 * 1024))  # max 64kB request size
    def on_get(self, req, resp, entity_id):
        """Handles GET requests
        Returns list of datasets found with this identifier

        http://localhost:8000/dataset/LC81920272016240LGN00.json

        """

        results_set = Persistance().get_dataset(entity_id)
        results = list()
        values = dict()
        types = dict()
        resultset = list()
        if results_set.count() > 0:
            for ds in results_set:
                for k, v in ds.__dict__.iteritems():
                    if '_' != k[0]:
                        values[k] = v
                        types[k] = type(v)
                x = General_Structure(values, types)
                x.__class__.__name__ = 'Catalog_Dataset'
                resultset.append(x)

            if req.get_header('Serialization') != 'General_Structure':
                # Return simple dict structure for web client
                for obj in resultset:
                    print obj
                    results.append(serialize(obj, as_json=False)['data'])
            else:
                for obj in resultset:
                    results.append(serialize(obj, as_json=False))

            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

        resp.set_header('Content-Type', 'application/json')
        if can_zip_response(req.headers):
            resp.set_header('Content-Type', 'application/json')
            resp.set_header('Content-Encoding', 'gzip')
            resp.body = compress_body(ujson.dumps(results))
        else:
            resp.set_header('Content-Type', 'application/json')
            resp.body = ujson.dumps(results)

    @falcon.before(max_body(64 * 1024))  # max 64kB request size
    def on_put(self, req, resp, entity_id):
        resp.set_header('Content-Type', 'application/json')
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
            ds_exists = Persistance().dataset_exists(obj.entity_id, obj.tile_identifier, obj.acq_time)
            if ds_exists.count() == 0:
                session = Context().getSession()
                c = Catalog_Dataset(**dict(obj))
                session.add(c)
                session.flush()
                resp.status = falcon.HTTP_201
            else:
                c = Catalog_Dataset(**dict(obj))
                logger.warn('Dataset (%s/%s/%s) already exists' % (obj.entity_id, obj.tile_identifier, obj.acq_time))

                description = 'Dataset (%s/%s/%s already exists' % (obj.entity_id, obj.tile_identifier, obj.acq_time)
                raise falcon.HTTPConflict('Dataset already exists', description,
                                          href='http://docs.example.com/auth')

        resp.body = ujson.dumps({'status': 'OK', "new_obj_id": c.id})
        session.commit()
