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

        results = Persistance().get_dataset(entity_id)
        values = dict()
        types = dict()
        resultset = list()
        if results.count() > 0:
            for ds in results:
                for k, v in ds.__dict__.iteritems():
                    if '_' != k[0]:
                        values[k] = v
                        types[k] = type(v)
                x = General_Structure(values, types)
                x.__class__.__name__ = 'Catalog_Dataset'
                resultset.append(x)

            if req.get_header('Serialization') != 'General_Structure':
                serialized_objs = [x['data'] for x in serialize(resultset, as_json=False)]
                results = serialized_objs
            else:
                results = serialize(resultset, as_json=False)

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

        obj = deserialize(output.getvalue())
        output.close()
        session = Context().getSession()

        ds_exists = Persistance().dataset_exists(entity_id, obj.tile_identifier, obj.acq_time)
        if ds_exists.count() == 0:
            c = Catalog_Dataset(**dict(obj))
            session.add(c)
            session.flush()
            resp.status = falcon.HTTP_201
        else:
            c = Catalog_Dataset(**dict(obj))
            print "ERROR: Found dataset:"
            print c
            if ds_exists.count() > 1:
                print 'Error: more than one similar dataset registered'
            for ds in ds_exists.all():
                if not ds == c:
                    print 'Error, datasets are not similar'
                print ds
            print
            description = 'Dataset with id: %s already exists' % entity_id
            raise falcon.HTTPConflict('Dataset already exists', description,
                                      href='http://docs.example.com/auth')

        resp.body = ujson.dumps({'status': 'OK', "new_obj_id": c.id})
        session.commit()
