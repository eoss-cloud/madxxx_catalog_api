import falcon
import ujson

from client.services.tools import can_zip_response, compress_body

struct = {
            'version':'v1',
            'description':'EOSS catalog api'
        }

class RootResource(object):
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.content_type = 'application/json'
        resp.set_header('api-version', struct['version'])

        if can_zip_response(req.headers):
            resp.set_header('Content-Type', 'application/json')
            resp.set_header('Content-Encoding', 'gzip')
            resp.body = compress_body(ujson.dumps(struct))
        else:
            resp.set_header('Content-Type', 'application/json')
            resp.body = ujson.dumps(struct)


    def on_head(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.content_type = 'application/json'
        resp.set_header('api-version', struct['version'])

        if can_zip_response(req.headers):
            resp.set_header('Content-Type', 'application/json')
            resp.set_header('Content-Encoding', 'gzip')
        else:
            resp.set_header('Content-Type', 'application/json')

