import falcon
import json

from client.services.tools import can_zip_response, compress_body

struct = {
            'version':'v1',
            'description':'EOSS catalog api'
        }

class RootResource(object):
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.content_type = 'application/json'

        if can_zip_response(req.headers):
            resp.set_header('Content-Type', 'application/json')
            resp.set_header('Content-Encoding', 'gzip')
            resp.body = compress_body(json.dumps(struct))
        else:
            resp.set_header('Content-Type', 'application/json')
            resp.body = json.dumps(struct)

