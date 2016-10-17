import ujson

import falcon

from client.services.tools import can_zip_response, compress_body

struct = {
    'version': 'v1',
    'description': 'EOSS catalog api'
}


class RootResource(object):
    """
    Main entry point to API
    """

    def __init__(self):
        self.default_status = falcon.HTTP_200
        self.default_content_type = 'application/json'
        self.headers = {'api-version': struct['version'],
                        'Content-Type': self.default_content_type}

    def default_response(self, req, resp):
        resp.status = self.default_status
        resp.content_type = self.default_content_type
        for key, value in self.headers.iteritems():
            resp.set_header(key, value)

        if can_zip_response(req.headers):
            resp.set_header('Content-Encoding', 'gzip')
            resp.body = compress_body(ujson.dumps(struct))
        else:
            resp.body = ujson.dumps(struct)
        return resp

    def on_get(self, req, resp):
        resp = self.default_response(req, resp)

    def on_head(self, req, resp):
        resp = self.default_response(req, resp)
