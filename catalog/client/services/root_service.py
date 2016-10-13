import falcon

class RootResource(object):
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.content_type = 'application/json'
        resp.body = {
            'version':'v1',
            'description':'EOSS catalog api'
        }
