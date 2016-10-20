#-*- coding: utf-8 -*-

""" EOSS catalog system
 client package providing gunicorn entry point
"""

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"

import falcon
from falcon.routing import DefaultRouter
from falcon_cors import CORS

# TODO: Right now everything is allowed; finer specification will be better
cors = CORS(allow_all_origins='*',
            allow_methods_list=['GET', 'POST', 'OPTIONS'],
            allow_all_headers=True,
            allow_credentials_all_origins=True)


class RequireJSON(object):
    """
    Falcon middleware which checks json conditions for each requests
    """

    def process_request(self, req, resp):
        if not req.client_accepts_json:
            raise falcon.HTTPNotAcceptable(
                'This API only supports responses encoded as JSON.',
                href='http://docs.examples.com/api/json')

        if req.method in ('POST', 'PUT'):
            if req.content_type is None:
                raise falcon.HTTPUnsupportedMediaType(
                    'No encoding given. This API only supports requests encoded as JSON.',
                    href='https://github.com/eoss-cloud/madxxx_catalog_api')
            if 'application/json' not in req.content_type:
                raise falcon.HTTPUnsupportedMediaType(
                    'This API only supports requests encoded as JSON.',
                    href='https://github.com/eoss-cloud/madxxx_catalog_api')


class ReverseRouter(DefaultRouter):
    """
    Default router which allows endpoint lookup with their current routing names
    """
    url_map = dict()

    # override add_route to add our map
    def add_route(self, uri_template, method_map, resource, name=None):
        if name:
            self.url_map[name] = uri_template
        DefaultRouter.add_route(self, uri_template, method_map, resource)

    def reverse(self, _name, **kwargs):
        '''
        reverse url
        '''
        assert _name in self.url_map, "url name: %s not in url map" % _name
        url_tmpl = self.url_map.get(_name)
        return url_tmpl.format(**kwargs)
