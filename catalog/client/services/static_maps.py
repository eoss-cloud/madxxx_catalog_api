#!/usr/bin/env python
# coding: utf8

import falcon
from jinja2 import Environment, FileSystemLoader


class StaticResource(object):
    def on_get(self, req, resp):
        resp.status = falcon.HTTP_200
        resp.content_type = 'text/html'
        with open('index.html', 'r') as f:
            resp.body = f.read()

import os

# Capture our current directory
THIS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
j2_env = Environment(loader=FileSystemLoader(THIS_DIR), trim_blocks=True)


class MapResource(object):
    def on_get(self, req, resp, name):
        resp.status = falcon.HTTP_200
        resp.content_type = 'text/html'
        resp.body = j2_env.get_template('leaflet_map.html').render(title='Reference object: %s' % name, center='[%f, %f]' % (21.5, -102),
                                                              zoomlevel=4)
