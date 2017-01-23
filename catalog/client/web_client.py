#-*- coding: utf-8 -*-

""" EOSS catalog system
 web interface for the catalog API
 start: gunicorn --config gunicorn_config.py web_client:app
"""

import falcon

from client import ReverseRouter, cors, RequireJSON
from client.services.catalog import CatalogApi
from client.services.catalog_status import CatalogStatus
from client.services.dataset import Dataset, DatasetSearch
from client.services.references import ReferenceSearcher, Reference
from client.services.root_service import RootResource
from client.services.sensors import Sensors
from api_logging import logger


__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Production"


# main app
logger.info('Starting EOSS data catalog API')
my_router = ReverseRouter()
app = falcon.API(middleware=[cors.middleware, RequireJSON()], router=my_router)

# specify URL routes
app.add_route('/', RootResource(my_router), name='root')
app.add_route('/dataset/{entity_id}.json', Dataset(), name='dataset_entity')
app.add_route('/catalog/search/result.{format}', CatalogApi(my_router), name='catalog_result')
app.add_route('/catalog/search/count', CatalogApi(my_router), name='catalog_count')

app.add_route('/reference/search/count', ReferenceSearcher(my_router), name='reference_count')
app.add_route('/reference/{group_id}/{reference_id}.{format}', Reference(my_router), name='reference_entity')

#app.add_route('/reference_types/{group_id}.json', ReferenceTypes(my_router), name='reference_types')

app.add_route('/sensors', Sensors(), name='sensors')
app.add_route('/sensors/{group}', Sensors(), name='sensors')

app.add_route('/catalog/status/count/{sensor}', CatalogStatus(), name='catalog_status')

app.add_route('/dataset/search.json', DatasetSearch(), name='dataset_search')
