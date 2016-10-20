#-*- coding: utf-8 -*-

""" EOSS catalog system
 functionality for the catalog endpoint
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
import os
from jinja2 import Environment, FileSystemLoader

# Capture our current directory
THIS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
j2_env = Environment(loader=FileSystemLoader(THIS_DIR), trim_blocks=True)
