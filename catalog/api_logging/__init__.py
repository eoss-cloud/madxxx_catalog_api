#!/usr/bin/env python
#-*- coding: utf-8 -*-

""" Madxxx processing system
Package for system runtime and production loggers

Copyright EOSS GmbH 2016
"""

import logging
from logging.config import fileConfig
import os
import yaml


from utilities import read_OS_var

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"
__credits__ = ["Thilo Wehrmann", "Steffen Gebhardt"]
__license__ = ""
__version__ = "1.0.0"
__maintainer__ = "Thilo Wehrmann"
__email__ = "twehrmann@eoss.cloud"
__status__ = "Development"


try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


if read_OS_var('LOGGING_CONF', mandatory=False) == None:
    path = os.path.dirname(__file__)
    log_config_file = os.path.join(path, 'logging.yaml')
else:
    log_config_file = read_OS_var('LOGGING_CONF', mandatory=False)

if os.path.exists(log_config_file):
    with open(log_config_file, 'rt') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)
else:
    raise Exception('No valid api_logging configuration defined!')

logger = logging.getLogger('eoss:catalog')
logger.addHandler(NullHandler())
logging.getLogger().addHandler(NullHandler())

# Configure default logger to do nothing
notificator = logging.getLogger('EOSS:notification')
heartbeat_log = logging.getLogger('EOSS:heartbeat')
tracer_log = logging.getLogger('EOSS:tracer')


# 3rd party logger configuration
logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARN)

if __name__ == '__main__':
    logger.info('test')
    logger.warn('warn-test')
    logger.debug('debug-test')