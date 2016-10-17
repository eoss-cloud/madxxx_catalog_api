import logging
from logging.config import fileConfig
import os
from utilities import read_OS_var

EOSS_notificator =  'EOSS:notification'

try:  # Python 2.7+
    from logging import NullHandler
except ImportError:
    class NullHandler(logging.Handler):
        def emit(self, record):
            pass


if read_OS_var('LOGGING_CONF', mandatory=False) == None:
    path = os.path.dirname(__file__)
    log_config_file = os.path.join(path, 'logging.ini')
else:
    log_config_file = read_OS_var('LOGGING_CONF', mandatory=False)

fileConfig(log_config_file)
logger = logging.getLogger()
logger.addHandler(NullHandler())
logging.getLogger(__name__).addHandler(NullHandler())

# Configure default logger to do nothing
notificator = logging.getLogger(EOSS_notificator)
