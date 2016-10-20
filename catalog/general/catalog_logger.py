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

logging.getLogger('boto3.resources.action').setLevel(logging.WARNING)
logging.getLogger('botocore.vendored.requests.packages.urllib3.connectionpool').setLevel(logging.WARNING)


heartbeat_log = logging.getLogger('EOSS:heartbeat')
tracer_log = logging.getLogger('EOSS:tracer')

CALL = 41
START = 42
BEATING = 43
STOP = 44
STROKE = 45
HEALTH = 46

logging.addLevelName(CALL, 'CALL')
logging.addLevelName(BEATING, 'BEATING')
logging.addLevelName(BEATING, 'BEATING')
logging.addLevelName(STROKE, 'STROKE')
logging.addLevelName(HEALTH, 'HEALTH')

logging.addLevelName(START, 'START BEAT')
logging.addLevelName(STOP, 'STOP BEAT')

