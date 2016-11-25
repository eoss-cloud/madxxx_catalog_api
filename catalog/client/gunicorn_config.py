# Default gunicorn configuration

__author__ = "Thilo Wehrmann, Steffen Gebhardt"
__copyright__ = "Copyright 2016, EOSS GmbH"


# direct client communication
bind = "0.0.0.0:8000"
# socket communication for nginx
#bind = "unix:/tmp/gunicorn/gunicorn.sock"
chdir = "/"
loglevel = "DEBUG"
workers = "4"
threads = "4"
worker_class = "gevent"
reload = True
timeout = "240"
errorlog = "-"
accesslog = "-"
name = 'EOSS-api'