# Default gunicorn configuration
#

bind = "unix:/tmp/gunicorn/gunicorn.sock"
chdir = "/"
loglevel = "INFO"
workers = "2"
threads = "4"
worker_class = "gevent"
reload = True
TIMEOUT = "480"
errorlog = "-"
accesslog = "-"
name = 'EOSS-api'