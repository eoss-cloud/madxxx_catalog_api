# Default gunicorn configuration
#

bind = "unix:/tmp/gunicorn/gunicorn.sock"
chdir = "/"
loglevel = "INFO"
workers = "1"
threads = "2"
worker_class = "gthread"
reload = True
TIMEOUT = "480"
errorlog = "-"
accesslog = "-"
name = 'EOSS-api'