# Default gunicorn configuration
#

bind = "unix:/tmp/gunicorn/gunicorn.sock"
chdir = "/"
loglevel = "DEBUG"
workers = "2"
threads = "4"
worker_class = "gevent"
reload = True
timeout = "480"
errorlog = "-"
accesslog = "-"
name = 'EOSS-api'