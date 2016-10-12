# Default gunicorn configuration
#

bind = "0.0.0.0:8000"
chdir = "/"
loglevel = "DEBUG"
workers = "2"
threads = "4"
worker_class = "gevent"
reload = True
TIMEOUT = "480"
errorlog = "-"
accesslog = "-"
