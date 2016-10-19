import logging
import pwd
import socket
import sys
import threading
import time
from threading import Thread
import resource

import os
from decorator import decorate
from general.catalog_logger import heartbeat_log, START, BEATING, STOP, STROKE, HEALTH, tracer_log, CALL

import signal
import datetime

from heartbeat.messenger import Queue

DO_LOGGING = False


def signal_term_handler(signal, frame):
    print 'got SIGTERM. Quit programm...'
    time.sleep(2)
    sys.exit(0)


def send_notification(logger, STATUS, message):
    if isinstance(logger, logging.Logger) and DO_LOGGING:
        logger.log(STATUS, message)
    exec_time = datetime.datetime.now()

    if logger.name == 'EOSS:heartbeat':
        struct = dict()
        struct['exec_time'] = exec_time.isoformat()
        struct['status'] =  logging.getLevelName(STATUS)
        struct['logger'] = logger.name
        struct['message'] = message
        q = Queue()
        q.send(struct)


def memory_usage_resource():
    """

    :return: kilobytes
    """
    rusage_denom = 1024.
    if sys.platform == 'darwin':
        # ... it seems that in OSX the output is different units ...
        rusage_denom = rusage_denom * rusage_denom
    mem = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / rusage_denom
    return mem


def _trace(f, *args, **kw):
    prog = os.path.basename(sys.argv[0]) or 'eoss-api'
    username = pwd.getpwuid(os.getuid()).pw_name
    hostname = socket.gethostname().split(".")[0]

    kwstr = ', '.join('%r: %r' % (k, kw[k]) for k in sorted(kw))
    send_notification(tracer_log, CALL, "[%s:%s@%s]: %s (%s, {%s})" % (prog, username, hostname, f.__name__, args, kwstr))
    return f(*args, **kw)


def trace(f):
    return decorate(f, _trace)


def lifesign(location, message, heartbeat_rate=2):
    t = threading.currentThread()
    while getattr(t, "do_run", True):
        time.sleep(heartbeat_rate)
        send_notification(heartbeat_log, BEATING, '(%s) func: <%s> is alive' % (location, message))


def health(location, message, heartbeat_rate=2):
    start_time = time.time()
    t = threading.currentThread()
    send_notification(heartbeat_log, START, 'Starting health monitoring for <%s> (every %d secs.)' % (message, heartbeat_rate))
    while getattr(t, "do_run", True):
        time.sleep(heartbeat_rate)
        l = os.getloadavg()
        m = memory_usage_resource()
        send_notification(heartbeat_log, HEALTH, '(%s) func: <%s>: %f, %f ' % (location, message, l[0], m))


def repeat(times, interval):
    def decorator_func(func):
        def wrapper_func(*args, **kwargs):
            for x in range(times):
                retval = func(*args, **kwargs)
                time.sleep(interval)
                t = threading.currentThread()
            return retval
        return wrapper_func
    return decorator_func


def _heartbeat(func, *args, **kwargs):
    heartbeat_rate_health = 10
    heartbeat_rate_life = 5
    prog = os.path.basename(sys.argv[0]) or 'eoss-api'
    hostname = socket.gethostname().split(".")[0]
    pid = os.getpid()
    lifesign_thread = Thread(name='<Heart>', target=lifesign, args=(
        "%s:%s:%d" % (prog, hostname, pid), func.__name__,), kwargs={'heartbeat_rate': heartbeat_rate_life})

    health_thread = Thread(name='<Condition>', target=health, args=(
        "%s:%s" % (prog, hostname), func.__name__,), kwargs={'heartbeat_rate': heartbeat_rate_health})
    lifesign_thread.daemon = True
    lifesign_thread.start()
    health_thread.daemon = True
    health_thread.start()
    start_time = time.time()
    send_notification(heartbeat_log, START, '[%s] Starting heartbeat monitoring (every %d secs.)' %
                      (lifesign_thread.name, heartbeat_rate_life))
    try:
        retval = func(*args, **kwargs)
        lifesign_thread.do_run = False
        health_thread.do_run = False
        send_notification(heartbeat_log, STOP, "[%s] Stopping heartbeat monitoring successfully. (exec. time: %f secs.)." %
                          (lifesign_thread.name, time.time() - start_time))
    except (KeyboardInterrupt, SystemExit), e:
        send_notification(heartbeat_log, STOP, "[%s] Stopping heartbeat monitoring gracefully (exec. time: %f secs.)." %
                          (lifesign_thread.name, time.time() - start_time))

        time.sleep(1)
        lifesign_thread.do_run = False
        health_thread.do_run = False
        return None
    except Exception, e:
        heartbeat_log.log(STROKE, e)
        send_notification(heartbeat_log, STROKE, e)
        heartbeat_log.exception('Error occured during heartbeat monitored execution')
        send_notification(heartbeat_log, STOP, "[%s] Stopping heartbeat monitoring with error (exec. time: %f secs.)." % (lifesign_thread.name, time.time() - start_time))
        lifesign_thread.do_run = False
        health_thread.do_run = False
        return None

    return retval


def heartbeat(f):
    return decorate(f, _heartbeat)

signal.signal(signal.SIGTERM, signal_term_handler)
signal.signal(signal.SIGINT, signal_term_handler)
