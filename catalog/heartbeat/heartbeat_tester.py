
from heartbeat import repeat, heartbeat, trace
import time


@trace
@heartbeat
def testing_function(*arg, **kwargs):
    for iter in range(10):
        print iter,  'sleeping...'
        time.sleep(2)

    return len(arg), len(kwargs)


if __name__ == '__main__':
    print testing_function('test', 1, foo='bar')