import time
import ujson

from heartbeat.messenger import Queue
import collections


def check_queue():
    q = Queue()
    timeline = collections.defaultdict(list)
    while True:
        if len(q) == 0:
            time_interval = 60
        else:
            time_interval = 0
        for message in q.get_messages():
            msg_body = ujson.loads(message.body)
            if 'logger' in msg_body.keys():
                timeline[msg_body['exec_time']].append((msg_body['logger'],
                                                        msg_body['status'],
                                                        msg_body['message']))
            #message.delete()
        print timeline.keys()
        for t in sorted(timeline.keys()):
            for each_t in timeline[t]:
                logger, status, message = each_t
                if 'BEATING' in status:
                    print t, message

        print
        time.sleep(time_interval)

if __name__ == '__main__':
    check_queue()
