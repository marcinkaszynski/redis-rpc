#!env python3

import time
import logging
from redis import StrictRedis
from redis_rpc import Server, has_updates


VALUES = {}


def rpc_set(k, v):
    VALUES[k] = v


def rpc_get(k):
    return VALUES[k]


@has_updates
def rpc_countdown(n, add_update):
    for i in range(n, 0, -1):
        time.sleep(0.1)
        add_update(i)
    return 0


REDIS_URI = "redis://localhost:6379/0"


def main():
    logging.basicConfig(level='INFO')

    redis = StrictRedis.from_url(REDIS_URI, socket_timeout=10)
    srv = Server(redis, {
        'set': rpc_set,
        'get': rpc_get,
        'countdown': rpc_countdown,
    }, 'rpc_example')
    srv.quit_on_signals()
    print("Listening on", REDIS_URI, "queues:", srv.queue_names)
    srv.serve()


if __name__ == '__main__':
    main()
