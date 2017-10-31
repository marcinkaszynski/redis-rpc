#!env python3

import logging
from redis import StrictRedis
from redis_rpc import Server

VALUES = {}


def rpc_set(k, v):
    VALUES[k] = v


def rpc_get(k):
    return VALUES[k]


REDIS_URI = "redis://localhost:6379/0"


def main():
    logging.basicConfig(level='INFO')

    redis = StrictRedis.from_url(REDIS_URI, socket_timeout=10)
    srv = Server(redis, {'set': rpc_set, 'get': rpc_get}, 'rpc_example')
    srv.quit_on_signals()
    print("Listening on", REDIS_URI, "queues:", srv.queue_names)
    srv.serve()


if __name__ == '__main__':
    main()
