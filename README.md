Minimal RPC-over-Redis
======================

[![Build Status](https://travis-ci.org/Codility/redis-rpc.svg?branch=master)](https://travis-ci.org/Codility/redis-rpc)

Why
---

Because using a queue system as backbone makes it easy to distribute
work between multiple RPC servers, and makes it easy to replace them
when necessary.


Goals
-----

- simple: make it easy to port
- low latency rather than guaranteed execution


Planned use
-----------

server:

    import redis_rpc

    def func1(arg1, arg2):
        return arg1 + arg2

    redis = StrictRedis.from_url(...)
    srv = redis_rpc.Server(redis, {'f1': func1, 'f2': func2, ...}, prefix)
    srv.serve()


client:

    import redis_rpc

    redis = StrictRedis.from_url(...)
    cli = redis_rpc.Client(redis, prefix)
    print(cli.call('f1', arg1=1, arg2=2))


Protocol
--------

Use JSON to encode requests and responses.

A published function is available as a Redis queue.  Send results via
single-use queues so that clients can BLPOP and receive them
immediately.

Queues:

- "{prefix}:{func}:calls"
- "{prefix}:{func}:result:{call-id}"

Call message:

    {"id": "{uuid}",  # random, client-assigned
     "ts": "{time-stamp-iso8601}",
     "kw": {"arg1": "value1",
            "arg2": ["some", "list", "of", "values"]}
    }

Arguments and results may be any JSON-encodable values, including
null/none.  This is not meant to be a framework that completely
ensures interoperability between clients and servers, it's up to the
author of request handlers to make sure exposed API will be usable for
planned clients.

Result message:

    # if successfull
    {"ts": "{time-stamp-iso8601}",
     "res": {result-value}}

    # or, if finished with an error
    {"ts": "{time-stamp-iso8601}",
     "err": "{human-readable description of the error}"}

If the call message contains `"no_response": true`, the result message
is not sent.
