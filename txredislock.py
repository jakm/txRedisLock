# -*- coding: utf8 -*-

from logging import getLogger
logger = getLogger(__name__)

from hashlib import sha1
from os      import urandom

from twisted.internet import defer

UNLOCK_SCRIPT = b"""
    if redis.call("get", KEYS[1]) == ARGV[1] then
        redis.call("lpush", KEYS[2], 1)
        redis.call("expire", KEYS[2], 10)
        return redis.call("del", KEYS[1])
    else
        return 0
    end
"""
UNLOCK_SCRIPT_HASH = sha1(UNLOCK_SCRIPT).hexdigest()


class txredisapiWrapper(object):
    def __init__(self, redis_client):
        self._client = redis_client

    def setnx(self, key, value, expire):
        return self._client.set(key, value, only_if_not_exists=True, expire=expire)

    def blpop(self, key, timeout):
        return self._client.blpop([key], timeout=timeout)

    def eval(self, script, script_sha, keys, args):
        return self._client.eval(script, keys, args)


class txredisWrapper(object):
    def __init__(self, redis_client):
        self._client = redis_client
        from txredis.exceptions import NoScript
        self._no_script = NoScript

    def setnx(self, key, value, expire):
        return self._client.send('SET', key, value, 'EX', str(expire), 'NX')


    def blpop(self, key, timeout):
        return self._client.bpop([key], timeout=timeout)

    @defer.inlineCallbacks
    def eval(self, script, script_sha, keys, args):
        try:
            yield self._client.evalsha(script_sha, keys, args)
        except self._no_script:
            yield self._client.eval(script, keys, args)


class Lock(object):
    def __init__(self, redis_client, name, expire=None, api=None):
        self._client = redis_client
        self._expire = expire if expire is None else int(expire)
        self._tok = None
        self._name = 'lock:'+name
        self._signal = 'lock-signal:'+name
        self._wrapper = self._getWrapper(api)

    def _getWrapper(self, api):
        module_name = api or self._client.__class__.__module__.split('.')[0]

        if module_name == 'txredisapi':
            return txredisapiWrapper(self._client)
        elif module_name == 'txredis':
            return txredisWrapper(self._client)
        else:
            raise NotImplementedError("Client module '%s' is not supported" % module_name)

    @defer.inlineCallbacks
    def acquire(self, blocking=True):
        logger.debug("Getting %r ...", self._name)

        if self._tok is None:
            self._tok = urandom(16) if self._expire else 1
        else:
            raise RuntimeError("Already aquired from this Lock instance.")

        busy = True
        while busy:
            busy = not (yield self._wrapper.setnx(self._name, self._tok, self._expire))
            if busy:
                if blocking:
                    yield self._wrapper.blpop(self._signal, self._expire or 0)
                else:
                    logger.debug("Failed to get %r.", self._name)
                    defer.returnValue(False)

        logger.debug("Got lock for %r.", self._name)
        defer.returnValue(True)

    def release(self):
        logger.debug("Releasing %r.", self._name)
        return self._wrapper.eval(UNLOCK_SCRIPT, UNLOCK_SCRIPT_HASH, [self._name, self._signal], [self._tok])
