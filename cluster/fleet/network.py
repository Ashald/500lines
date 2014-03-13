import cPickle as pickle
import uuid
import logging
import time
import heapq
import socket


NAMESPACE = uuid.UUID('7e0d7720-fa98-4270-94ff-650a2c25f3f0')


def addr_to_tuple(addr):
    parts = addr.split('-')
    return parts[0], int(parts[1])


def tuple_to_addr(addr):
    if addr[0] == '0.0.0.0':
        addr = socket.gethostbyname(socket.gethostname()), addr[1]
    return '%s-%s' % addr


class Node(object):

    def __init__(self, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.bind(('', port))
        self._timers = []
        self._logger = logging.getLogger('node.%s' % self.address)
        self._components = []
        self._running = None

        self.address = tuple_to_addr(self._sock.getsockname())
        self.unique_id = uuid.uuid3(NAMESPACE, self.address).int

    def run(self):
        self._logger.debug("node starting")
        self._running = True
        while self._running:
            if self._timers:
                next_timer = self._timers[0][0]
                if next_timer < time.time():
                    when, do, callback = heapq.heappop(self._timers)
                    if do:
                        callback()
                    continue
            else:
                next_timer = 0
            timeout = max(0.1, next_timer - self.now())
            self._sock.settimeout(timeout)
            try:
                # TODO: 102400 looks like a magic number => should be moved in some constant
                msg, address = self._sock.recvfrom(102400)
            except socket.timeout:
                # TODO: maybe we should at least a little wait that's so process will respond to Ctrl+C
                continue
            action, kwargs = pickle.loads(msg)
            self._logger.debug("received %r with args %r", action, kwargs)
            for comp in self._components[:]:
                fn = getattr(comp, 'do_%s' % action, None)
                if callable(fn):
                    fn(**kwargs)

    def kill(self):
        self._running = False

    def set_timer(self, seconds, callback):
        timer = [self.now() + seconds, True, callback]
        heapq.heappush(self._timers, timer)
        return timer

    def cancel_timer(self, timer):
        timer[1] = False

    def now(self):
        return time.time()

    def send(self, destinations, action, **kwargs):
        self._logger.debug("sending %s with args %s to %s", action, kwargs, destinations)
        pkl = pickle.dumps((action, kwargs))
        for dest in destinations:
            self._sock.sendto(pkl, addr_to_tuple(dest))

    def register(self, component):
        self._components.append(component)

    def unregister(self, component):
        self._components.remove(component)


# TODO: Remove code bellow

# tests

import unittest
import threading


class TestNode(Node):
    foo_called = False
    bar_called = False

    def do_FOO(self, x, y):
        self.foo_called = True
        self.stop()


class NodeTests(unittest.TestCase):

    def test_comm(self):
        sender = Node()
        receiver = TestNode()
        rxthread = threading.Thread(target=receiver.run)
        rxthread.start()
        sender.send([receiver.address], 'FOO', x=10, y=20)
        rxthread.join()
        self.failUnless(receiver.foo_called)

    def test_timeout(self):
        node = TestNode()

        def cb():
            node.bar_called = True
            node.stop()

        node.set_timer(0.01, cb)
        node.run()
        self.failUnless(node.bar_called)

    def test_cancel_timeout(self):
        node = TestNode()

        def fail():
            raise RuntimeError("nooo")

        nonex = node.set_timer(0.01, fail)

        def cb():
            node.bar_called = True
            node.stop()

        node.set_timer(0.02, cb)
        node.cancel_timer(nonex)
        node.run()
        # this just needs to not crash
