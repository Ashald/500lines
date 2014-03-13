from . import HEARTBEAT_GONE_COUNT, HEARTBEAT_INTERVAL
from .member import Component


class Heartbeat(Component):

    def __init__(self, member, clock):
        super(Heartbeat, self).__init__(member)
        self._running = False
        self._last_heard_from = {}
        self._peers = None
        self._clock = clock

    def on_view_change_event(self, slot, view_id, peers):
        self._peers = set(peers)
        self._last_heard_from.update({peer: self._clock() for peer in self._peers})

        if not self._running:
            self._heartbeat()
            self._running = True

    def do_HEARTBEAT(self, sender):
        self._last_heard_from[sender] = self._clock()

    def _heartbeat(self):
        # send heartbeats to other nodes
        self.send(self._peers, 'HEARTBEAT', sender=self.address)

        # determine if any peers are down, and notify if so; note that this
        # notification will occur repeatedly until a view change
        too_old = self._clock() - HEARTBEAT_GONE_COUNT * HEARTBEAT_INTERVAL
        active_peers = set(p for p in self._last_heard_from if self._last_heard_from[p] >= too_old)
        if active_peers != self._peers:
            self.event('peers_down', down=self._peers - active_peers)

        self.set_timer(HEARTBEAT_INTERVAL, self._heartbeat)
