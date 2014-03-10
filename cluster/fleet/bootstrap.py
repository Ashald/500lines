from itertools import cycle
from . import JOIN_RETRANSMIT
from .member import Component


class Bootstrap(Component):

    def __init__(self, member, peers, bootstrapped_callback):
        super(Bootstrap, self).__init__(member)

        self._peers_cycle = cycle(peers)
        self._bootstrap_timer = None
        self._bootstrapped_callback = bootstrapped_callback

    def start(self):
        self._join()

    def _join(self):
        """Try to join the cluster"""
        self.send([next(self._peers_cycle)], 'JOIN', requester=self.address)
        self._bootstrap_timer = self.set_timer(JOIN_RETRANSMIT, self._join)

    def do_WELCOME(self, state, slot_num, decisions, viewid, peers, peer_history):
        self._bootstrapped_callback(state, slot_num, decisions, viewid, peers, peer_history)
        self.event('view_change', viewid=viewid, peers=peers, slot=slot_num)
        self.event('peer_history_update', peer_history=peer_history)

        if self._bootstrap_timer:
            self.cancel_timer(self._bootstrap_timer)

        self.stop()
