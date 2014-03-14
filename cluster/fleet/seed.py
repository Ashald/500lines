from . import ALPHA, JOIN_RETRANSMIT
from .member import Component


class Seed(Component):
    """A component which simply provides an initial state and view.  It waits
    until it has heard JOIN requests from enough nodes to form a cluster, then
    WELCOMEs them all to the same view with the given initial state."""

    def __init__(self, member, initial_state):
        super(Seed, self).__init__(member)
        self._initial_state = initial_state
        self._peers = set()
        self._exit_timer = None

    def do_JOIN(self, requester):
        # three is the minimum membership for a working cluster, so don't
        # respond until then, but don't expand the peers list beyond 3
        if len(self._peers) < 3:
            self._peers.add(requester)
            if len(self._peers) < 3:
                return

        # otherwise, we have a cluster, but don't welcome any nodes not
        # part of that cluster (the cluster can do that itself)
        if requester not in self._peers:
            return

        peer_history = {sl: self._peers for sl in xrange(0, ALPHA)}
        self.send(self._peers, 'WELCOME',
                  state=self._initial_state,
                  slot_num=ALPHA,
                  decisions=[],
                  view_id=0,
                  peers=list(self._peers),
                  peer_history=peer_history.copy())

        # stick around for long enough that we don't hear any new JOINs from
        # the newly formed cluster
        if self._exit_timer:
            self.cancel_timer(self._exit_timer)
        self._exit_timer = self.set_timer(JOIN_RETRANSMIT * 2, self.stop)
