from . import ACCEPT_RETRANSMIT
from .member import Component


class Commander(Component):

    def __init__(self, member, leader, ballot_num, slot, proposal, commander_id, peers):
        super(Commander, self).__init__(member)
        self._leader = leader
        self._ballot_num = ballot_num
        self._slot = slot
        self._proposal = proposal
        self._commander_id = commander_id
        self._accepted = set()
        self._peers = peers
        self._quorum = len(peers) / 2 + 1
        self._commander_timer = None

    def start(self):
        self.send(set(self.peers) - self.accepted, 'ACCEPT',  # p2a
                  commander_id=self.commander_id,
                  ballot_num=self.ballot_num,
                  slot=self.slot,
                  proposal=self.proposal)
        self.timer = self.set_timer(ACCEPT_RETRANSMIT, self.start)

    def finished(self, ballot_num, preempted):
        self.leader.commander_finished(self.commander_id, ballot_num, preempted)
        if self.timer:
            self.cancel_timer(self.timer)
        self.stop()

    def do_ACCEPTED(self, commander_id, acceptor, ballot_num):  # p2b
        if commander_id != self._commander_id:
            return

        preempted = True
        if ballot_num == self._ballot_num:
            self._accepted.add(acceptor)
            if len(self._accepted) < self._quorum:
                return
            # make sure that this node hears about the decision, otherwise the
            # slot can get "stuck" if all of the DECISION messages get lost, or
            # if this node is not in self.peers
            self.event('decision', slot=self._slot, proposal=self._proposal)
            self.send(self._peers, 'DECISION',
                      slot=self._slot,
                      proposal=self._proposal)
            preempted = False

        self._finished(ballot_num, preempted)
