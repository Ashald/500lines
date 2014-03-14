from collections import defaultdict
from . import ScoutId, PREPARE_RETRANSMIT
from .member import Component


class Scout(Component):
    def __init__(self, member, leader, ballot_num, peers):
        super(Scout, self).__init__(member)
        self._leader = leader
        self._scout_id = ScoutId(self.address, ballot_num)
        self._ballot_num = ballot_num
        self._pvals = defaultdict()
        self._accepted = set()
        self._peers = peers
        self._quorum = len(peers) / 2 + 1
        self._retransmit_timer = None

    def start(self):
        self.logger.info("scout starting")
        self._send_prepare()

    def _send_prepare(self):
        self.send(self._peers, 'PREPARE',  # p1a
                  scout_id=self._scout_id,
                  ballot_num=self._ballot_num)
        self._retransmit_timer = self.set_timer(PREPARE_RETRANSMIT, self._send_prepare)

    def finished(self, adopted, ballot_num):
        self.cancel_timer(self._retransmit_timer)
        self.logger.info("finished - adopted" if adopted else "finished - preempted")
        self._leader.scout_finished(adopted, ballot_num, self._pvals)
        if self._retransmit_timer:
            self.cancel_timer(self._retransmit_timer)
        self.stop()

    def do_PROMISE(self, scout_id, acceptor, ballot_num, accepted):  # p1b
        if scout_id != self._scout_id:
            return
        if ballot_num == self._ballot_num:
            self.logger.info("got matching promise; need %d", self._quorum)
            self._pvals.update(accepted)
            self._accepted.add(acceptor)
            if len(self._accepted) >= self._quorum:
                # We're adopted; note that this does *not* mean that no other leader is active.
                # Any such conflicts will be handled by the commanders.
                self.finished(True, ballot_num)
        else:
            # ballot_num > self.ballot_num; responses to other scouts don't
            # result in a call to this method
            self.finished(False, ballot_num)
