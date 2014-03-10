from collections import defaultdict
from . import Ballot
from .member import Component


class Acceptor(Component):

    def __init__(self, member):
        super(Acceptor, self).__init__(member)
        self._ballot_num = Ballot(-1, -1, -1)
        self._accepted = defaultdict()  # { (b, s) : p }

    def do_PREPARE(self, scout_id, ballot_num):  # p1a
        if ballot_num > self._ballot_num:
            self._ballot_num = ballot_num

        self.send([scout_id.address], 'PROMISE',  # p1b
                  scout_id=scout_id,
                  acceptor=self.address,
                  ballot_num=self._ballot_num,
                  accepted=self._accepted)

    def do_ACCEPT(self, commander_id, ballot_num, slot, proposal):  # p2a
        if ballot_num >= self._ballot_num:
            self._ballot_num = ballot_num
            self._accepted[(ballot_num, slot)] = proposal

        self.send([commander_id.address], 'ACCEPTED',  # p2b
                  commander_id=commander_id,
                  acceptor=self.address,
                  ballot_num=self._ballot_num)
