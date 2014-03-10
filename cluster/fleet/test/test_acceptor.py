from .. import acceptor
from .. import Ballot, ScoutId, CommanderId, Proposal
from . import utils


class Tests(utils.ComponentTestCase):

    def setUp(self):
        super(Tests, self).setUp()
        self.ac = acceptor.Acceptor(self.member)

    def assertState(self, ballot_num, accepted):
        self.assertEqual(self.ac._ballot_num, ballot_num)
        self.assertEqual(self.ac._accepted, accepted)

    def test_prepare_new_ballot(self):
        proposal = Proposal('cli', 123, 'INC')
        self.ac._accepted = {(Ballot(7, 19, 19), 33): proposal}
        self.ac._ballot_num = Ballot(7, 10, 10)
        self.node.fake_message('PREPARE',
                               scout_id=ScoutId(address='SC', ballot_num=Ballot(7, 19, 19)),
                               # newer than the acceptor's ballot_num
                               ballot_num=Ballot(7, 19, 19))
        self.assertMessage(['SC'], 'PROMISE',
                           scout_id=ScoutId(address='SC', ballot_num=Ballot(7, 19, 19)),
                           acceptor='F999',
                           # replies with updated ballot_num
                           ballot_num=Ballot(7, 19, 19),
                           # including accepted ballots
                           accepted={(Ballot(7, 19, 19), 33): proposal})
        self.assertState(Ballot(7, 19, 19), {(Ballot(7, 19, 19), 33): proposal})

    def test_prepare_old_ballot(self):
        self.ac._ballot_num = Ballot(7, 10, 10)
        self.node.fake_message('PREPARE',
                               scout_id=ScoutId(address='SC', ballot_num=Ballot(7, 5, 10)),
                               # older than the acceptor's ballot_num
                               ballot_num=Ballot(7, 5, 10))
        self.assertMessage(['SC'], 'PROMISE',
                           scout_id=ScoutId(address='SC', ballot_num=Ballot(7, 5, 10)),
                           acceptor='F999',
                           # replies with newer ballot_num
                           ballot_num=Ballot(7, 10, 10),
                           accepted={})
        self.assertState(Ballot(7, 10, 10), {})

    def test_accept_new_ballot(self):
        proposal = Proposal('cli', 123, 'INC')
        cmd_id = CommanderId(address='CMD', slot=33, proposal=proposal)
        self.ac._ballot_num = Ballot(7, 10, 10)
        self.node.fake_message('ACCEPT',
                               commander_id=cmd_id,
                               ballot_num=Ballot(7, 19, 19),
                               slot=33,
                               proposal=proposal)
        self.assertMessage(['CMD'], 'ACCEPTED',
                           commander_id=cmd_id,
                           acceptor='F999',
                           # replies with updated ballot_num
                           ballot_num=Ballot(7, 19, 19))
        # and state records acceptance of proposal
        self.assertState(Ballot(7, 19, 19), {(Ballot(7, 19, 19), 33): proposal})

    def test_accept_old_ballot(self):
        proposal = Proposal('cli', 123, 'INC')
        cmd_id = CommanderId(address='CMD', slot=33, proposal=proposal)
        self.ac._ballot_num = Ballot(7, 10, 10)
        self.node.fake_message('ACCEPT',
                               commander_id=cmd_id,
                               ballot_num=Ballot(7, 5, 5),
                               slot=33,
                               proposal=proposal)
        self.assertMessage(['CMD'], 'ACCEPTED',
                           commander_id=cmd_id,
                           acceptor='F999',
                           # replies with newer ballot_num
                           ballot_num=Ballot(7, 10, 10))
        # and doesn't accept the proposal
        self.assertState(Ballot(7, 10, 10), {})
