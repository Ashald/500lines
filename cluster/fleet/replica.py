from . import Proposal, CATCHUP_INTERVAL
from .member import Component


class Replica(Component):

    def __init__(self, member, execute_fn):
        super(Replica, self).__init__(member)
        self.execute_fn = execute_fn
        self.proposals = {}

    def start(self, state, slot_num, decisions, peers):
        self.state = state
        self.slot_num = slot_num
        # next slot num for a proposal (may lead slot_num)
        self.next_slot = slot_num
        self.decisions = decisions
        self.peers = peers

        # TODO: Can be replaced with 'assert slot_num not in self._decisions'
        # if decision value cannot be None
        assert decisions.get(slot_num) is None

        self.catchup()

    # making proposals

    def do_INVOKE(self, caller, client_id, input_value):
        proposal = Proposal(caller, client_id, input_value)
        if proposal not in self.proposals.viewvalues():
            self.propose(proposal)
        else:
            # It's the only drawback of using dict instead of defaultlist
            slot = next(s for s, p in self.proposals.iteritems()
                        if p == proposal)
            self.logger.info(
                "proposal %s already proposed in slot %d", proposal, slot)

    def propose(self, proposal, slot=None):
        """Send (or resend, if slot is specified) a proposal to the leader"""
        if not slot:
            slot = self.next_slot
            self.next_slot += 1
        self.proposals[slot] = proposal
        # TODO: find a leader we think is working, when re-proposing
        leader = self.peers[0]
        self.logger.info("proposing %s at slot %d to leader %s" %
                         (proposal, slot, leader))
        self.send([leader], 'PROPOSE', slot=slot, proposal=proposal)

    # catching up with the rest of the cluster

    def catchup(self):
        """Try to catch up on un-decided slots"""
        # TODO: some way to gossip about `next_slot` with other replicas
        if self.slot_num != self.next_slot:
            self.logger.debug("catching up on %d .. %d" %
                              (self.slot_num, self.next_slot - 1))
        for slot in xrange(self.slot_num, self.next_slot):
            # ask peers for information regardless
            self.send(self.peers, 'CATCHUP', slot=slot, sender=self.address)
            # TODO: Can be replaced with 'if slot in self._proposals and slot not in self._decisions'
            # TODO: if proposal value cannot be None
            if self.proposals.get(slot) and not self.decisions.get(slot):
                # resend a proposal we initiated
                self.propose(self.proposals[slot], slot)
            else:
                # make an empty proposal in case nothing has been decided
                self.propose(Proposal(None, None, None), slot)
        self.set_timer(CATCHUP_INTERVAL, self.catchup)

    def do_CATCHUP(self, slot, sender):
        # if we have a decision for this proposal, spread the knowledge
        # TODO: Can be replaced with 'if slot in self._decisions' if decision
        # value cannot be None
        if self.decisions.get(slot):
            self.send([sender], 'DECISION',
                      slot=slot, proposal=self.decisions[slot])

    # handling decided proposals

    def do_DECISION(self, slot, proposal):
        # TODO: Can be replaced with 'if slot in self._decisions' if decision
        # value cannot be None
        if self.decisions.get(slot) is not None:
            assert self.decisions[slot] == proposal, \
                "slot %d already decided: %r!" % (
                    slot, self.decisions[slot])
            return
        self.decisions[slot] = proposal
        self.next_slot = max(self.next_slot, slot + 1)

        # execute any pending, decided proposals, eliminating duplicates
        while True:
            commit_proposal = self.decisions.get(self.slot_num)
            if not commit_proposal:
                break  # not decided yet
            commit_slot, self.slot_num = self.slot_num, self.slot_num + 1

            self.commit(commit_slot, commit_proposal)

            # re-propose any of our proposals which have lost in their slot
            our_proposal = self.proposals.get(commit_slot)
            if our_proposal is not None and our_proposal != commit_proposal:
                self.propose(our_proposal)
    on_decision_event = do_DECISION

    def commit(self, slot, proposal):
        """Actually commit a proposal that is decided and in sequence"""
        decided_proposals = [p for s,
                             p in self.decisions.iteritems() if s < slot]
        if proposal in decided_proposals:
            self.logger.info(
                "not committing duplicate proposal %r at slot %d", proposal, slot)
            return  # duplicate

        self.logger.info("committing %r at slot %d" % (proposal, slot))
        self.event('commit', slot=slot, proposal=proposal)

        if proposal.caller is not None:
            # perform a client operation
            self.state, output = self.execute_fn(self.state, proposal.input)
            self.send([proposal.caller], 'INVOKED',
                      client_id=proposal.client_id, output=output)

    # adding new cluster members

    def do_JOIN(self, requester):
        if requester in self.peers:
            self.send([requester], 'WELCOME',
                      state=self.state,
                      slot_num=self.slot_num,
                      decisions=self.decisions,
                      peers=self.peers)
