from . import Proposal, ViewChange, CATCHUP_INTERVAL, ALPHA, view_primary
from .member import Component


class Replica(Component):
    def __init__(self, member, execute_fn):
        super(Replica, self).__init__(member)
        self._execute_fn = execute_fn
        self._proposals = dict()
        self._view_change_proposal = None

        self._state = None
        self._slot_num = None
        self._next_slot = None
        self._decisions = None
        self._view_id = None
        self._peers = None
        self._peers_down = None
        self._peer_history = None
        self._welcome_peers = None

    def start(self, state, slot_num, decisions, view_id, peers, peer_history):
        self._state = state
        self._slot_num = slot_num
        # next slot num for a proposal (may lead slot_num)
        self._next_slot = slot_num
        self._decisions = decisions
        self._view_id = view_id
        self._peers = peers
        self._peers_down = set()
        self._peer_history = peer_history
        self._welcome_peers = set()

        # TODO: Can be replaced with 'assert slot_num not in self._decisions' if decision value cannot be None
        assert decisions.get(slot_num) is None

        self._catchup()

    # creating proposals

    def do_INVOKE(self, caller, client_id, input_value):
        proposal = Proposal(caller, client_id, input_value)
        if proposal not in self._proposals.viewvalues():
            self._propose(proposal)
        else:
            # It's the only drawback of using dict instead of defaultlist
            slot = next(s for s, p in self._proposals.iteritems() if p == proposal)
            self.logger.info("proposal %s already proposed in slot %d", proposal, slot)

    def do_JOIN(self, requester):
        if requester not in self._peers:
            view_change = ViewChange(self._view_id + 1, tuple(sorted(set(self._peers) | {requester})))
            self._propose(Proposal(None, None, view_change))

    # injecting proposals

    def _propose(self, proposal, slot=None):
        if not slot:
            slot = self._next_slot
            self._next_slot += 1
        self._proposals[slot] = proposal
        # find a leader we think is working, deterministically
        leaders = [view_primary(self._view_id, self._peers)] + list(self._peers)
        leader = next(l for l in leaders if l not in self._peers_down)
        self.logger.info("proposing %s at slot %d to leader %s", proposal, slot, leader)
        self.send([leader], 'PROPOSE', slot=slot, proposal=proposal)

    def _catchup(self):
        """Try to catch up on un-decided slots"""
        if self._slot_num != self._next_slot:
            self.logger.debug("catching up on %d .. %d" % (self._slot_num, self._next_slot - 1))
        for slot in xrange(self._slot_num, self._next_slot):
            # ask peers for information regardless
            self.send(self._peers, 'CATCHUP', slot=slot, sender=self.address)
            # TODO: Can be replaced with 'if slot in self._proposals and slot not in self._decisions'
            # TODO: if proposal value cannot be None
            if self._proposals.get(slot) and not self._decisions.get(slot):
                # resend a proposal we initiated
                self._propose(self._proposals[slot], slot)
            else:
                # make an empty proposal in case nothing has been decided
                self._propose(Proposal(None, None, None), slot)
        self.set_timer(CATCHUP_INTERVAL, self._catchup)

    # view changes

    def on_view_change_event(self, slot, view_id, peers):
        self._peers = peers
        self._peers_down = set()

    def on_peers_down_event(self, down):
        self._peers_down = down
        if not self._peers_down:
            return
        if self._view_change_proposal and self._view_change_proposal not in self._decisions.viewvalues():
            return  # we're still working on a view change that hasn't been decided
        new_peers = tuple(sorted(set(self._peers) - set(down)))
        if len(new_peers) < 3:
            self.logger.info("lost peer(s) %s; need at least three peers", down)
            return
        self.logger.info("lost peer(s) %s; proposing new view", down)
        self._view_change_proposal = Proposal(
            None, None,
            ViewChange(self._view_id + 1, tuple(sorted(set(self._peers) - set(down)))))
        self._propose(self._view_change_proposal)

    # handling decided proposals

    def do_DECISION(self, slot, proposal):
        # TODO: Can be replaced with 'if slot in self._decisions' if decision value cannot be None
        if self._decisions.get(slot) is not None:
            assert self._decisions[slot] == proposal, "slot %d already decided: %r!" % (slot, self._decisions[slot])
            return
        self._decisions[slot] = proposal
        self._next_slot = max(self._next_slot, slot + 1)

        # execute any pending, decided proposals, eliminating duplicates
        while True:
            commit_proposal = self._decisions.get(self._slot_num)
            if not commit_proposal:
                break  # not decided yet
            commit_slot, self._slot_num = self._slot_num, self._slot_num + 1

            # update the view history *before* committing, so the WELCOME message contains
            # an appropriate history
            self._peer_history[commit_slot] = self._peers
            if commit_slot - ALPHA in self._peer_history:
                del self._peer_history[commit_slot - ALPHA]
            exp_peer_history = list(range(commit_slot - ALPHA + 1, commit_slot + 1))

            assert list(sorted(self._peer_history)) == exp_peer_history, "bad peer history %s, exp %s" % (
                self._peer_history, exp_peer_history)

            self.event('update_peer_history', peer_history=self._peer_history)

            self._commit_decided_proposal(commit_slot, commit_proposal)

            # re-propose any of our proposals which have lost in their slot
            our_proposal = self._proposals.get(commit_slot)
            if our_proposal is not None and our_proposal != commit_proposal:
                # TODO: filter out unnecessary proposals - no-ops and outdated
                # view changes (proposal.input.view_id <= self._view_id)
                self._propose(our_proposal)

    on_decision_event = do_DECISION

    def _commit_decided_proposal(self, slot, proposal):
        """Actually commit a proposal that is decided and in sequence"""
        decided_proposals = [p for s, p in self._decisions.iteritems() if s < slot]
        if proposal in decided_proposals:
            self.logger.info("not committing duplicate proposal %r at slot %d", proposal, slot)
            return  # duplicate

        self.logger.info("committing %r at slot %d", proposal, slot)
        self.event('commit', slot=slot, proposal=proposal)

        if isinstance(proposal.input, ViewChange):
            self._commit_view_change(slot, proposal.input)
        elif proposal.caller is not None:
            # perform a client operation
            self._state, output = self._execute_fn(self._state, proposal.input)
            self.send([proposal.caller], 'INVOKED',
                      client_id=proposal.client_id, output=output)

    def _send_welcome(self):
        if self._welcome_peers:
            self.send(list(self._welcome_peers), 'WELCOME',
                      state=self._state,
                      slot_num=self._slot_num,
                      decisions=self._decisions,
                      view_id=self._view_id,
                      peers=self._peers,
                      peer_history=self._peer_history)
            self._welcome_peers = set()

    def _commit_view_change(self, slot, view_change):
        if view_change.view_id == self._view_id + 1:
            self.logger.info("entering view %d with peers %s", view_change.view_id, view_change.peers)
            self._view_id = view_change.view_id

            # now make sure that next_slot is at least slot + ALPHA, so that we don't
            # try to make any new proposals depending on the old view.  The catchup()
            # method will take care of proposing these later.
            self._next_slot = max(slot + ALPHA, self._next_slot)

            # WELCOMEs need to be based on a quiescent state of the replica,
            # not in the middle of a decision-commiting loop, so defer this
            # until the next timer interval.
            self._welcome_peers |= set(view_change.peers) - set(self._peers)
            self.set_timer(0, self._send_welcome)

            if self.address not in view_change.peers:
                self.stop()
                return
            self.event('view_change', slot=slot, view_id=view_change.view_id, peers=view_change.peers)
        else:
            self.logger.info("ignored out-of-sequence view change operation")

    def do_CATCHUP(self, slot, sender):
        # if we have a decision for this proposal, spread the knowledge
        # TODO: Can be replaced with 'if slot in self._decisions' if decision value cannot be None
        if self._decisions.get(slot):
            self.send([sender], 'DECISION',
                      slot=slot, proposal=self._decisions[slot])
