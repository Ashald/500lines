from . import Ballot, ALPHA, CommanderId, view_primary
from .commander import Commander
from .member import Component
from .scout import Scout


class Leader(Component):
    """
    Issues:
        * there is a '_is_primary' field that's being initialized conditionally. As I understand, the code that
            uses this field cannot be executed earlier that this field will be initialized but it is unclear
            from sources.

            If we add a simple initialization to None in __init__ than it may lead to wrong behaviour due to
            implicit cast of None to False. On the other hand if this field will not be initialized before the code
            that uses it will be referenced it will lead to the AttributeError.

            Proposal:
                change the approach of how this variable is being initialized and used
        * _spawn_commander method has an unused parameter called peers.
            Proposal:
                since this method is being used only internally we may remove that parameter without a risk.

        * pvals parameter in scout_finished
            Proposal:
                should be renamed or comment that describe contents of this parameter should be added

    """

    def __init__(self, member, unique_id, peer_history, commander_cls=Commander, scout_cls=Scout):
        super(Leader, self).__init__(member)
        self._ballot_num = Ballot(-1, 0, unique_id)
        self._active = False
        self._proposals = {}
        self._commander_cls = commander_cls
        self._commanders = {}
        self._scout_cls = scout_cls
        self._scout = None
        self._view_id = -1
        self._peers = None
        self._peer_history = peer_history

    def on_update_peer_history_event(self, peer_history):
        self._peer_history = peer_history

    def on_view_change_event(self, slot, view_id, peers):
        self._view_id = view_id
        self._peers = peers
        self._is_primary = view_primary(view_id, peers) == self.address

        # we are not an active leader in this new view
        if self._scout:
            self._scout.finished(None, None)  # eventually calls preempted
        elif self._active:
            self._preempted(None)
        elif self._is_primary:
            self._spawn_scout()

    def _spawn_scout(self):
        assert not self._scout
        self._ballot_num = Ballot(self._view_id, self._ballot_num.n, self._ballot_num.leader)
        sct = self._scout = self._scout_cls(self.member, self, self._ballot_num, self._peers)
        sct.start()

    def scout_finished(self, adopted, ballot_num, pvals):
        self._scout = None
        if adopted:
            # pvals is a defaultdict of proposal by (ballot num, slot); we need the proposal with
            # highest ballot number for each slot.

            # This *will* work since proposals with lower ballot numbers will be overwritten
            # by proposals with higher ballot numbers. It is guaranteed since we sorting pvals items in ascending order.
            last_by_slot = {s: p for (b, s), p in sorted(pvals.items())}
            for slot_id, proposal in last_by_slot.iteritems():
                self._proposals[slot_id] = proposal

            # re-spawn commanders for any potentially outstanding proposals
            for view_slot in sorted(self._peer_history):
                slot = view_slot + ALPHA
                # TODO: Can be replaced with 'if slot in self._proposals' if proposal value cannot be None
                if self._proposals.get(slot) is not None:
                    self._spawn_commander(self._ballot_num, slot, self._proposals[slot], self._peer_history[view_slot])
            # note that we don't re-spawn commanders here; if there are undecided
            # proposals, the replicas will re-propose
            self.logger.info("leader becoming active")
            self._active = True
        else:
            self._preempted(ballot_num)

    def _preempted(self, ballot_num):
        # ballot_num is None when we are preempted by a view change
        if ballot_num:
            self.logger.info("leader preempted by %s", ballot_num.leader)
        else:
            self.logger.info("leader preempted by view change")
        self._active = False
        self._ballot_num = Ballot(self._view_id, (ballot_num or self._ballot_num).n + 1, self._ballot_num.leader)
        # if we're the primary for this view, re-scout immediately
        if not self._scout and self._is_primary:
            self.logger.info("re-scouting as the primary for this view")
            self._spawn_scout()

    def _spawn_commander(self, ballot_num, slot, proposal, peers):
        peers = self._peer_history[slot - ALPHA]
        commander_id = CommanderId(self.address, slot, self._proposals[slot])
        if commander_id in self._commanders:
            return
        cmd = self._commander_cls(self.member, self, ballot_num, slot, proposal, commander_id, peers)
        self._commanders[commander_id] = cmd
        cmd.start()

    def commander_finished(self, commander_id, ballot_num, preempted):
        del self._commanders[commander_id]
        if preempted:
            self._preempted(ballot_num)

    def do_PROPOSE(self, slot, proposal):
        # TODO: Can be replaced with 'if slot in self._proposals' if proposal value cannot be None
        if self._proposals.get(slot) is None:
            if self._active:
                # find the peers ALPHA slots ago, or ignore if unknown
                if slot - ALPHA not in self._peer_history:
                    self.logger.info("slot %d not in peer history %r", slot - ALPHA, sorted(self._peer_history))
                    return
                self._proposals[slot] = proposal
                self.logger.info("spawning commander for slot %d", slot)
                self._spawn_commander(self._ballot_num, slot, proposal, self._peer_history[slot - ALPHA])
            else:
                if not self._scout:
                    self.logger.info("got PROPOSE when not active - scouting")
                    self._spawn_scout()
                else:
                    self.logger.info("got PROPOSE while scouting; ignored")
        else:
            self.logger.info("got PROPOSE for a slot already being proposed")
