import Queue
import threading
from . import client
from . import member
from . import member_replicated
from . import network


class Listener(member.Component):

    def __init__(self, member, event_queue):
        super(Listener, self).__init__(member)
        self._event_queue = event_queue

    def on_view_change_event(self, slot, view_id, peers):
        self.event_queue.put(("membership_change", peers))


class Ship(object):

    def __init__(self, state_machine, port=10001, peers=None, seed=None):
        peers = peers or ['255.255.255.255-%d' % port]
        self.node = network.Node(port)
        if seed is not None:
            self._cluster_member = member_replicated.ClusterSeed(self._node, seed)
        else:
            self._cluster_member = member_replicated.ClusterMember(self._node, state_machine, peers=peers)
        self._event_queue = Queue.Queue()
        self._current_request = None
        self._listener = Listener(self._cluster_member, self._event_queue)

        self._thread = None

    def start(self):
        def run():
            self.cluster_member.start()
            self.node.run()

        self.thread = threading.Thread(target=run)
        self.thread.setDaemon(1)
        self.thread.start()

    def invoke(self, input_value):
        assert self.current_request is None
        q = Queue.Queue()

        def done(output):
            self._current_request = None
            q.put(output)
        self.current_request = client.Request(self.cluster_member, input_value, done)
        self.current_request.start()
        return q.get()

    def events(self):
        while True:
            evt = self.event_queue.get()
            yield evt
