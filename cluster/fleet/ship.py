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
        self._event_queue.put(("membership_change", peers))


class Ship(object):

    def __init__(self, state_machine, port=10001, peers=None, seed=None):
        peers = peers or ['255.255.255.255-%d' % port]
        self._node = network.Node(port)
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
            self._cluster_member.start()
            self._node.run()

        self._thread = threading.Thread(target=run)
        self._thread.setDaemon(True)
        self._thread.start()

    def invoke(self, input_value):
        assert self._current_request is None
        q = Queue.Queue()

        def done(output):
            self._current_request = None
            q.put(output)

        self._current_request = client.Request(self._cluster_member, input_value, done)
        self._current_request.start()
        return q.get()

    def events(self):
        while True:
            yield self._event_queue.get()
