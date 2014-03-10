import sys
from member import Component


class Request(Component):

    # TODO: consider using itertools.count(10**6) instead of iter(xrange(10**6, sys.maxint))
    client_ids = iter(xrange(10**6, sys.maxint))
    RETRANSMIT_TIME = 0.5

    def __init__(self, member, n, callback):
        super(Request, self).__init__(member)
        self._client_id = next(self.client_ids)
        self._n = n  # TODO: rename - use name with semantic meaning
        self._invoke_timer = None
        self._completed_callback = callback

    def start(self):
        self.send([self.address], 'INVOKE', caller=self.address, client_id=self._client_id, input_value=self._n)
        self._invoke_timer = self.set_timer(self.RETRANSMIT_TIME, self.start)

    def do_INVOKED(self, client_id, output):
        if client_id != self._client_id:
            return
        self.logger.debug("received output %r", output)
        self.cancel_timer(self._invoke_timer)
        self._completed_callback(output)
        self.stop()
