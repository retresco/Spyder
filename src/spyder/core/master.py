#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# master.py 31-Jan-2011
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# under the License.
# All programs in this directory and
# subdirectories are published under the GNU General Public License as
# described below.
#
#

from Queue import Empty

import zmq
from zmq.eventloop.ioloop import IOLoop, PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_AVAIL
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
from spyder.core.messages import DataMessage, MgmtMessage


class ZmqMaster(object):
    """
    This is the ZMQ Master implementation.

    The master will send :class:`DataMessage` object to the workers and receive
    the processed messages. Unknown links will then be added to the frontier.
    """

    def __init__(self, identity, insocket, outsocket, mgmt, frontier, io_loop):
        """
        Initialize the master.
        """
        self._identity = identity
        self._io_loop = io_loop or IOLoop.instance()

        self._in_stream = ZMQStream(insocket, io_loop)
        self._out_stream = ZMQStream(outsocket, io_loop)

        self._mgmt = mgmt
        self._frontier = frontier

        self._running = False
        self._available_workers = []
        self._current_curis = []

        # check every minute if there are pending URIs to crawl
        self._periodic_callback = PeriodicCallback(self._send_next_uri,
                60*1000, io_loop=io_loop)

    def start(self):
        """
        Start the master.
        """
        self._mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, self._worker_msg)
        self._in_stream.on_recv(self._receive_processed_uri)
        self._running = True

    def stop(self):
        """
        Stop the master gracefully, i.e. stop sending more URIs that should get
        processed.
        """
        self._running = False
        self._periodic_callback.stop()

    def finished(self):
        """
        Return true if all uris have been processed and the master is ready to
        be shut down.
        """
        return not self._running and len(self._current_curis) == 0

    def _worker_msg(self, raw_msg):
        """
        Called when a worker has sent a :class:`MgmtMessage`.
        """
        msg = MgmtMessage(raw_msg)

        if ZMQ_SPYDER_MGMT_WORKER_AVAIL == msg.data:
            self._available_workers.append(msg.identity)

        if ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK == msg.data:
            if msg.identity in self._available_workers:
                self._available_workers.remove(msg.identity)

    def _send_next_uri(self):
        """
        See if there are more uris to process and send them to the workers if
        there are any.

        At this point there is a very small heuristic in order to maximize the
        throughput: try to keep the `self._out_stream._send_queue` full.
        """
        num_workers = len(self._available_workers)

        if self._running and num_workers > 0:
            while self._out_stream._send_queue.qsize() < num_workers * 4:

                try:
                    next_curi = self._frontier.get_next()
                except Empty:
                    # well, frontier has nothing to process right now
                    break

                self._current_curis.append(next_curi.url)
                msg = DataMessage(identity=self._identity, curi=next_curi)
                self._out_stream.send_multipart(msg.serialize())

    def _receive_processed_uri(self, raw_msg):
        """
        Receive and reschedule an URI that has been processed. Additionally add
        all extracted URLs to the frontier.
        """
        msg = DataMessage(raw_msg)

        if 200 <= msg.curi.status_code < 300:
            # we have some kind of success code! yay
            self._frontier.process_successful_crawl(msg.curi)
        elif 300 <= msg.curi.status_code < 400:
            # Some kind of redirect code. This will only happen if the number
            # of redirects exceeds settings.MAX_REDIRECTS
            self._frontier.process_redirect(msg.curi)
        elif 400 <= msg.curi.status_code < 500:
            # some kind of error where the resource could not be found.
            self._frontier.process_not_found(msg.curi)
        elif 500 <= msg.curi.status_code < 600:
            # some kind of server error
            self._frontier.process_server_error(msg.curi)
