#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# master.py 31-Jan-2011
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
A ZeroMQ master, i.e. the producer of URIs.
"""
import traceback
from Queue import Empty

from zmq.eventloop.ioloop import IOLoop, PeriodicCallback
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_AVAIL
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
from spyder.core.messages import DataMessage
from spyder.core.log import LoggingMixin


class ZmqMaster(object, LoggingMixin):
    """
    This is the ZMQ Master implementation.

    The master will send :class:`DataMessage` object to the workers and receive
    the processed messages. Unknown links will then be added to the frontier.
    """

    def __init__(self, settings, identity, insocket, outsocket, mgmt, frontier,
            log_handler, log_level, io_loop):
        """
        Initialize the master.
        """
        LoggingMixin.__init__(self, log_handler, log_level)
        self._identity = identity
        self._io_loop = io_loop or IOLoop.instance()

        self._in_stream = ZMQStream(insocket, io_loop)
        self._out_stream = ZMQStream(outsocket, io_loop)

        self._mgmt = mgmt
        self._frontier = frontier

        self._running = False
        self._available_workers = []

        # periodically check if there are pending URIs to crawl
        self._periodic_update = PeriodicCallback(self._send_next_uri,
                settings.MASTER_PERIODIC_UPDATE_INTERVAL, io_loop=io_loop)
        # start this periodic callback when you are waiting for the workers to
        # finish
        self._periodic_shutdown = PeriodicCallback(self._shutdown_wait, 500,
                io_loop=io_loop)
        self._shutdown_counter = 0
        self._logger.debug("zmqmaster::initialized")

    def start(self):
        """
        Start the master.
        """
        self._mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, self._worker_msg)
        self._in_stream.on_recv(self._receive_processed_uri)
        self._periodic_update.start()
        self._running = True
        self._logger.debug("zmqmaster::starting...")

    def stop(self):
        """
        Stop the master gracefully, i.e. stop sending more URIs that should get
        processed.
        """
        self._logger.debug("zmqmaster::stopping...")
        self._running = False
        self._periodic_update.stop()

    def shutdown(self):
        """
        Shutdown the master and notify the workers.
        """
        self._logger.debug("zmqmaster::shutdown...")
        self.stop()
        self._mgmt.publish(topic=ZMQ_SPYDER_MGMT_WORKER,
                identity=self._identity, data=ZMQ_SPYDER_MGMT_WORKER_QUIT)
        self._frontier.close()
        self._periodic_shutdown.start()

    def _shutdown_wait(self):
        """
        Callback called from `self._periodic_shutdown` in order to wait for the
        workers to finish.
        """
        self._shutdown_counter += 1
        if 0 == len(self._available_workers) or self._shutdown_counter > 5:
            self._periodic_shutdown.stop()
            self._logger.debug("zmqmaster::bye bye...")
            self._io_loop.stop()

    def close(self):
        """
        Close all open sockets.
        """
        self._in_stream.close()
        self._out_stream.close()

    def finished(self):
        """
        Return true if all uris have been processed and the master is ready to
        be shut down.
        """
        return not self._running

    def _worker_msg(self, msg):
        """
        Called when a worker has sent a :class:`MgmtMessage`.
        """
        if ZMQ_SPYDER_MGMT_WORKER_AVAIL == msg.data:
            self._available_workers.append(msg.identity)
            self._logger.info("zmqmaster::A new worker is available (%s)" %
                    msg.identity)
            self._send_next_uri()

        if ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK == msg.data:
            if msg.identity in self._available_workers:
                self._available_workers.remove(msg.identity)
                self._logger.info("zmqmaster::Removing worker (%s)" %
                        msg.identity)

    def _send_next_uri(self):
        """
        See if there are more uris to process and send them to the workers if
        there are any.

        At this point there is a very small heuristic in order to maximize the
        throughput: try to keep the `self._out_stream._send_queue` full.
        """
        if not self._running:
            self._logger.error("Master is not running, not sending more uris")
            return

        num_workers = len(self._available_workers)

        if self._running and num_workers > 0:
            while self._out_stream._send_queue.qsize() < num_workers * 4:

                try:
                    next_curi = self._frontier.get_next()
                except Empty:
                    # well, frontier has nothing to process right now
                    self._logger.debug("zmqmaster::Nothing to crawl right now")
                    break

                self._logger.info("zmqmaster::Begin crawling next URL (%s)" %
                        next_curi.url)
                msg = DataMessage(identity=self._identity, curi=next_curi)
                self._out_stream.send_multipart(msg.serialize())

    def _receive_processed_uri(self, raw_msg):
        """
        Receive and reschedule an URI that has been processed. Additionally add
        all extracted URLs to the frontier.
        """
        msg = DataMessage(raw_msg)
        self._logger.info("zmqmaster::Crawling URL (%s) finished" %
                msg.curi.url)

        try:
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
        except:
            self._logger.critical("zmqmaster::Uncaught exception in the sink")
            self._logger.critical("zmqmaster::%s" % (traceback.format_exc(),))
            self.stop()

        self._send_next_uri()
