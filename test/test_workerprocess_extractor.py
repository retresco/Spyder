#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_workerprocess_extractor.py 19-Jan-2011
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

import unittest

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import CURI_OPTIONAL_TRUE
from spyder.core.constants import CURI_EXTRACTION_FINISHED
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
from spyder.core.messages import DataMessage, MgmtMessage
from spyder.core.mgmt import ZmqMgmt
from spyder.core.settings import Settings
from spyder.processor import limiter
from spyder.thrift.gen.ttypes import CrawlUri
from spyder import workerprocess


class ZmqTornadoIntegrationTest(unittest.TestCase):

    def setUp(self):

        # create the io_loop
        self._io_loop = IOLoop.instance()

        # and the context
        self._ctx = zmq.Context(1)

        self._settings = Settings()
        self._settings.ZEROMQ_MASTER_PUSH = 'inproc://spyder-zmq-master-push'
        self._settings.ZEROMQ_WORKER_PROC_FETCHER_PULL = \
            self._settings.ZEROMQ_MASTER_PUSH
        self._settings.ZEROMQ_MASTER_SUB = 'inproc://spyder-zmq-master-sub'
        self._settings.ZEROMQ_WORKER_PROC_SCOPER_PUB = \
            self._settings.ZEROMQ_MASTER_SUB

        self._settings.ZEROMQ_MGMT_MASTER = 'inproc://spyder-zmq-mgmt-master'
        self._settings.ZEROMQ_MGMT_WORKER = 'inproc://spyder-zmq-mgmt-worker'

        # setup the mgmt sockets
        self._setup_mgmt_sockets()

        # setup the data sockets
        self._setup_data_servers()

        # setup the management interface
        self._mgmt = ZmqMgmt( self._mgmt_sockets['worker_sub'],
            self._mgmt_sockets['worker_pub'], io_loop=self._io_loop)
        self._mgmt.start()
        self._mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, self.on_mgmt_end)

    def tearDown(self):
        # stop the mgmt
        self._mgmt.stop()
        self._mgmt._in_stream.close()
        self._mgmt._out_stream.close()

        # close all sockets
        for socket in self._mgmt_sockets.itervalues():
            socket.close()
        for socket in self._worker_sockets.itervalues():
            socket.close()

        # terminate the context
        self._ctx.term()

    def _setup_mgmt_sockets(self):

        self._mgmt_sockets = dict()

        # adress for the communication from master to worker(s)
        mgmt_master_worker = self._settings.ZEROMQ_MGMT_MASTER

        # connect the master with the worker
        # the master is a ZMQStream because we are sending msgs from the test
        sock = self._ctx.socket(zmq.PUB)
        sock.bind(mgmt_master_worker)
        self._mgmt_sockets['master_pub'] = ZMQStream(sock, self._io_loop)
        # the worker stream is created inside the ZmqMgmt class
        self._mgmt_sockets['worker_sub'] = self._ctx.socket(zmq.SUB)
        self._mgmt_sockets['worker_sub'].setsockopt(zmq.SUBSCRIBE, "")
        self._mgmt_sockets['worker_sub'].connect(mgmt_master_worker)

        # adress for the communication from worker(s) to master
        mgmt_worker_master = self._settings.ZEROMQ_MGMT_WORKER

        # connect the worker with the master
        self._mgmt_sockets['worker_pub'] = self._ctx.socket(zmq.PUB)
        self._mgmt_sockets['worker_pub'].bind(mgmt_worker_master)
        sock = self._ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, "")
        sock.connect(mgmt_worker_master)
        self._mgmt_sockets['master_sub'] = ZMQStream(sock, self._io_loop)

    def _setup_data_servers(self):

        self._worker_sockets = dict()

        # address for master -> worker communication
        data_master_worker = self._settings.ZEROMQ_WORKER_PROC_EXTRACTOR_PULL

        sock = self._ctx.socket(zmq.PUSH)
        sock.bind(data_master_worker)
        self._worker_sockets['master_push'] = ZMQStream(sock, self._io_loop)

    def _setup_data_client(self):
        # address for worker -> master communication
        data_worker_master = self._settings.ZEROMQ_WORKER_PROC_EXTRACTOR_PUSH

        sock = self._ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, "")
        sock.connect(data_worker_master)
        self._worker_sockets['master_sub'] = ZMQStream(sock, self._io_loop)

    def on_mgmt_end(self, _msg):
        self._io_loop.stop()


class WorkerExtractorTestCase(ZmqTornadoIntegrationTest):

    def test_that_creating_extractor_works(self):

        self._settings.SPYDER_EXTRACTOR_PIPELINE = ['spyder.processor.limiter',]

        extractor = workerprocess.create_worker_extractor(self._settings,
                self._mgmt, self._ctx, self._io_loop)
        extractor.start()

        self._setup_data_client()

        curi = CrawlUri(url="http://localhost:80/robots.txt",
                effective_url="http://127.0.0.1:%s/robots.txt",
                optional_vars=dict(),
                )
        msg = DataMessage()
        msg.identity = "me"
        msg.curi = curi

        def assert_expected_result_and_stop(raw_msg):
            msg2 = DataMessage(raw_msg)
            self.assertEqual(CURI_OPTIONAL_TRUE,
                    msg2.curi.optional_vars[CURI_EXTRACTION_FINISHED])
            death = MgmtMessage(topic=ZMQ_SPYDER_MGMT_WORKER,
                    data=ZMQ_SPYDER_MGMT_WORKER_QUIT)
            self._mgmt_sockets['master_pub'].send_multipart(death.serialize())

        self._worker_sockets['master_sub'].on_recv(assert_expected_result_and_stop)

        def assert_correct_mgmt_message(raw_msg):
            self.assertEqual(ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK, raw_msg)

        self._mgmt_sockets['master_sub'].on_recv(assert_correct_mgmt_message)

        self._worker_sockets['master_push'].send_multipart(msg.serialize())

        self._io_loop.start()

        extractor._out_stream.close()
        extractor._in_stream.close()


if __name__ == '__main__':
    unittest.main()
