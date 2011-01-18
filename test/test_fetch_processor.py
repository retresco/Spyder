#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_fetch_processor.py 17-Jan-2011
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
#
#

import os.path
import time
import random

import unittest

import tornado
import tornado.httpserver
import tornado.web

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
from spyder.core.messages import DataMessage
from spyder.core.mgmt import ZmqMgmt
from spyder.core.worker import AsyncZmqWorker
from spyder.core.settings import Settings
from spyder.processor.fetcher import FetchProcessor
from spyder.thrift.gen.ttypes import CrawlUri


class ZmqWorkerIntegrationTestBase(unittest.TestCase):

    def setUp(self):
        
        # create context
        self._context = zmq.Context(1)

        # create the ioloop
        self._ioloop = IOLoop.instance()

        t = time.time()

        mgmt_master_worker = 'inproc://master/worker/coordination/%s' % t
        mgmt_worker_master = 'inproc://worker/master/coordination/%s' % t

        # create sockets
        self._mgmt_sockets = dict()
        self._mgmt_sockets['master_pub'] = self._context.socket(zmq.PUB)
        self._mgmt_sockets['master_pub'].bind(mgmt_master_worker)

        self._mgmt_sockets['worker_sub'] = self._context.socket(zmq.SUB)
        self._mgmt_sockets['worker_sub'].connect(mgmt_master_worker)

        self._mgmt_sockets['worker_pub'] = self._context.socket(zmq.PUB)
        self._mgmt_sockets['worker_pub'].bind(mgmt_worker_master)

        self._mgmt_sockets['master_sub'] = self._context.socket(zmq.SUB)
        self._mgmt_sockets['master_sub'].connect(mgmt_worker_master)
        self._mgmt_sockets['master_sub'].setsockopt(zmq.SUBSCRIBE, "")

        data_master_worker = 'inproc://master/worker/pipeline/%s' % t
        data_worker_master = 'inproc://worker/master/pipeline/%s' % t

        self._worker_sockets = dict()
        self._worker_sockets['master_push'] = self._context.socket(zmq.PUSH)
        self._worker_sockets['master_push'].bind(data_master_worker)

        self._worker_sockets['worker_pull'] = self._context.socket(zmq.PULL)
        self._worker_sockets['worker_pull'].connect(data_master_worker)

        self._worker_sockets['worker_pub'] = self._context.socket(zmq.PUB)
        self._worker_sockets['worker_pub'].bind(data_worker_master)

        self._worker_sockets['master_sub'] = self._context.socket(zmq.SUB)
        self._worker_sockets['master_sub'].connect(data_worker_master)
        self._worker_sockets['master_sub'].setsockopt(zmq.SUBSCRIBE, "")

        self._mgmt = ZmqMgmt( self._mgmt_sockets['worker_sub'],
            self._mgmt_sockets['worker_pub'], ioloop=self._ioloop)
        self._mgmt.start()
        self._mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, self.on_mgmt_end)

    def tearDown(self):

        self._mgmt.stop()

        for socket in self._mgmt_sockets.itervalues():
            socket.close()

        for socket in self._worker_sockets.itervalues():
            socket.close()

        self._context.term()

    def on_mgmt_end(self, _msg):
        self._ioloop.stop()


class SimpleFetcherTestCase(ZmqWorkerIntegrationTestBase):

    port = random.randint(8000, 9000)

    def setUp(self):
        ZmqWorkerIntegrationTestBase.setUp(self)

        path = os.path.join(os.path.dirname(__file__), "static")
        application = tornado.web.Application([
            (r"/(.*)", tornado.web.StaticFileHandler, {"path": path}),
        ])
        self._server = tornado.httpserver.HTTPServer(application, io_loop =
                self._ioloop)
        self._server.listen(self.port)

    def test_fetching_works(self):

        settings = Settings()
        fetcher = FetchProcessor(settings, io_loop=self._ioloop)

        worker = AsyncZmqWorker( self._worker_sockets['worker_pull'],
            self._worker_sockets['worker_pub'],
            self._mgmt,
            fetcher,
            self._ioloop)
        worker.start()

        curi = CrawlUri(url="http://localhost:%s/robots.txt" % self.port,
                host_identifier="127.0.0.1",
                effective_url="http://127.0.0.1:%s/robots.txt" % self.port,
                )
        msg = DataMessage()
        msg.identity = "me"
        msg.curi = curi

        self._worker_sockets['master_push'].send_multipart(msg.serialize())

        def assert_expected_result_and_stop(raw_msg):
            msg = DataMessage(raw_msg)
            robots = open(os.path.join(os.path.dirname(__file__),
                        "static/robots.txt")).read()
            self.assertEqual(robots, msg.curi.content_body)
            self._mgmt_sockets['master_pub'].send_multipart(ZMQ_SPYDER_MGMT_WORKER_QUIT)

        stream = ZMQStream(self._worker_sockets['master_sub'], self._ioloop)
        stream.on_recv(assert_expected_result_and_stop)

        self._ioloop.start()
        stream.stop_on_recv()


if __name__ == '__main__':
    unittest.main()
