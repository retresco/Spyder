#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_async_worker.py 14-Jan-2011
#
# All programs in this directory and
# subdirectories are published under the GNU General Public License as
# described below.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at
# your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
# USA
#
# Further information about the GNU GPL is available at:
# http://www.gnu.org/copyleft/gpl.html
#
#
import unittest

import time

import zmq
from zmq import Socket
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
from spyder.core.mgmt import ZmqMgmt
from spyder.core.worker import ZmqWorker, AsyncZmqWorker
from spyder.core.messages import DataMessage, MgmtMessage
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

        self._mgmt._stream.flush()
        self._mgmt.stop()

        for socket in self._mgmt_sockets.itervalues():
            socket.close()

        for socket in self._worker_sockets.itervalues():
            socket.close()

        self._context.term()

    def on_mgmt_end(self, _msg):
        self._ioloop.stop()


class AsyncZmqWorkerIntegrationTest(ZmqWorkerIntegrationTestBase):

    def echo_processing(self, data_message, out_socket):
        self._mgmt_sockets['master_pub'].send_multipart(ZMQ_SPYDER_MGMT_WORKER_QUIT)
        out_socket.send_multipart(data_message.serialize())

    def test_that_async_worker_works(self):
        worker = AsyncZmqWorker( self._worker_sockets['worker_pull'],
            self._worker_sockets['worker_pub'],
            self._mgmt,
            self.echo_processing,
            self._ioloop)

        worker.start()

        curi = CrawlUri(url="http://localhost", host_identifier="127.0.0.1")
        msg = DataMessage()
        msg.identity = "me"
        msg.curi = curi

        self._worker_sockets['master_push'].send_multipart(msg.serialize())

        self._ioloop.start()
        worker._stream.flush()

        msg2 = DataMessage(self._worker_sockets['master_sub'].recv_multipart())
        self.assertEquals(msg, msg2)
        self.assertEqual(ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK,
            self._mgmt_sockets['master_sub'].recv_multipart())


if __name__ == '__main__':
    unittest.main()
