#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# test_mgmt.py 10-Jan-2011
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#

import unittest

import time

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.messages import MgmtMessage
from spyder.core.mgmt import ZmqMgmt
from spyder.core.constants import *


class ManagementIntegrationTest(unittest.TestCase):


    def setUp(self):
        self._io_loop = IOLoop.instance()
        self._ctx = zmq.Context(1)

        sock = self._ctx.socket(zmq.PUB)
        sock.bind('inproc://master/worker/coordination')
        self._master_pub = ZMQStream(sock, self._io_loop)

        self._worker_sub = self._ctx.socket(zmq.SUB)
        self._worker_sub.setsockopt(zmq.SUBSCRIBE, "")
        self._worker_sub.connect('inproc://master/worker/coordination')

        self._worker_pub = self._ctx.socket(zmq.PUB)
        self._worker_pub.bind( 'inproc://worker/master/coordination' )

        sock = self._ctx.socket(zmq.SUB)
        sock.setsockopt(zmq.SUBSCRIBE, "")
        sock.connect( 'inproc://worker/master/coordination' )
        self._master_sub = ZMQStream(sock, self._io_loop)

        self._topic = ZMQ_SPYDER_MGMT_WORKER + 'testtopic'

    def tearDown(self):
        self._master_pub.close()
        self._worker_sub.close()
        self._worker_pub.close()
        self._master_sub.close()
        self._ctx.term()

    def call_me(self, msg):
        self.assertEqual(self._topic, msg.topic)
        self.assertEqual('test'.encode(), msg.data)
        death = MgmtMessage(topic=ZMQ_SPYDER_MGMT_WORKER,
                data=ZMQ_SPYDER_MGMT_WORKER_QUIT)
        self._master_pub.send_multipart(death.serialize())

    def on_end(self, msg):
        self.assertEqual(ZMQ_SPYDER_MGMT_WORKER_QUIT, msg.data)
        self._io_loop.stop()


    def test_simple_mgmt_session(self):
        
        mgmt = ZmqMgmt(self._worker_sub, self._worker_pub, io_loop=self._io_loop)
        mgmt.start()

        self.assertRaises(ValueError, mgmt.add_callback, "test", "test")

        mgmt.add_callback(self._topic, self.call_me)
        mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, self.on_end)

        test_msg = MgmtMessage(topic=self._topic, data='test'.encode())
        self._master_pub.send_multipart(test_msg.serialize())

        def assert_correct_mgmt_answer(raw_msg):
            msg = MgmtMessage(raw_msg)
            self.assertEqual(ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK, msg.data)
            mgmt.remove_callback(self._topic, self.call_me)
            mgmt.remove_callback(ZMQ_SPYDER_MGMT_WORKER, self.on_end)
            self.assertEqual({}, mgmt._callbacks)

        self._master_sub.on_recv(assert_correct_mgmt_answer)

        self._io_loop.start()


if __name__ == '__main__':
    unittest.main()
