#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# test_workerprocess.py 18-Jan-2011
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

import unittest
import time

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
from spyder.core.messages import MgmtMessage
from spyder.core.settings import Settings
from spyder.processor import limiter
from spyder import workerprocess


class WorkerProcessTestCase(unittest.TestCase):

    def test_that_creating_mgmt_works(self):

        ctx = zmq.Context()
        io_loop = IOLoop.instance()

        def stop_looping(_msg):
            io_loop.stop()

        settings = Settings()
        settings.ZEROMQ_MASTER_PUSH = 'inproc://spyder-zmq-master-push'
        settings.ZEROMQ_WORKER_PROC_FETCHER_PULL = \
            settings.ZEROMQ_MASTER_PUSH
        settings.ZEROMQ_MASTER_SUB = 'inproc://spyder-zmq-master-sub'
        settings.ZEROMQ_WORKER_PROC_SCOPER_PUB = \
            settings.ZEROMQ_MASTER_SUB

        settings.ZEROMQ_MGMT_MASTER = 'inproc://spyder-zmq-mgmt-master'
        settings.ZEROMQ_MGMT_WORKER = 'inproc://spyder-zmq-mgmt-worker'

        pubsocket = ctx.socket(zmq.PUB)
        pubsocket.bind(settings.ZEROMQ_MGMT_MASTER)
        pub_stream = ZMQStream(pubsocket, io_loop)

        subsocket = ctx.socket(zmq.SUB)
        subsocket.setsockopt(zmq.SUBSCRIBE, "")
        subsocket.bind(settings.ZEROMQ_MGMT_WORKER)
        sub_stream = ZMQStream(subsocket, io_loop)

        mgmt = workerprocess.create_worker_management(settings, ctx, io_loop)
        mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, stop_looping)
        mgmt.start()

        def assert_quit_message(msg):
            self.assertEqual(ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK, msg.data)
            
        sub_stream.on_recv(assert_quit_message)

        death = MgmtMessage(topic=ZMQ_SPYDER_MGMT_WORKER,
                data=ZMQ_SPYDER_MGMT_WORKER_QUIT)
        pub_stream.send_multipart(death.serialize())

        io_loop.start()

        mgmt._out_stream.close()
        mgmt._in_stream.close()
        mgmt._publisher.close()
        mgmt._subscriber.close()
        pub_stream.close()
        pubsocket.close()
        sub_stream.close()
        subsocket.close()
        ctx.term()


if __name__ == '__main__':
    unittest.main()
