#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_workerprocess.py 18-Jan-2011
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
import time

import zmq
from zmq.eventloop.ioloop import IOLoop

from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
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
        pubsocket = ctx.socket(zmq.PUB)
        pubsocket.bind(settings.ZEROMQ_MGMT_MASTER)
        time.sleep(1)
        subsocket = ctx.socket(zmq.SUB)
        subsocket.bind(settings.ZEROMQ_MGMT_WORKER)
        subsocket.setsockopt(zmq.SUBSCRIBE, "")
        time.sleep(1)

        mgmt = workerprocess.create_worker_management(settings, ctx, io_loop)
        mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, stop_looping)
        mgmt.start()

        pubsocket.send_multipart(ZMQ_SPYDER_MGMT_WORKER_QUIT)
        time.sleep(1)
        io_loop.start()

        self.assertEqual(ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK,
                subsocket.recv_multipart())

        mgmt._subscriber.close()
        mgmt._publisher.close()
        subsocket.close()
        pubsocket.close()
        ctx.term()


if __name__ == '__main__':
    unittest.main()
