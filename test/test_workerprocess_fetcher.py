#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_workerprocess_fetcher.py 19-Jan-2011
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
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import CURI_OPTIONAL_TRUE
from spyder.core.constants import CURI_EXTRACTION_FINISHED
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.settings import Settings
from spyder.core.worker import AsyncZmqWorker
from spyder import workerprocess

from spyder.processor.fetcher import FetchProcessor

class WorkerExtractorTestCase(unittest.TestCase):

    def test_that_creating_fetcher_works(self):
        ctx = zmq.Context()
        io_loop = IOLoop.instance()

        def stop_looping(_msg):
            io_loop.stop()

        settings = Settings()

        pubsocket = ctx.socket(zmq.PUB)
        pubsocket.bind(settings.ZEROMQ_MGMT_MASTER)
        subsocket = ctx.socket(zmq.SUB)
        subsocket.bind(settings.ZEROMQ_MGMT_WORKER)
        subsocket.setsockopt(zmq.SUBSCRIBE, "")

        # we need to sleep here since zmq seems to take a breath creating the
        # sockets. Otherwise the test will hang forever
        time.sleep(.5)

        mgmt = workerprocess.create_worker_management(settings, ctx, io_loop)
        mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, stop_looping)
        mgmt.start()

        master_push = ctx.socket(zmq.PUSH)
        master_push.bind(settings.ZEROMQ_MASTER_PUSH)

        fetcher = workerprocess.create_worker_fetcher(settings, mgmt, ctx,
                io_loop)

        self.assertTrue(isinstance(fetcher._processing, FetchProcessor))
        self.assertTrue(isinstance(fetcher, AsyncZmqWorker))

        mgmt._subscriber.close()
        mgmt._publisher.close()
        subsocket.close()
        pubsocket.close()

        fetcher._insocket.close()
        fetcher._outsocket.close()
        master_push.close()
        ctx.term()


if __name__ == '__main__':
    unittest.main()
