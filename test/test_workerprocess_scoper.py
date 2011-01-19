#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_workerprocess_scoper.py 19-Jan-2011
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
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
from spyder.core.messages import DataMessage
from spyder.core.settings import Settings
from spyder.processor import limiter
from spyder.thrift.gen.ttypes import CrawlUri
from spyder import workerprocess


class WorkerExtractorTestCase(unittest.TestCase):

    def test_that_creating_extractor_works(self):
        ctx = zmq.Context()
        io_loop = IOLoop.instance()

        def stop_looping(_msg):
            io_loop.stop()

        settings = Settings()
        settings.SPYDER_SCOPER_PIPELINE = ['spyder.processor.limiter',]

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
        master_push.setsockopt(zmq.HWM,
                settings.ZEROMQ_WORKER_PROC_EXTRACTOR_PUSH_HWM)
        master_push.bind(settings.ZEROMQ_WORKER_PROC_EXTRACTOR_PUSH)

        master_pull = ctx.socket(zmq.SUB)
        master_pull.setsockopt(zmq.SUBSCRIBE, "")
        master_pull.bind(settings.ZEROMQ_MASTER_SUB)

        time.sleep(.5)

        extractor = workerprocess.create_worker_scoper(settings, mgmt, ctx,
                io_loop)
        extractor.start()

        time.sleep(.5)

        curi = CrawlUri(url="http://localhost:80/robots.txt",
                host_identifier="127.0.0.1",
                effective_url="http://127.0.0.1:%s/robots.txt",
                optional_vars=dict(),
                )
        msg = DataMessage()
        msg.identity = "me"
        msg.curi = curi

        def assert_expected_result_and_stop(raw_msg):
            msg = DataMessage(raw_msg)
            self.assertEqual(CURI_OPTIONAL_TRUE,
                    msg.curi.optional_vars[CURI_EXTRACTION_FINISHED])
            pubsocket.send_multipart(ZMQ_SPYDER_MGMT_WORKER_QUIT)

        stream = ZMQStream(master_pull, io_loop)
        stream.on_recv(assert_expected_result_and_stop)

        master_push.send_multipart(msg.serialize())

        time.sleep(1)
        io_loop.start()

        self.assertEqual(ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK,
                subsocket.recv_multipart())

        mgmt._subscriber.close()
        mgmt._publisher.close()
        subsocket.close()
        pubsocket.close()

        master_pull.close()
        master_push.close()
        extractor._insocket.close()
        extractor._outsocket.close()
        ctx.term()


if __name__ == '__main__':
    unittest.main()

