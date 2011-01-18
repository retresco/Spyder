#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_workerprocess.py 18-Jan-2011
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
