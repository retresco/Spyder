#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# masterprocess.py 31-Jan-2011
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
import os
import signal
import socket

import zmq
from zmq.eventloop.ioloop import IOLoop

from spyder.core.frontier import SingleHostFrontier
from spyder.core.master import ZmqMaster
from spyder.core.mgmt import ZmqMgmt


def create_master_management(settings, zmq_context, io_loop):
    """
    Create the management interface for master processes.
    """
    listening_socket = zmq_context.socket(zmq.SUB)
    listening_socket.setsockopt(zmq.SUBSCRIBE, "")
    listening_socket.bind(settings.ZEROMQ_MGMT_WORKER)

    publishing_socket = zmq_context.socket(zmq.PUB)
    publishing_socket.bind(settings.ZEROMQ_MGMT_MASTER)

    return ZmqMgmt(listening_socket, publishing_socket, io_loop=io_loop)


def create_frontier(settings):
    """
    Create the frontier to use.
    """
    return SingleHostFrontier(settings)


def main(settings):
    """
    Main method for master processes.
    """
    # create my own identity
    identity = "master:%s:%s" % (socket.gethostname(), os.getpid())

    ctx = zmq.Context()
    io_loop = IOLoop.instance()

    mgmt = create_master_management(settings, ctx, io_loop)
    frontier = create_frontier(settings)

    publishing_socket = ctx.socket(zmq.PUSH)
    publishing_socket.setsockopt(zmq.HWM, settings.ZEROMQ_MASTER_PUSH_HWM)
    publishing_socket.bind(settings.ZEROMQ_MASTER_PUSH)

    receiving_socket = ctx.socket(zmq.SUB)
    receiving_socket.setsockopt(zmq.SUBSCRIBE, "")
    receiving_socket.bind(settings.ZEROMQ_MASTER_SUB)

    master = ZmqMaster(identity, receiving_socket, publishing_socket, mgmt,
            frontier, io_loop)

    def handle_shutdown_signal(sig, frame):
        master.shutdown()

    # handle kill signals
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    mgmt.start()
    master.start()

    # this will block until the master stops
    io_loop.start()

    master.close()
    mgmt.close()

    ctx.term()
