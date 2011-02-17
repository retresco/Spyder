#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# masterprocess.py 31-Jan-2011
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
"""
This module contains the default architecture for master process.
"""

import logging
import os
import signal
import socket

import zmq
from zmq.eventloop.ioloop import IOLoop
from zmq.log.handlers import PUBHandler

from spyder.import_util import import_class
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


def create_frontier(settings, log_handler):
    """
    Create the frontier to use.
    """
    frontier = import_class(settings.FRONTIER_CLASS)
    return frontier(settings, log_handler)


def main(settings):
    """
    Main method for master processes.
    """
    # create my own identity
    identity = "master:%s:%s" % (socket.gethostname(), os.getpid())

    ctx = zmq.Context()
    io_loop = IOLoop.instance()

    # initialize the logging subsystem
    log_pub = ctx.socket(zmq.PUB)
    log_pub.connect(settings.ZEROMQ_LOGGING)
    zmq_logging_handler = PUBHandler(log_pub)
    zmq_logging_handler.root_topic = "spyder.master"
    logger = logging.getLogger()
    logger.addHandler(zmq_logging_handler)
    logger.setLevel(settings.LOG_LEVEL_MASTER)

    logger.info("process::Starting up the master")

    mgmt = create_master_management(settings, ctx, io_loop)
    frontier = create_frontier(settings, zmq_logging_handler)

    publishing_socket = ctx.socket(zmq.PUSH)
    publishing_socket.setsockopt(zmq.HWM, settings.ZEROMQ_MASTER_PUSH_HWM)
    publishing_socket.bind(settings.ZEROMQ_MASTER_PUSH)

    receiving_socket = ctx.socket(zmq.SUB)
    receiving_socket.setsockopt(zmq.SUBSCRIBE, "")
    receiving_socket.bind(settings.ZEROMQ_MASTER_SUB)

    master = ZmqMaster(settings, identity, receiving_socket,
            publishing_socket, mgmt, frontier, zmq_logging_handler,
            settings.LOG_LEVEL_MASTER, io_loop)

    def handle_shutdown_signal(_sig, _frame):
        """
        Called from the os when a shutdown signal is fired.
        """
        master.shutdown()

    # handle kill signals
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    if settings.MASTER_CALLBACK:
        callback = import_class(settings.MASTER_CALLBACK)
        callback(settings, ctx, io_loop, frontier)

    mgmt.start()
    master.start()

    # this will block until the master stops
    io_loop.start()

    master.close()
    mgmt.close()

    logger.info("process::Master is down.")
    log_pub.close()

    ctx.term()
