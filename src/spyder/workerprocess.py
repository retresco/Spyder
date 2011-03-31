#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# workerprocess.py 18-Jan-2011
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
"""
This module contains the default architecture for worker processes. In order to
start a new worker process you should simply call this modules `main` method.

Communication between master -> worker and inside the worker is as follows:

    Master              -> PUSH ->              Worker Fetcher
    Worker Fetcher      -> PUSH ->              Worker Extractor
    Worker Extractor    -> PUSH ->              Worker Scoper
    Worker Scoper       -> PUB  ->              Master

Each Worker is a ZmqWorker (or ZmqAsyncWorker). The Master pushes new CrawlUris
to the `Worker Fetcher`. This will download the content from the web and `PUSH`
the resulting `CrawlUri` to the `Worker Extractor`. At this stage several
Modules for extracting new URLs are running. The `Worker Scoper` will decide if
the newly extracted URLs are within the scope of the crawl.
"""
import logging
import os
import signal
import socket
import traceback

import zmq
from zmq.core.error import ZMQError
from zmq.eventloop.ioloop import IOLoop, DelayedCallback
from zmq.log.handlers import PUBHandler

from spyder.import_util import custom_import
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_AVAIL
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
from spyder.core.messages import MgmtMessage
from spyder.core.mgmt import ZmqMgmt
from spyder.core.worker import ZmqWorker, AsyncZmqWorker
from spyder.processor.fetcher import FetchProcessor


def create_worker_management(settings, zmq_context, io_loop):
    """
    Create and return a new instance of the `ZmqMgmt`.
    """
    listening_socket = zmq_context.socket(zmq.SUB)
    listening_socket.setsockopt(zmq.SUBSCRIBE, "")
    listening_socket.connect(settings.ZEROMQ_MGMT_MASTER)

    publishing_socket = zmq_context.socket(zmq.PUB)
    publishing_socket.connect(settings.ZEROMQ_MGMT_WORKER)

    return ZmqMgmt(listening_socket, publishing_socket, io_loop=io_loop)


def create_worker_fetcher(settings, mgmt, zmq_context, log_handler, io_loop):
    """
    Create and return a new `Worker Fetcher`.
    """
    pulling_socket = zmq_context.socket(zmq.PULL)
    pulling_socket.connect(settings.ZEROMQ_WORKER_PROC_FETCHER_PULL)

    pushing_socket = zmq_context.socket(zmq.PUSH)
    pushing_socket.setsockopt(zmq.HWM,
            settings.ZEROMQ_WORKER_PROC_FETCHER_PUSH_HWM)
    pushing_socket.bind(settings.ZEROMQ_WORKER_PROC_FETCHER_PUSH)

    fetcher = FetchProcessor(settings, io_loop)

    return AsyncZmqWorker(pulling_socket, pushing_socket, mgmt, fetcher,
            log_handler, settings.LOG_LEVEL_WORKER, io_loop)


def create_processing_function(settings, pipeline):
    """
    Create a processing method that iterates all processors over the incoming
    message.
    """
    processors = []
    for processor in pipeline:
        processor_module = custom_import(processor)
        if "create_processor" not in dir(processor_module):
            raise ValueError("Processor module (%s) does not have a \
                    'create_processor' method!" % processor)
        processors.append(processor_module.create_processor(settings))

    def processing(data_message):
        """
        The actual processing function calling each configured processor in the
        order they have been configured.
        """
        next_message = data_message
        for processor in processors:
            next_message = processor(next_message)
        return next_message

    return processing


def create_worker_extractor(settings, mgmt, zmq_context, log_handler, io_loop):
    """
    Create and return a new `Worker Extractor` that will combine all configured
    extractors to a single :class:`ZmqWorker`.
    """
    # the processing function used to process the incoming `DataMessage` by
    # iterating over all available processors
    processing = create_processing_function(settings,
        settings.SPYDER_EXTRACTOR_PIPELINE)

    pulling_socket = zmq_context.socket(zmq.PULL)
    pulling_socket.connect(settings.ZEROMQ_WORKER_PROC_EXTRACTOR_PULL)

    pushing_socket = zmq_context.socket(zmq.PUSH)
    pushing_socket.setsockopt(zmq.HWM,
            settings.ZEROMQ_WORKER_PROC_EXTRACTOR_PUSH_HWM)
    pushing_socket.bind(settings.ZEROMQ_WORKER_PROC_EXTRACTOR_PUSH)

    return ZmqWorker(pulling_socket, pushing_socket, mgmt, processing,
        log_handler, settings.LOG_LEVEL_WORKER, io_loop=io_loop)


def create_worker_scoper(settings, mgmt, zmq_context, log_handler, io_loop):
    """
    Create and return a new `Worker Scoper` that will check if the newly
    extracted URLs are within this crawls scope.
    """
    processing = create_processing_function(settings,
        settings.SPYDER_SCOPER_PIPELINE)

    pulling_socket = zmq_context.socket(zmq.PULL)
    pulling_socket.connect(settings.ZEROMQ_WORKER_PROC_SCOPER_PULL)

    pushing_socket = zmq_context.socket(zmq.PUB)
    pushing_socket.connect(settings.ZEROMQ_WORKER_PROC_SCOPER_PUB)

    return ZmqWorker(pulling_socket, pushing_socket, mgmt, processing,
        log_handler, settings.LOG_LEVEL_WORKER, io_loop=io_loop)


def main(settings):
    """
    The :meth:`main` method for worker processes.

    Here we will:

     - create a :class:`ZmqMgmt` instance

     - create a :class:`Fetcher` instance

     - initialize and instantiate the extractor chain

     - initialize and instantiate the scoper chain

    The `settings` have to be loaded already.
    """
    # create my own identity
    identity = "worker:%s:%s" % (socket.gethostname(), os.getpid())

    ctx = zmq.Context()
    io_loop = IOLoop.instance()

    # initialize the logging subsystem
    log_pub = ctx.socket(zmq.PUB)
    log_pub.connect(settings.ZEROMQ_LOGGING)
    zmq_logging_handler = PUBHandler(log_pub)
    zmq_logging_handler.root_topic = "spyder.worker"
    logger = logging.getLogger()
    logger.addHandler(zmq_logging_handler)
    logger.setLevel(settings.LOG_LEVEL_WORKER)

    logger.info("process::Starting up another worker")

    mgmt = create_worker_management(settings, ctx, io_loop)

    logger.debug("process::Initializing fetcher, extractor and scoper")

    fetcher = create_worker_fetcher(settings, mgmt, ctx, zmq_logging_handler,
        io_loop)
    fetcher.start()
    extractor = create_worker_extractor(settings, mgmt, ctx,
        zmq_logging_handler, io_loop)
    extractor.start()
    scoper = create_worker_scoper(settings, mgmt, ctx, zmq_logging_handler,
        io_loop)
    scoper.start()

    def quit_worker(raw_msg):
        """
        When the worker should quit, stop the io_loop after 2 seconds.
        """
        msg = MgmtMessage(raw_msg)
        if ZMQ_SPYDER_MGMT_WORKER_QUIT == msg.data:
            logger.info("process::We have been asked to shutdown, do so")
            DelayedCallback(io_loop.stop, 2000, io_loop).start()
            ack = MgmtMessage(topic=ZMQ_SPYDER_MGMT_WORKER, identity=identity,
                    data=ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK)
            mgmt._out_stream.send_multipart(ack.serialize())

    mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, quit_worker)
    mgmt.start()

    # notify the master that we are online
    msg = MgmtMessage(topic=ZMQ_SPYDER_MGMT_WORKER, identity=identity,
            data=ZMQ_SPYDER_MGMT_WORKER_AVAIL)
    mgmt._out_stream.send_multipart(msg.serialize())

    def handle_shutdown_signal(_sig, _frame):
        """
        Called from the os when a shutdown signal is fired.
        """
        msg = MgmtMessage(data=ZMQ_SPYDER_MGMT_WORKER_QUIT)
        quit_worker(msg.serialize())

    # handle kill signals
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)

    logger.info("process::waiting for action")
    # this will block until the worker quits
    try:
        io_loop.start()
    except ZMQError:
        logger.debug("Caught a ZMQError. Hopefully during shutdown")
        logger.debug(traceback.format_exc())

    for mod in [fetcher, extractor, scoper, mgmt]:
        mod.close()

    logger.info("process::Houston: Worker down")
    ctx.term()
