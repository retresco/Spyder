#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# workerprocess.py 18-Jan-2011
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
#
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

import zmq

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

    return ZmqMgmt(listening_socket, publishing_socket, ioloop=io_loop)


def create_worker_fetcher(settings, mgmt, zmq_context, io_loop):
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
            io_loop)


def custom_import(module):
    """
    A custom import method to import a module.
    see: stackoverflow.com: 547829/how-to-dynamically-load-a-python-class
    """
    mod = __import__(module)
    components = module.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def create_processing_function(settings, pipeline):
    """
    Create a processing method that iterates all processors over the incoming
    message.
    """
    processors = []
    for processor in pipeline:
        p = custom_import(processor)
        if "create_processor" not in dir(p):
            raise ValueError("Processor module (%s) does not have a \
                    'create_processor' method!" % processor)
        processors.append(p.create_processor(settings))

    def processing(data_message):
        next_message = data_message
        for p in processors:
            next_message = p(next_message)
        return next_message

    return processing


def create_worker_extractor(settings, mgmt, zmq_context, io_loop):
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
        io_loop=io_loop)


def create_worker_scoper(settings, mgmt, zmq_context, io_loop):
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
        io_loop=io_loop)
