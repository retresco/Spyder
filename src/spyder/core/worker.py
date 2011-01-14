#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# worker.py 10-Jan-2011
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
This module contains a ZeroMQ based Worker abstraction.

The `ZmqWorker` class expects an incoming and one outgoing `zmq.socket` as well
as an instance of the `spyder.core.mgmt.ZmqMgmt` class.
"""

from  thrift import TSerialization

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.thrift.gen.ttypes import CrawlUri


class ZmqWorker(object):
    """
    This is the ZMQ worker implementation.

    The worker will register a `ZMQStream` with the configured `zmq.socket` and
    `zmq.eventloop.ioloop` instance.

    Upon `ZMQStream.on_recv` the configured `processors` will be executed
    with the deserialized context and the result will be published through the
    configured `zmq.socket`.
    """

    def __init__(self, insocket, outsocket, mgmt, processing, io_loop=None):
        """
        Initialize the `ZMQStream` with the `insocket` and `io_loop` and store
        the `outsocket`.

        `insocket` should be of the type `zmq.socket.PULL` `outsocket` should
        be of the type `zmq.socket.PUB`

        `mgmt` is an instance of `spyder.core.mgmt.ZmqMgmt` that handles
        communication between master and worker processes.
        """

        self._insocket = insocket
        self._io_loop = io_loop or IOLoop.instance()
        self._outsocket = outsocket

        self._processing = processing
        self._mgmt = mgmt
        self._stream = ZMQStream(self._insocket, self._io_loop)

    def _quit(self, msg):
        """
        The worker is quitting, stop receiving messages.
        """
        if msg == ZMQ_SPYDER_MGMT_WORKER_QUIT:
            self.stop()

    def _receive(self, msg):
        """
        We have a message!

        The `msg` has to have only one part: a serialized version of
        `spyder.thrift.gen.ttype.CrawlUri`.
        """

        crawl_uri = deserialize_crawl_uri(msg[0])

        # this is the real work we want to do
        crawl_uri = self._processing(crawl_uri)

        # finished, now send the result back to the master
        msg = [serialize_crawl_uri(crawl_uri)]
        self._outsocket.send_multipart(msg)

    def start(self):
        """
        Start the worker.
        """
        self._mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, self._quit)
        self._stream.on_recv(self._receive)

    def stop(self):
        """
        Stop the worker.
        """
        self._mgmt.remove_callback(ZMQ_SPYDER_MGMT_WORKER, self._quit)
        self._stream.stop_on_recv()


class AsyncZmqWorker(ZmqWorker):
    """
    Asynchronous version of the `ZmqWorker`.

    This worker differs in that the _processing method should ahve two
    arguments: the message and the socket where the result should be sen to!
    """

    def _receive(self, msg):
        """
        We have a message!

        Instead of the synchronous version we do not handle serializing and
        sending the result to the `self._outsocket`. This will be handled by
        the `self._processing` method.
        """
        crawl_uri = deserialize_crawl_uri(msg[0])
        self._processing(crawl_uri, self._outsocket)


def deserialize_crawl_uri(serialized):
    """
    Deserialize a `CrawlUri` that has been serialized using Thrift.
    """
    return TSerialization.deserialize(CrawlUri(), serialized)


def serialize_crawl_uri(crawl_uri):
    """
    Serialize a `CrawlUri` using Thrift.
    """
    return TSerialization.serialize(crawl_uri)
