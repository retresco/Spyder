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

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.messages import DataMessage, MgmtMessage


class ZmqWorker(object):
    """
    This is the ZMQ worker implementation.

    The worker will register a :class:`ZMQStream` with the configured
    :class:`zmq.Socket` and :class:`zmq.eventloop.ioloop.IOLoop` instance.

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
        self._in_stream = ZMQStream(self._insocket, self._io_loop)
        self._out_stream = ZMQStream(self._outsocket, self._io_loop)

    def _quit(self, msg):
        """
        The worker is quitting, stop receiving messages.
        """
        if ZMQ_SPYDER_MGMT_WORKER_QUIT == msg.data:
            self.stop()

    def _receive(self, msg):
        """
        We have a message!

        `msg` is a serialized version of a `DataMessage`.
        """
        message = DataMessage(msg)

        # this is the real work we want to do
        curi = self._processing(message.curi)

        # finished, now send the result back to the master
        message.curi = curi
        self._out_stream.send_multipart(message.serialize())

    def start(self):
        """
        Start the worker.
        """
        self._mgmt.add_callback(ZMQ_SPYDER_MGMT_WORKER, self._quit)
        self._in_stream.on_recv(self._receive)

    def stop(self):
        """
        Stop the worker.
        """
        # stop receiving
        self._in_stream.stop_on_recv()
        self._mgmt.remove_callback(ZMQ_SPYDER_MGMT_WORKER, self._quit)
        # but work on anything we might already have
        self._in_stream.flush()
        self._out_stream.flush()


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
        sending the result to the `self._outsocket`. This has to be handled by
        the `self._processing` method.
        """
        message = DataMessage(msg)
        self._processing(message, self._out_stream)
