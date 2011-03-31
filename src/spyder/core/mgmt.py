#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# mgmt.py 10-Jan-2011
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
A management module for managing components via ZeroMQ.
"""

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT
from spyder.core.constants import ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK
from spyder.core.messages import MgmtMessage


class ZmqMgmt(object):
    """
    A :class:`ZMQStream` object handling the management sockets.
    """

    def __init__(self, subscriber, publisher, **kwargs):
        """
        Initialize the management interface.

        The `subscriber` socket is the socket used by the Master to send
        commands to the workers. The publisher socket is used to send commands
        to the Master.

        You have to set the `zmq.SUBSCRIBE` socket option yourself!
        """
        self._io_loop = kwargs.get('io_loop', IOLoop.instance())

        self._subscriber = subscriber
        self._in_stream = ZMQStream(self._subscriber, self._io_loop)

        self._publisher = publisher
        self._out_stream = ZMQStream(self._publisher, self._io_loop)

        self._callbacks = dict()

    def _receive(self, raw_msg):
        """
        Main method for receiving management messages.

        `message` is a multipart message where `message[0]` contains the topic,
        `message[1]` is 0 and `message[1]` contains the actual message.
        """
        msg = MgmtMessage(raw_msg)

        if msg.topic in self._callbacks:
            for callback in self._callbacks[msg.topic]:
                if callable(callback):
                    callback(msg)

        if ZMQ_SPYDER_MGMT_WORKER_QUIT == msg.data:
            self.stop()

    def start(self):
        """
        Start the MGMT interface.
        """
        self._in_stream.on_recv(self._receive)

    def stop(self):
        """
        Stop the MGMT interface.
        """
        self._in_stream.stop_on_recv()
        self.publish(topic=ZMQ_SPYDER_MGMT_WORKER, identity=None,
                data=ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK)

    def close(self):
        """
        Close all open sockets.
        """
        self._in_stream.close()
        self._subscriber.close()
        self._out_stream.close()
        self._publisher.close()

    def add_callback(self, topic, callback):
        """
        Add a callback to the specified topic.
        """
        if not callable(callback):
            raise ValueError('callback must be callable')

        if topic not in self._callbacks:
            self._callbacks[topic] = []

        self._callbacks[topic].append(callback)

    def remove_callback(self, topic, callback):
        """
        Remove a callback from the specified topic.
        """
        if topic in self._callbacks and callback in self._callbacks[topic]:
            self._callbacks[topic].remove(callback)

    def publish(self, topic=None, identity=None, data=None):
        """
        Publish a message to the intended audience.
        """
        assert topic is not None
        assert data is not None
        msg = MgmtMessage(topic=topic, identity=identity, data=data)
        self._out_stream.send_multipart(msg.serialize())
