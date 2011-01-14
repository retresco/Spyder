#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# fetcher.py 14-Jan-2011
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

from zmq.eventloop.ioloop import IOLoop
from zmq.eventloop.zmqstream import ZMQStream

from tornado.httpclient import AsyncHTTPClient, HTTPRequest

from spyder.core.worker import AsyncZmqWorker
from spyder.core.messages import DataMessage
from spyder.thrift.gen.ttypes import CrawlUri


class Fetcher(object):
    """
    A processing class for downloading all kinds of stuff from the web.
    """

    def __init__(self, io_loop=None, max_clients=None,
            max_simultaneous_connections=None):
        """
        Initialize the members.
        """
        self._io_loop = io_loop or IOLoop.instance()
        self._max_clients = max_clients or 10
        self._max_simultaneous_connections = max_simultaneous_connections or 1

        self._client = AsyncHTTPClient(self._io_loop,
            max_clients=self._max_clients,
            max_simultaneous_connections=self._max_simultaneous_connections)

        self._stream = ZMQStream(self._in_socket, self._io_loop)
        self.start()

    def __call__(self, raw_message, out_socket):
        """
        We have a message.

        Deserialize the crawl_uri and start working with it.
        """
        msg = DataMessage(raw_message)

        request = HTTPRequest(msg.curi.url)
        self._client.fetch(request, self._handle_response(msg, out_socket))

    def _handle_response(self, msg, out_socket):
        """
        Callback is being called when the data has been retrieved from the web.
        """
        def extract_and_send_data(response):
            msg.curi.content_body = response.body
            out_socket.send_multipart(msg.serialize())

        return extract_and_send_data
