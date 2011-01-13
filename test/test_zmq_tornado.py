#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_zmq_tornado.py 13-Jan-2011
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

import time

import zmq
from zmq.eventloop import ioloop
from zmq.eventloop.zmqstream import ZMQStream

from tornado.httpclient import AsyncHTTPClient, HTTPRequest


class Downloader(object):

    def __init__(self, in_socket, out_socket, io_loop):
        self._in_socket = in_socket
        self._in_socket.setsockopt(zmq.HWM, 10)
        self._out_socket = out_socket
        self._io_loop = io_loop
        self._client = AsyncHTTPClient(self._io_loop,
            max_clients=10, max_simultaneous_connections=1)

        self._stream = ZMQStream(self._in_socket, self._io_loop)
        self._stream.on_recv(self._receive)

    def _receive(self, msg):
        """
        Msg is a URL we should download or 'EXIT'.
        """
        if msg[0] == "EXIT":
            print "stopping downloader"
            self._stream.flush()
            self._stream.stop_on_recv()
            self._out_socket.send_unicode(msg[0])
        else:
            self._download_this(msg)

    def _download_this(self, url):
        print url[0]
        req = HTTPRequest(url[0].encode("utf-8"))
        self._client.fetch(req, self._handle_response)
        
    def _handle_response(self, response):
        if response.error:
            print "Error: %s", response.error
        else:
            # simply send the response body to the ougoing ZMQ socket
            self._out_socket.send_multipart([response.request.url,
                str(response.request_time)])


def main(urls):
    ctx = zmq.Context(1)
    io_loop = ioloop.IOLoop.instance()
    
    master_push = ctx.socket(zmq.PUSH)
    master_push.bind('inproc://master/download')

    worker_pull = ctx.socket(zmq.PULL)
    worker_pull.connect('inproc://master/download')

    worker_pub = ctx.socket(zmq.PUB)
    worker_pub.bind('inproc://worker/fetched')

    master_sub = ctx.socket(zmq.SUB)
    master_sub.connect('inproc://worker/fetched')
    master_sub.setsockopt(zmq.SUBSCRIBE, "")

    def print_and_send_2_more(msg):
        if len(msg) == 2:
            print "Downloading of %s took %s'" % (msg[0], msg[1])
        else:
            print "should exit"
            ioloop.DelayedCallback(io_loop.stop, 5000.0, io_loop).start()

    master_stream = ZMQStream(master_sub, io_loop)
    master_stream.on_recv(print_and_send_2_more)

    downloader = Downloader(worker_pull, worker_pub, io_loop)

    for url in urls:
        master_push.send_unicode(url)

    print "starting"
    io_loop.start()
    print "finished"

    master_push.close()
    master_sub.close()
    worker_pull.close()
    worker_pub.close
    ctx.term()


if __name__ == '__main__':
    urls = [
        u'http://www.google.com',
        u'http://www.yahoo.com',
        u'http://www.microsoft.com',
        u'EXIT'
    ]
    main(urls)

