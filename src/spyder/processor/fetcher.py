#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# fetcher.py 14-Jan-2011
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
Module for downloading content from the web.

TODO: document pycurls features, i.e. what it can download.
"""

import logging

from urlparse import urlsplit

from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.httputil import HTTPHeaders

from zmq.eventloop.ioloop import IOLoop

from spyder.core.constants import CURI_SITE_USERNAME
from spyder.core.constants import CURI_SITE_PASSWORD
from spyder.time import deserialize_date_time

LOG = logging.getLogger('fetcher')


class FetchProcessor(object):
    """
    A processing class for downloading all kinds of stuff from the web.
    """

    def __init__(self, settings, io_loop=None):
        """
        Initialize the members.
        """
        self._user_agent = settings.USER_AGENT
        assert self._user_agent

        self._io_loop = io_loop or IOLoop.instance()

        self._follow_redirects = settings.FOLLOW_REDIRECTS
        self._max_redirects = settings.MAX_REDIRECTS
        self._gzip = settings.USE_GZIP

        if settings.PROXY_HOST:
            proxy_port = settings.PROXY_PORT
            assert proxy_port
            assert isinstance(proxy_port, int)

            self._proxy_configuration = dict(
                    host = settings.PROXY_HOST,
                    port = settings.PROXY_PORT,
                    user = settings.PROXY_USERNAME,
                    password = settings.PROXY_PASSWORD
                    )

        self._request_timeout = settings.REQUEST_TIMEOUT
        self._connect_timeout = settings.CONNECT_TIMEOUT

        max_clients = settings.MAX_CLIENTS
        max_simultaneous_connections = settings.MAX_SIMULTANEOUS_CONNECTIONS

        self._client = AsyncHTTPClient(self._io_loop,
            max_clients=max_clients,
            max_simultaneous_connections=max_simultaneous_connections)

    def __call__(self, msg, out_stream):
        """
        Work on the current `DataMessage` and send the result to `out_stream`.
        """
        # prepare the HTTPHeaders
        headers = prepare_headers(msg)

        last_modified = None
        if msg.curi.req_header:
            # check if we have a date when the page was last crawled
            if "Last-Modified" in msg.curi.req_header:
                last_modified = deserialize_date_time(
                        msg.curi.req_header["Last-Modified"])

        # check if we have username and password present
        auth_username = None
        auth_password = None
        if msg.curi.optional_vars and \
            CURI_SITE_USERNAME in msg.curi.optional_vars and \
            CURI_SITE_PASSWORD in msg.curi.optional_vars:

            auth_username = msg.curi.optional_vars[CURI_SITE_USERNAME]
            auth_password = msg.curi.optional_vars[CURI_SITE_PASSWORD]

        # create the request
        request = HTTPRequest(msg.curi.effective_url,
                method="GET",
                headers=headers,
                auth_username=auth_username,
                auth_password=auth_password,
                if_modified_since=last_modified,
                follow_redirects=self._follow_redirects,
                max_redirects=self._max_redirects,
                user_agent=self._user_agent,
                request_timeout = self._request_timeout,
                connect_timeout = self._connect_timeout)

        if hasattr(self, '_proxy_configuration'):
            request.proxy_host = self._proxy_configuration['host']
            request.proxy_port = self._proxy_configuration['port']
            request.proxy_username = \
                    self._proxy_configuration.get('user', None)
            request.proxy_password = \
                    self._proxy_configuration.get('password', None)


        LOG.info("proc.fetch::request for %s" % msg.curi.url)
        self._client.fetch(request, handle_response(msg, out_stream))


def prepare_headers(msg):
    """
    Construct the :class:`HTTPHeaders` with all the necessary information for
    the request.
    """
    # construct the headers
    headers = HTTPHeaders()

    if msg.curi.req_header:
        # check if we have a previous Etag
        if "Etag" in msg.curi.req_header:
            headers["If-None-Match"] = \
                msg.curi.req_header["Etag"]

    # manually set the Host header since we are requesting using an IP
    host = urlsplit(msg.curi.url).hostname
    if host is None:
        LOG.error("proc.fetch::cannot extract hostname from url '%s'" %
                msg.curi.url)
    else:
        headers["Host"] = host

    return headers


def handle_response(msg, out_stream):
    """
    Decorator for the actual callback function that will extract interesting
    info and forward the response.
    """
    def handle_server_response(response):
        """
        The actual callback function.

        Extract interesting info from the response using
        :meth:`extract_info_from_response` and forward the result to the
        `out_stream`.
        """
        extract_info_from_response(response, msg)
        LOG.info("proc.fetch::response for %s (took '%s'ms)" %
                (msg.curi.url, response.request_time))
        if response.code >= 400:
            LOG.error("proc.fetch::response error: %s", response)
        out_stream.send_multipart(msg.serialize())

    return handle_server_response


def extract_info_from_response(response, msg):
    """
    Extract the interesting information from a HTTPResponse.
    """
    msg.curi.status_code = response.code
    msg.curi.req_header = response.request.headers
    msg.curi.rep_header = response.headers
    msg.curi.req_time = response.request_time
    msg.curi.queue_time = response.time_info["queue"]
    msg.curi.content_body = response.body

    return msg
