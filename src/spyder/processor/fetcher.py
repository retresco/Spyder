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

import logging

from urlparse import urlsplit

from tornado.httpclient import AsyncHTTPClient, HTTPRequest
from tornado.httputil import HTTPHeaders

from zmq.eventloop.ioloop import IOLoop

from spyder.core.constants import CURI_SITE_USERNAME
from spyder.core.constants import CURI_SITE_PASSWORD
from spyder.core.messages import deserialize_date_time

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
        self._max_clients = settings.MAX_CLIENTS
        self._max_simultaneous_connections = \
            settings.MAX_SIMULTANEOUS_CONNECTIONS
        self._follow_redirects = settings.FOLLOW_REDIRECTS
        self._max_redirects = settings.MAX_REDIRECTS
        self._gzip = settings.USE_GZIP

        self._client = AsyncHTTPClient(self._io_loop,
            max_clients=self._max_clients,
            max_simultaneous_connections=self._max_simultaneous_connections)

    def __call__(self, msg, out_socket):
        """
        Work on the current `DataMessage` and send the result to `out_socket`.
        """
        # prepare the HTTPHeaders
        headers = self._prepare_headers(msg)

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
                user_agent=self._user_agent)

        LOG.info("proc.fetch::request for %s" % msg.curi.url)
        self._client.fetch(request, self._handle_response(msg, out_socket))

    def _prepare_headers(self, msg):
        """
        Construct the `HTTPHeaders` with all the necessary information for the
        request.
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

    def _handle_response(self, msg, out_socket):
        """
        Callback is being called when the data has been retrieved from the web.
        """
        def handle_server_response(response):
            self._extract_info_from_response(response, msg)
            LOG.info("proc.fetch::response for %s (took '%s'ms)" %
                    (msg.curi.url, response.request_time))
            out_socket.send_multipart(msg.serialize())

        return handle_server_response

    def _extract_info_from_response(self, response, msg):
        """
        Extract the interesting information from a HTTPResponse.
        """
        msg.curi.status_code = response.code
        msg.curi.content_body = response.body
        msg.curi.req_header = response.request.headers
        msg.curi.rep_header = response.headers
        msg.curi.req_time = response.request_time
        msg.curi.queue_time = response.time_info["queue"]
