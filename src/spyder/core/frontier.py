#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# frontier.py 26-Jan-2011
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
# All programs in this directory and
# subdirectories are published under the GNU General Public License as
# described below.
#
#
"""
Generic Frontier implementation.

The :class:`SingleHostFrontier` will only select URIs from the queues by
iterating over all available queues and added into a priority queue.

The priority is calculated based on the timestamp it should be crawled next.

In contrast to the :mod:`spyder.core.sqlitequeues` module, URIs in this module
are represented as :class:`spyder.thrift.gen.ttypes.CrawlUri`.
"""

import time
from datetime import datetime

from Queue import PriorityQueue, Empty, Full
from urlparse import urlparse

from spyder.core.constants import CURI_SITE_USERNAME, CURI_SITE_PASSWORD
from spyder.core.dnscache import DnsCache
from spyder.core.messages import serialize_date_time, deserialize_date_time
from spyder.core.sqlitequeues import SQLiteUriQueues
from spyder.thrift.gen.ttypes import CrawlUri


# some default port numbers as of /etc/services
PROTOCOLS_DEFAULT_PORT = {
    "http": 80,
    "https": 443,
    "ftp": 21,
    "ftps": 990,
    "sftp": 115,
}


class AbstractBaseFrontier(object):
    """
    A base class for implementing frontiers.

    Basically this class provides the different general methods and
    configuration parameters used for frontiers.
    """

    def __init__(self, settings):
        """
        Initialize the frontier and instantiate the :class:`SQLiteUriQueues`.
        """
        # front end queues
        self._default_priority = settings.FRONTIER_DEFAULT_PRIORITY
        self._max_priorities = settings.FRONTIER_NUM_PRIORITIES
        self._front_end_queues = SQLiteUriQueues(settings.FRONTIER_STATE_FILE)

        # the heap
        self._heap = PriorityQueue(maxsize=settings.FRONTIER_HEAP_SIZE)
        self._heap_min_size = settings.FRONTIER_HEAP_MIN

        # a list of uris currently being crawled.
        self._current_uris = []

        self._dns_cache = DnsCache(settings.FRONTIER_SIZE_DNS_CACHE)

    def add_uri(self, curi, next_date, prio=None):
        """
        Add the specified :class:`CrawlUri` to the frontier.

        `next_date` is a datetime object for the next time the uri should be
        crawled.

        Note: time based crawling is never strict, it is generally used as some
        kind of prioritization.

        This method may throw an `AssertionError` when the `curi` is already
        known to the frontier. In this case you should use :meth:`update_uri`.
        """
        assert curi.url not in self._current_uris

        if prio is None:
            prio = self._default_priority

        etag = mod_date = None
        if curi.rep_header:
            if "Etag" in curi.rep_header:
                etag = curi.rep_header["Etag"]
            if "Date" in curi.rep_header:
                mod_date = time.mktime(deserialize_date_time(
                    curi.rep_header["Date"]).timetuple())

        next_crawl_date = time.mktime(next_date.timetuple())

        self._front_end_queues.add_uri((curi.url, etag, mod_date,
            prio, next_crawl_date))

    def get_next(self):
        """
        Return the next uri scheduled for crawling.
        """
        if self._heap.qsize() < self._heap_min_size:
            self._update_heap()

        try:
            (next_date, next_uri) = self._heap.get_nowait()
        except Empty:
            # heap is empty, there is nothing to crawl right now!
            # mabe log this in the future
            raise

        return self._crawluri_from_uri(next_uri)

    def _update_heap(self):
        """
        Abstract method. Implement this in the actual Frontier.

        The implementation should really only add uris to the heap if they can
        be downloaded right away.
        """
        pass

    def _crawluri_from_uri(self, uri):
        """
        Convert an URI tuple to a :class:`CrawlUri`.

        Replace the hostname with the real IP in order to cache DNS queries.
        """
        (url, etag, mod_date, queue, next_date) = uri

        parsed_url = urlparse(url)

        # dns resolution and caching
        port = None
        if not parsed_url.port:
            port = PROTOCOLS_DEFAULT_PORT[parsed_url.scheme]

        effective_netloc = self._dns_cache["%s:%s" % (parsed_url.hostname,
            port)]

        curi = CrawlUri(url)
        curi.effective_url = url.replace(parsed_url.netloc, "%s:%s" %
                effective_netloc)
        curi.req_header = dict()
        if etag:
            curi.req_header["Etag"] = etag
        if mod_date:
            mod_date_time = datetime.fromtimestamp(mod_date)
            curi.req_header["Last-Modified"] = serialize_date_time(
                    mod_date_time)

        curi.optional_vars = dict()
        if parsed_url.username and parsed_url.password:
            curi.optional_vars[CURI_SITE_USERNAME] = \
                parsed_url.username.encode()
            curi.optional_vars[CURI_SITE_PASSWORD] = \
                parsed_url.password.encode()

        return curi


class SingleHostFrontier(AbstractBaseFrontier):
    """
    A frontier for crawling a single host.
    """

    def __init__(self, settings):
        """
        Initialize the base frontier.
        """
        AbstractBaseFrontier.__init__(self, settings)

    def _update_heap(self):
        """
        Update the heap with URIs we should crawl.

        Note: it is possible that the heap is not full after it was updated!
        """
        now = time.time()

        for q in range(1, self._max_priorities):

            for uri in self._front_end_queues.queue_head(q, n=50):

                (url, etag, mod_date, queue, next_date) = uri

                if uri not in self._current_uris:
                    if next_date < now:
                        # add this uri
                        try:
                            self._heap.put_nowait((next_date, uri))
                        except Full:
                            # heap is full, return to the caller
                            return
                    else:
                        # no more uris in this queue
                        break
