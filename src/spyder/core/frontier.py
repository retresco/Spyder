#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# frontier.py 26-Jan-2011
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
Generic Frontier implementation.

The :class:`SingleHostFrontier` will only select URIs from the queues by
iterating over all available queues and added into a priority queue.

The priority is calculated based on the timestamp it should be crawled next.

In contrast to the :mod:`spyder.core.sqlitequeues` module, URIs in this module
are represented as :class:`spyder.thrift.gen.ttypes.CrawlUri`.
"""

import time
from datetime import datetime
from datetime import timedelta

from Queue import PriorityQueue, Empty, Full
from urlparse import urlparse

from spyder.core.constants import CURI_SITE_USERNAME, CURI_SITE_PASSWORD
from spyder.core.constants import CURI_EXTRACTED_URLS
from spyder.core.dnscache import DnsCache
from spyder.time import serialize_date_time, deserialize_date_time
from spyder.core.log import LoggingMixin
from spyder.core.sqlitequeues import SQLiteSingleHostUriQueue
from spyder.core.sqlitequeues import SQLiteMultipleHostUriQueue
from spyder.core.uri_uniq import UniqueUriFilter
from spyder.thrift.gen.ttypes import CrawlUri
from spyder.import_util import import_class


# some default port numbers as of /etc/services
PROTOCOLS_DEFAULT_PORT = {
    "http": 80,
    "https": 443,
    "ftp": 21,
    "ftps": 990,
    "sftp": 115,
}


class AbstractBaseFrontier(object, LoggingMixin):
    """
    A base class for implementing frontiers.

    Basically this class provides the different general methods and
    configuration parameters used for frontiers.
    """

    def __init__(self, settings, log_handler, front_end_queues, prioritizer,
        unique_hash='sha1'):
        """
        Initialize the frontier and instantiate the
        :class:`SQLiteSingleHostUriQueue`.

        The default frontier we will use the `sha1` hash function for the
        unique uri filter. For very large crawls you might want to use a
        larger hash function (`sha512`, e.g.)
        """
        LoggingMixin.__init__(self, log_handler, settings.LOG_LEVEL_MASTER)
        # front end queue
        self._prioritizer = prioritizer
        self._front_end_queues = front_end_queues
        # checkpointing
        self._checkpoint_interval = settings.FRONTIER_CHECKPOINTING
        self._uris_added = 0

        # the heap
        self._heap = PriorityQueue(maxsize=settings.FRONTIER_HEAP_SIZE)
        self._heap_min_size = settings.FRONTIER_HEAP_MIN

        # a list of uris currently being crawled.
        self._current_uris = dict()
        # dns cache
        self._dns_cache = DnsCache(settings)
        # unique uri filter
        self._unique_uri = UniqueUriFilter(unique_hash)
        for url in self._front_end_queues.all_uris():
            assert not self._unique_uri.is_known(url, add_if_unknown=True)

        # the sinks
        self._sinks = []

        # timezone
        self._timezone = settings.LOCAL_TIMEZONE
        self._logger.info("frontier::initialized")

    def add_sink(self, sink):
        """
        Add a sink to the frontier. A sink will be responsible for the long
        term storage of the crawled contents.
        """
        self._sinks.append(sink)

    def add_uri(self, curi):
        """
        Add the specified :class:`CrawlUri` to the frontier.

        `next_date` is a datetime object for the next time the uri should be
        crawled.

        Note: time based crawling is never strict, it is generally used as some
        kind of prioritization.
        """
        if self._unique_uri.is_known(curi.url, add_if_unknown=True):
            # we already know this uri
            self._logger.debug("frontier::Trying to update a known uri... " + \
                    "(%s)" % (curi.url,))
            return

        self._logger.info("frontier::Adding '%s' to the frontier" % curi.url)
        self._front_end_queues.add_uri(self._uri_from_curi(curi))
        self._maybe_checkpoint()

    def update_uri(self, curi):
        """
        Update a given uri.
        """
        self._front_end_queues.update_uri(self._uri_from_curi(curi))
        self._maybe_checkpoint()

    def get_next(self):
        """
        Return the next uri scheduled for crawling.
        """
        if self._heap.qsize() < self._heap_min_size:
            self._update_heap()

        try:
            (_next_date, next_uri) = self._heap.get_nowait()
        except Empty:
            # heap is empty, there is nothing to crawl right now!
            # maybe log this in the future
            raise

        return self._crawluri_from_uri(next_uri)

    def close(self):
        """
        Close the underlying frontend queues.
        """
        self._front_end_queues.checkpoint()
        self._front_end_queues.close()

    def _crawl_now(self, uri):
        """
        Convinience method for crawling an uri right away.
        """
        self._add_to_heap(uri, 3000)

    def _add_to_heap(self, uri, next_date):
        """
        Add an URI to the heap that is ready to be crawled.
        """
        self._heap.put_nowait((next_date, uri))
        (url, _etag, _mod_date, _next_date, _prio) = uri
        self._current_uris[url] = uri
        self._logger.debug("frontier::Adding '%s' to the heap" % url)

    def _reschedule_uri(self, curi):
        """
        Return the `next_crawl_date` for :class:`CrawlUri`s.
        """
        (prio, delta) = self._prioritizer.calculate_priority(curi)
        now = datetime.now(self._timezone)
        return (prio, time.mktime((now + delta).timetuple()))

    def _ignore_uri(self, curi):
        """
        Ignore a :class:`CrawlUri` from now on.
        """
        self._front_end_queues.ignore_uri(curi.url, curi.status_code)

    def _uri_from_curi(self, curi):
        """
        Create the uri tuple from the :class:`CrawlUri` and calculate the
        priority.

        Overwrite this method in more specific frontiers.
        """
        etag = mod_date = None
        if curi.rep_header:
            if "Etag" in curi.rep_header:
                etag = curi.rep_header["Etag"]
            if "Last-Modified" in curi.rep_header:
                mod_date = time.mktime(deserialize_date_time(
                    curi.rep_header["Last-Modified"]).timetuple())
            if not mod_date and 'Date' in curi.rep_header:
                mod_date = time.mktime(deserialize_date_time(
                    curi.rep_header["Date"]).timetuple())

        if mod_date:
            # only reschedule if it has been crawled before
            (prio, next_crawl_date) = self._reschedule_uri(curi)
        else:
            (prio, next_crawl_date) = (1,
                    time.mktime(datetime.now(self._timezone).timetuple()))

        return (curi.url, etag, mod_date, next_crawl_date, prio)

    def _crawluri_from_uri(self, uri):
        """
        Convert an URI tuple to a :class:`CrawlUri`.

        Replace the hostname with the real IP in order to cache DNS queries.
        """
        (url, etag, mod_date, _next_date, prio) = uri

        parsed_url = urlparse(url)

        # dns resolution and caching
        port = parsed_url.port
        if not port:
            port = PROTOCOLS_DEFAULT_PORT[parsed_url.scheme]

        effective_netloc = self._dns_cache["%s:%s" % (parsed_url.hostname,
            port)]

        curi = CrawlUri(url)
        curi.effective_url = url.replace(parsed_url.netloc, "%s:%s" %
                effective_netloc)
        curi.current_priority = prio
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

    def _update_heap(self):
        """
        Abstract method. Implement this in the actual Frontier.

        The implementation should really only add uris to the heap if they can
        be downloaded right away.
        """
        pass

    def _maybe_checkpoint(self, force_checkpoint=False):
        """
        Periodically checkpoint the state db.
        """
        self._uris_added += 1
        if self._uris_added > self._checkpoint_interval or force_checkpoint:
            self._front_end_queues.checkpoint()
            self._uris_added = 0

    def process_successful_crawl(self, curi):
        """
        Called when an URI has been crawled successfully.

        `curi` is a :class:`CrawlUri`
        """
        self.update_uri(curi)

        if curi.optional_vars and CURI_EXTRACTED_URLS in curi.optional_vars:
            for url in curi.optional_vars[CURI_EXTRACTED_URLS].split("\n"):
                if len(url) > 5 and not self._unique_uri.is_known(url):
                    self.add_uri(CrawlUri(url))

        del self._current_uris[curi.url]

        for sink in self._sinks:
            sink.process_successful_crawl(curi)

    def process_not_found(self, curi):
        """
        Called when an URL was not found.

        This could mean, that the URL has been removed from the server. If so,
        do something about it!

        Override this method in the actual frontier implementation.
        """
        del self._current_uris[curi.url]
        self._ignore_uri(curi)

        for sink in self._sinks:
            sink.process_not_found(curi)

    def process_redirect(self, curi):
        """
        Called when there were too many redirects for an URL, or the site has
        note been updated since the last visit.

        In the latter case, update the internal uri and increase the priority
        level.
        """
        del self._current_uris[curi.url]

        if curi.status_code in [301, 302]:
            # simply ignore the URL. The URL that is being redirected to is
            # extracted and added in the processing
            self._ignore_uri(curi)

        if curi.status_code == 304:
            # the page has not been modified since the last visit! Update it
            # NOTE: prio increasing happens in the prioritizer
            self.update_uri(curi)

        for sink in self._sinks:
            sink.process_redirect(curi)

    def process_server_error(self, curi):
        """
        Called when there was some kind of server error.

        Override this method in the actual frontier implementation.
        """
        del self._current_uris[curi.url]
        self._ignore_uri(curi)

        for sink in self._sinks:
            sink.process_server_error(curi)


class SingleHostFrontier(AbstractBaseFrontier):
    """
    A frontier for crawling a single host.
    """

    def __init__(self, settings, log_handler):
        """
        Initialize the base frontier.
        """
        prio_clazz = import_class(settings.PRIORITIZER_CLASS)
        AbstractBaseFrontier.__init__(self, settings, log_handler,
                SQLiteSingleHostUriQueue(settings.FRONTIER_STATE_FILE),
                prio_clazz(settings))

        self._crawl_delay = settings.FRONTIER_CRAWL_DELAY_FACTOR
        self._min_delay = settings.FRONTIER_MIN_DELAY
        self._next_possible_crawl = time.time()

    def get_next(self):
        """
        Get the next URI.

        Only return the next URI if  we have waited enough.
        """
        if self._heap.qsize() < self._heap_min_size:
            self._update_heap()

        if time.time() >= self._next_possible_crawl:
            (next_date, next_uri) = self._heap.get_nowait()

            now = datetime.now(self._timezone)
            localized_next_date = self._timezone.fromutc(
                    datetime.utcfromtimestamp(next_date))

            if now < localized_next_date:
                # reschedule the uri for crawling
                self._heap.put_nowait((next_date, next_uri))
                raise Empty()

            self._next_possible_crawl = time.time() + self._min_delay
            return self._crawluri_from_uri(next_uri)

        raise Empty()

    def _update_heap(self):
        """
        Update the heap with URIs we should crawl.

        Note: it is possible that the heap is not full after it was updated!
        """
        self._logger.debug("frontier::Updating heap")
        for uri in self._front_end_queues.queue_head(n=50):

            (url, _etag, _mod_date, next_date, _prio) = uri

            if url not in self._current_uris:
                try:
                    self._add_to_heap(uri, next_date)
                except Full:
                    # heap is full, return to the caller
                    self._logger.error("singlehostfrontier::Heap is full " + \
                            "during update")
                    return

    def process_successful_crawl(self, curi):
        """
        Add the timebased politeness to this frontier.
        """
        AbstractBaseFrontier.process_successful_crawl(self, curi)
        now = time.time()
        self._next_possible_crawl = now + max(self._crawl_delay *
                curi.req_time, self._min_delay)
        self._logger.debug("singlehostfrontier::Next possible crawl: %s" %
                (self._next_possible_crawl,))


class MultipleHostFrontier(AbstractBaseFrontier):
    """
    A Frontier for crawling many hosts simultaneously.
    """

    def __init__(self, settings, log_handler):
        """
        Initialize the abstract base frontier and this implementation with the
        different configuration parameters.
        """
        prio_clazz = import_class(settings.PRIORITIZER_CLASS)
        AbstractBaseFrontier.__init__(self, settings, log_handler,
                SQLiteMultipleHostUriQueue(settings.FRONTIER_STATE_FILE),
                prio_clazz(settings))

        self._delay_factor = settings.FRONTIER_CRAWL_DELAY_FACTOR
        self._min_delay = settings.FRONTIER_MIN_DELAY
        self._num_active_queues = settings.FRONTIER_ACTIVE_QUEUES
        self._max_queue_budget = settings.FRONTIER_QUEUE_BUDGET
        self._budget_punishment = settings.FRONTIER_QUEUE_BUDGET_PUNISH

        self._queue_ids = []
        for (queue, _) in self._front_end_queues.get_all_queues():
            self._queue_ids.append(queue)

        qs_clazz = import_class(settings.QUEUE_SELECTOR_CLASS)
        self._backend_selector = qs_clazz(len(self._queue_ids))

        qa_clazz = import_class(settings.QUEUE_ASSIGNMENT_CLASS)
        self._backend_assignment = qa_clazz(self._dns_cache)

        self._current_queues = dict()
        self._current_queues_in_heap = []
        self._time_politeness = dict()
        self._budget_politeness = dict()

    def _uri_from_curi(self, curi):
        """
        Override the uri creation in order to assign the queue to it. Otherwise
        the uri would not end up in the correct queue.
        """
        uri = AbstractBaseFrontier._uri_from_curi(self, curi)
        (url, etag, mod_date, next_crawl_date, prio) = uri
        ident =  self._backend_assignment.get_identifier(url)

        queue = self._front_end_queues.add_or_create_queue(ident)
        if queue not in self._queue_ids:
            self._queue_ids.append(queue)
            self._backend_selector.reset_queues(len(self._queue_ids))
        return (url, queue, etag, mod_date, next_crawl_date, prio)

    def _add_to_heap(self, uri, next_date):
        """
        Override the base method since it only accepts the smaller tuples.
        """
        (url, queue, etag, mod_date, next_crawl_date, prio) = uri
        queue_free_uri = (url, etag, mod_date, next_crawl_date, prio)
        return AbstractBaseFrontier._add_to_heap(self, queue_free_uri,
                next_date)

    def get_next(self):
        """
        Get the next URI that is ready to be crawled.
        """
        if self._heap.qsize() < self._heap_min_size:
            self._update_heap()

        (_date, uri) = self._heap.get_nowait()
        return self._crawluri_from_uri(uri)

    def _update_heap(self):
        """
        Update the heap from the currently used queues. Respect the time based
        politeness and the queue's budget.

        The algorithm is as follows:

          1. Remove queues that are out of budget and add new ones

          2. Add all URIs to the heap that are crawlable with respect to the
              time based politeness
        """
        self._maybe_add_queues()
        self._cleanup_budget_politeness()

        now = time.mktime(datetime.now(self._timezone).timetuple())
        for q in self._time_politeness.keys():
            if now >= self._time_politeness[q] and \
                q not in self._current_queues_in_heap:

                # we may crawl from this queue!
                queue = self._current_queues[q]
                try:
                    (localized_next_date, next_uri) = queue.get_nowait()
                except Empty:
                    # this queue is empty! Remove it and check the next queue
                    self._remove_queue_from_memory(q)
                    continue

                if now < localized_next_date:
                    # reschedule the uri for crawling
                    queue.put_nowait((localized_next_date, next_uri))
                else:
                    # add this uri to the heap, i.e. it can be crawled
                    self._add_to_heap(next_uri, localized_next_date)
                    self._current_queues_in_heap.append(q)

    def _maybe_add_queues(self):
        """
        If there are free queue slots available, add inactive queues from the
        backend.
        """
        qcount = self._front_end_queues.get_queue_count()
        acount = len(self._current_queues)

        while self._num_active_queues > acount and acount < qcount:
            next_queue = self._get_next_queue()
            if next_queue:
                self._add_queue_from_storage(next_queue)
                self._logger.debug("multifrontier::Adding queue with id=%s" %
                        (next_queue,))
                acount = len(self._current_queues)
            else:
                break

    def _cleanup_budget_politeness(self):
        """
        Check if any queue has reached the `self._max_queue_budget` and replace
        those with queues from the storage.
        """
        removeable = []
        for q in self._budget_politeness.keys():
            if self._budget_politeness[q] <= 0:
                removeable.append(q)

        for rm_queue in removeable:
            next_queue = self._get_next_queue()
            if next_queue:
                self._add_queue_from_storage(next_queue)

            self._remove_queue_from_memory(rm_queue)
            self._logger.debug("multifrontier::Removing queue with id=%s" %
                    rm_queue)

    def _get_next_queue(self):
        """
        Get the next queue candidate.
        """
        for i in range(0, 10):
            next_id = self._backend_selector.get_queue()
            q = self._queue_ids[next_id]
            if q not in self._budget_politeness.keys():
                return q

        return None

    def _get_queue_for_url(self, url):
        """
        Determine the queue for a given `url`.
        """
        ident =  self._backend_assignment.get_identifier(url)
        return self._front_end_queues.get_queue_for_ident(ident)

    def _remove_queue_from_memory(self, queue):
        """
        Remove a queue from the internal memory buffers.
        """
        del self._time_politeness[queue]
        del self._budget_politeness[queue]
        del self._current_queues[queue]

    def _add_queue_from_storage(self, next_queue):
        """
        Called when a queue should be crawled from now on.
        """
        self._budget_politeness[next_queue] = self._max_queue_budget
        self._time_politeness[next_queue] = time.mktime(datetime.now(self._timezone).timetuple())
        self._current_queues[next_queue] = \
            PriorityQueue(maxsize=self._max_queue_budget)

        queue = self._current_queues[next_queue]

        for uri in self._front_end_queues.queue_head(next_queue,
                n=self._max_queue_budget):

            (_url, _queue, _etag, _mod_date, next_date, _prio) = uri
            localized_next_date = self._timezone.fromutc(
                    datetime.utcfromtimestamp(next_date))
            queue.put_nowait((time.mktime(localized_next_date.timetuple()), uri))

    def _update_politeness(self, curi):
        """
        Update all politeness rules.
        """
        uri = self._uri_from_curi(curi)
        (url, queue, etag, mod_date, next_crawl_date, prio) = uri

        if 200 <= curi.status_code < 500:
            self._budget_politeness[queue] -= 1
        if 500 <= curi.status_code < 600:
            self._budget_politeness[queue] -= self._budget_punishment

        now = datetime.now(self._timezone)
        delta_seconds = max(self._delay_factor * curi.req_time,
                self._min_delay)
        self._time_politeness[queue] = time.mktime((now + timedelta(seconds=delta_seconds)).timetuple())

        self._current_queues_in_heap.remove(queue)

    def process_successful_crawl(self, curi):
        """
        Crawling was successful, now update the politeness rules.
        """
        self._update_politeness(curi)
        AbstractBaseFrontier.process_successful_crawl(self, curi)

    def process_not_found(self, curi):
        """
        The page does not exist anymore!
        """
        self._update_politeness(curi)
        AbstractBaseFrontier.process_not_found(self, curi)

    def process_redirect(self, curi):
        """
        There was a redirect.
        """
        self._update_politeness(curi)
        AbstractBaseFrontier.process_server_error(self, curi)

    def process_server_error(self, curi):
        """
        Punish any server errors in the budget for this queue.
        """
        self._update_politeness(curi)
        AbstractBaseFrontier.process_server_error(self, curi)
