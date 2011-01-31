#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_frontier.py 27-Jan-2011
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

import time
from datetime import datetime, timedelta
import unittest

from spyder.core.constants import *
from spyder.core.frontier import *
from spyder.core.messages import serialize_date_time, deserialize_date_time
from spyder.core.settings import Settings
from spyder.thrift.gen.ttypes import CrawlUri


class BaseFrontierTest(unittest.TestCase):

    def test_adding_uri_works(self):

        now = datetime(*datetime.fromtimestamp(time.time()).timetuple()[0:6])
        next_crawl_date = now + timedelta(days=1)

        s = Settings()
        s.FRONTIER_STATE_FILE = ":memory:"

        curi = CrawlUri("http://localhost")
        curi.rep_header = { "Etag" : "123", "Date" : serialize_date_time(now) }

        frontier = AbstractBaseFrontier(s,
                SQLiteUriQueues(s.FRONTIER_STATE_FILE))
        frontier.add_uri(curi, next_crawl_date)

        for uri in frontier._front_end_queues.queue_head(1):
            (url, etag, mod_date, queue, next_date) = uri
            self.assertEqual("http://localhost", url)
            self.assertEqual("123", etag)
            self.assertEqual(now, datetime.fromtimestamp(mod_date))
            self.assertEqual(next_crawl_date,
                    datetime.fromtimestamp(next_date))
            frontier._current_uris[url] = uri

        self.assertRaises(AssertionError, frontier.add_uri, curi,
            next_crawl_date)

    def test_crawluri_from_uri(self):

        now = datetime(*datetime.fromtimestamp(time.time()).timetuple()[0:6])
        now_timestamp = time.mktime(now.timetuple())
        next_crawl_date = now + timedelta(days=1)
        next_crawl_date_timestamp = time.mktime(next_crawl_date.timetuple())

        s = Settings()
        s.FRONTIER_STATE_FILE = ":memory:"

        frontier = AbstractBaseFrontier(s,
                SQLiteUriQueues(s.FRONTIER_STATE_FILE))

        uri = ("http://localhost", "123", now_timestamp, 1,
                next_crawl_date_timestamp)

        curi = frontier._crawluri_from_uri(uri)

        self.assertEqual("http://localhost", curi.url)
        self.assertEqual("123", curi.req_header["Etag"])
        self.assertEqual(serialize_date_time(now), curi.req_header["Last-Modified"])

    def test_crawluri_from_uri_with_credentials(self):

        now = datetime(*datetime.fromtimestamp(time.time()).timetuple()[0:6])
        now_timestamp = time.mktime(now.timetuple())
        next_crawl_date = now + timedelta(days=1)
        next_crawl_date_timestamp = time.mktime(next_crawl_date.timetuple())

        s = Settings()
        s.FRONTIER_STATE_FILE = ":memory:"

        frontier = AbstractBaseFrontier(s,
                SQLiteUriQueues(s.FRONTIER_STATE_FILE))

        uri = ("http://user:passwd@localhost", "123", now_timestamp, 1,
            next_crawl_date_timestamp)

        curi = frontier._crawluri_from_uri(uri)

        self.assertEqual("http://user:passwd@localhost", curi.url)
        self.assertEqual("123", curi.req_header["Etag"])
        self.assertEqual(serialize_date_time(now),
            curi.req_header["Last-Modified"])
        self.assertEqual("user", curi.optional_vars[CURI_SITE_USERNAME])
        self.assertEqual("passwd", curi.optional_vars[CURI_SITE_PASSWORD])


class SingleHostFrontierTest(unittest.TestCase):

    def test_that_updating_heap_works(self):

        s = Settings()
        s.FRONTIER_STATE_FILE = ":memory:"
        s.FRONTIER_NUM_PRIORITIES = 2

        frontier = SingleHostFrontier(s)

        q1 = []
        q2 = []

        now = datetime(*datetime.fromtimestamp(
            time.time()).timetuple()[0:6])

        for i in range(1, 20):
            next_crawl = now - timedelta(minutes=(10-i))
            next_timestamp = time.mktime(next_crawl.timetuple())

            curi = CrawlUri("http://localhost/test/%s" % i)
            curi.rep_header = { "Etag" : "123%s" % i, "Date" : serialize_date_time(now) }

            q1.append((curi, next_timestamp))
            frontier.add_uri(curi, next_crawl)

        self.assertRaises(Empty, frontier._heap.get_nowait)

        frontier._update_heap()

        for i in range(0, 10):
            candidate_uri = frontier._heap.get_nowait()
            (curi, next_crawl) = q1[i]
            (nd, (url, etag, mod_date, queue, next_date)) = candidate_uri

            self.assertEqual(curi.url, url)
            self.assertEqual(curi.rep_header["Etag"], etag)
            self.assertEqual(curi.rep_header["Date"],
                    serialize_date_time(datetime.fromtimestamp(mod_date)))

    def test_that_updating_heap_from_multiple_queues_works(self):

        s = Settings()
        s.FRONTIER_STATE_FILE = ":memory:"
        s.FRONTIER_NUM_PRIORITIES = 2

        frontier = SingleHostFrontier(s)

        q1 = []
        q2 = []

        now = datetime(*datetime.fromtimestamp(
            time.time()).timetuple()[0:6])

        for i in range(1, 20):
            next_crawl = now - timedelta(minutes=(10-i))
            next_timestamp = time.mktime(next_crawl.timetuple())

            curi = CrawlUri("http://localhost/q1/%s" % i)
            curi.rep_header = { "Etag" : "123%s" % i, "Date" : serialize_date_time(now) }

            q1.append((curi, next_timestamp))
            frontier.add_uri(curi, next_crawl)

            next_crawl = now - timedelta(minutes=(5-i), seconds=50)
            next_timestamp = time.mktime(next_crawl.timetuple())

            curi = CrawlUri("http://localhost/q2/%s" % i)
            curi.rep_header = { "Etag" : "123%s" % i, "Date" : serialize_date_time(now) }

            q2.append((curi, next_timestamp))
            frontier.add_uri(curi, next_crawl)

        self.assertRaises(Empty, frontier._heap.get_nowait)

        j = 0
        k = 0
        for i in range(0, 15):
            candidate_uri = frontier.get_next()

            n = None
            if 5 <= i <= 15:
                if i % 2 > 0:
                    n = q2[i-5-j]
                    j += 1
                else:
                    n = q1[i-1-k]
                    k += 1
            else:
                n = q1[i]
            (curi, next_crawl) = n

            self.assertEqual(curi.url, candidate_uri.url)
            self.assertEqual(curi.rep_header["Etag"],
                candidate_uri.req_header["Etag"])
            self.assertEqual(curi.rep_header["Date"],
                candidate_uri.req_header["Last-Modified"])

        self.assertRaises(Empty, frontier.get_next)


if __name__ == '__main__':
    unittest.main()
