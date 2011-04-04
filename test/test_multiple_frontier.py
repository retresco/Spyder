#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# test_multiple_frontier.py 31-Mar-2011
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
import logging
from logging import StreamHandler

from datetime import datetime
from datetime import timedelta
import time
import unittest
import sys

from spyder.core.frontier import MultipleHostFrontier
from spyder.core.settings import Settings
from spyder.time import serialize_date_time, deserialize_date_time
from spyder.thrift.gen.ttypes import CrawlUri


class MultipleHostFrontierTest(unittest.TestCase):

    def test_that_adding_uris_works(self):

        s = Settings()
        s.FRONTIER_STATE_FILE = ":memory:"

        frontier = MultipleHostFrontier(s, StreamHandler(sys.stdout))

        now = datetime(*datetime.fromtimestamp(time.time()).timetuple()[0:6])
        next_crawl_date = now + timedelta(days=1)
        curi = CrawlUri("http://localhost")
        curi.rep_header = { "Etag" : "123", "Date" : serialize_date_time(now) }
        curi.current_priority = 2

        frontier.add_uri(curi)

        cur = frontier._front_end_queues._cursor

        curi = CrawlUri("http://foreignhost")
        curi.rep_header = { "Etag" : "123", "Date" : serialize_date_time(now) }
        curi.current_priority = 1

        frontier.add_uri(curi)

        idents = {"localhost": -1, "foreignhost": -1}
        cur.execute("SELECT * FROM queue_identifiers")
        for row in cur:
            self.assertTrue(row['identifier'] in idents.keys())
            idents["http://%s" % row['identifier']] = row['queue']

        cur.execute("SELECT * FROM queues")
        for row in cur:
            self.assertEqual(idents[row['url']], row['queue'])

        self.assertEqual(2, frontier._front_end_queues.get_queue_count())

    def test_queues_work(self):

        s = Settings()
        s.FRONTIER_STATE_FILE = ":memory:"
        s.FRONTIER_ACTIVE_QUEUES = 1
        s.FRONTIER_QUEUE_BUDGET = 4
        s.FRONTIER_QUEUE_BUDGET_PUNISH = 5

        frontier = MultipleHostFrontier(s, StreamHandler(sys.stdout))

        now = datetime(*datetime.fromtimestamp(time.time()).timetuple()[0:6])
        curi1 = CrawlUri("http://localhost")
        curi1.current_priority = 2
        curi1.req_time = 0.4

        frontier.add_uri(curi1)

        cur = frontier._front_end_queues._cursor

        curi2 = CrawlUri("http://foreignhost")
        curi2.current_priority = 1
        curi2.req_time = 1.4

        frontier.add_uri(curi2)

        self.assertEqual(0, len(frontier._current_queues))
        frontier._maybe_add_queues()

        self.assertEqual(1, len(frontier._current_queues))
        for q1 in frontier._current_queues.keys():
            pass

        self.assertEquals(4, frontier._budget_politeness[q1])
        frontier._cleanup_budget_politeness()
        self.assertEquals(4, frontier._budget_politeness[q1])

        frontier._update_heap()
        self.assertEqual(1, len(frontier._current_queues))

        if q1 == 1:
            frontier.process_server_error(curi1)
        else:
            frontier.process_server_error(curi2)

        self.assertEquals(-1, frontier._budget_politeness[q1])

        frontier._cleanup_budget_politeness()

        self.assertEqual(1, len(frontier._current_queues))
        for q2 in frontier._current_queues.keys():
            pass

        self.assertEquals(4, frontier._budget_politeness[q2])
        frontier._cleanup_budget_politeness()
        self.assertEquals(4, frontier._budget_politeness[q2])
 
        frontier._update_heap()
        self.assertEqual(1, len(frontier._current_queues))

        if q2 == 1:
            frontier.process_successful_crawl(curi1)
        else:
            frontier.process_successful_crawl(curi2)

        self.assertEquals(3, frontier._budget_politeness[q2])

        frontier._cleanup_budget_politeness()


if __name__ == '__main__':
    unittest.main()
