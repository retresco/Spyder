#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_sqlite_queues.py 25-Jan-2011
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

import unittest

import time

from spyder.core.sqlitequeues import SQLiteUriQueues


class SqliteQueuesTest(unittest.TestCase):

    def test_adding_works(self):

        uri = ("http://localhost", "etag", int(time.time()*1000), 1,
                int(time.time() * 1000))

        q = SQLiteUriQueues(":memory:")
        q.add_uri(uri)

        cursor = q._connection.execute("SELECT * FROM queues")
        uri_res = cursor.fetchone()
        (url, etag, mod_date, queue, last_date) = uri
        (url_res, etag_res, mod_date_res, queue_res, last_date_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(queue, queue_res)
        self.assertEqual(last_date, last_date_res)

        q.close()

    def test_updating_works(self):

        uri = ("http://localhost", "etag", int(time.time()*1000), 1,
                int(time.time() * 1000))

        q = SQLiteUriQueues(":memory:")
        q.add_uri(uri)

        uri = ("http://localhost", "etag", int(time.time()*1000), 2,
                int(time.time() * 1000))

        q.update_uri(uri)

        cursor = q._connection.execute("SELECT * FROM queues")
        uri_res = cursor.fetchone()
        (url, etag, mod_date, queue, last_date) = uri
        (url_res, etag_res, mod_date_res, queue_res, last_date_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(queue, queue_res)
        self.assertEqual(last_date, last_date_res)

    def test_adding_lists_works(self):

        uris = [("http://localhost", "etag", int(time.time()*1000), 1,
                int(time.time() * 1000)),
        ]

        q = SQLiteUriQueues(":memory:")
        q.add_uris(uris)

        cursor = q._connection.execute("SELECT * FROM queues")
        uri_res = cursor.fetchone()
        (url, etag, mod_date, queue, last_date) = uris[0]
        (url_res, etag_res, mod_date_res, queue_res, last_date_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(queue, queue_res)
        self.assertEqual(last_date, last_date_res)

    def test_updating_lists_works(self):

        uris = [("http://localhost", "etag", int(time.time()*1000), 1,
                int(time.time() * 1000)),
        ]

        q = SQLiteUriQueues(":memory:")
        q.add_uris(uris)

        uris = [("http://localhost", "etag", int(time.time()*1000), 2,
                int(time.time() * 1000)),
        ]

        q.update_uris(uris)

        cursor = q._connection.execute("SELECT * FROM queues")
        uri_res = cursor.fetchone()
        (url, etag, mod_date, queue, last_date) = uris[0]
        (url_res, etag_res, mod_date_res, queue_res, last_date_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(queue, queue_res)
        self.assertEqual(last_date, last_date_res)

    def test_removing_lists_works(self):

        uris = [("http://localhost", "etag", int(time.time()*1000), 1,
                int(time.time() * 1000)),
            ("http://fogeignhost", "ETAG", int(time.time()*1000), 2,
             int(time.time() * 1000)),
        ]

        q = SQLiteUriQueues(":memory:")
        q.add_uris(uris)

        q.remove_uris(uris)

        cursor = q._connection.execute("SELECT * FROM queues")
        self.assertTrue(None is cursor.fetchone())

    def test_queue_head_works(self):

        uris = [("http://localhost", "etag", int(time.time()*1000), 1,
                int(time.time() * 1000)),
            ("http://fogeignhost", "ETAG", int(time.time()*1000), 2,
             int(time.time() * 1000)),
        ]

        q = SQLiteUriQueues(":memory:")
        q.add_uris(uris)

        (url1, etag1, mod_date1, queue1, last_date1) = uris[0]
        (url2, etag2, mod_date2, queue2, last_date2) = uris[1]

        for uri_res in q.queue_head(1):
            (url_res, etag_res, mod_date_res, queue_res, last_date_res) = uri_res
            self.assertEqual(url1, url_res)
            self.assertEqual(etag1, etag_res)
            self.assertEqual(mod_date1, mod_date_res)
            self.assertEqual(queue1, queue_res)
            self.assertEqual(last_date1, last_date_res)

        for uri_res in q.queue_head(2):
            (url_res, etag_res, mod_date_res, queue_res, last_date_res) = uri_res
            self.assertEqual(url2, url_res)
            self.assertEqual(etag2, etag_res)
            self.assertEqual(mod_date2, mod_date_res)
            self.assertEqual(queue2, queue_res)
            self.assertEqual(last_date2, last_date_res)

        uris.append(("http://localhost/1", "eTag", int(time.time()*1000), 1,
                    int(time.time()*1000)))
        (url3, etag3, mod_date3, queue3, last_date3) = uris[2]
        q.add_uri(uris[2])

        for uri_res in q.queue_head(1, n=1, offset=1):
            (url_res, etag_res, mod_date_res, queue_res, last_date_res) = uri_res
            self.assertEqual(url3, url_res)
            self.assertEqual(etag3, etag_res)
            self.assertEqual(mod_date3, mod_date_res)
            self.assertEqual(queue3, queue_res)
            self.assertEqual(last_date3, last_date_res)

        index = [0, 2]
        i = 0
        for uri_res in q.queue_head(1, n=10):
            (url, etag, mod_date, queue, last_date) = uris[index[i]]
            (url_res, etag_res, mod_date_res, queue_res, last_date_res) = uri_res
            self.assertEqual(url, url_res)
            self.assertEqual(etag, etag_res)
            self.assertEqual(mod_date, mod_date_res)
            self.assertEqual(queue, queue_res)
            self.assertEqual(last_date, last_date_res)
            i += 1


if __name__ == '__main__':
    unittest.main()
