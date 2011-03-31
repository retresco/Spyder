#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_sqlite_multiple_queues.py 15-Mar-2011
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

from spyder.core.sqlitequeues import SQLiteMultipleHostUriQueue, UriNotFound


class SqliteQueuesTest(unittest.TestCase):


    def test_adding_works(self):

        uri = ("http://localhost", 1, "etag", int(time.time()*1000),
                int(time.time() * 1000), 1)

        q = SQLiteMultipleHostUriQueue(":memory:")
        q.add_uri(uri)

        self.assertEqual(1, q.qsize())

        cursor = q._connection.execute("SELECT * FROM queues WHERE queue=1")
        uri_res = cursor.fetchone()
        (url, queue, etag, mod_date, next_date, prio) = uri
        (url_res, queue_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(queue, queue_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(prio, prio_res)
        self.assertEqual(next_date, next_date_res)

        q.close()

    def test_updating_works(self):

        uri = ("http://localhost", 1, "etag", int(time.time()*1000),
                int(time.time() * 1000), 1)

        q = SQLiteMultipleHostUriQueue(":memory:")
        q.add_uri(uri)

        uri = ("http://localhost", 1, "etag", int(time.time()*1000),
                int(time.time() * 1000), 2)

        q.update_uri(uri)

        cursor = q._connection.execute("SELECT * FROM queues WHERE queue=1")
        uri_res = cursor.fetchone()
        (url, queue, etag, mod_date, next_date, prio) = uri
        (url_res, queue_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(prio, prio_res)
        self.assertEqual(next_date, next_date_res)

        q.close()

    def test_adding_lists_works(self):

        uris = [("http://localhost", 1, "etag", int(time.time()*1000),
                int(time.time() * 1010), 1),
        ]

        q = SQLiteMultipleHostUriQueue(":memory:")
        q.add_uris(uris)

        cursor = q._connection.execute("SELECT * FROM queues WHERE queue=1")
        uri_res = cursor.fetchone()
        (url, queue, etag, mod_date, next_date, prio) = uris[0]
        (url_res, queue_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(prio, prio_res)
        self.assertEqual(next_date, next_date_res)

    def test_updating_lists_works(self):

        uris = [("http://localhost", 1, "etag", int(time.time()*1000),
                int(time.time() * 1000), 1),
        ]

        q = SQLiteMultipleHostUriQueue(":memory:")
        q.add_uris(uris)

        uris = [("http://localhost", 1, "etag", int(time.time()*1000),
                int(time.time() * 1000), 2),
        ]

        q.update_uris(uris)

        cursor = q._connection.execute("SELECT * FROM queues WHERE queue=1")
        uri_res = cursor.fetchone()
        (url, queue, etag, mod_date, next_date, prio) = uris[0]
        (url_res, queue_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(prio, prio_res)
        self.assertEqual(next_date, next_date_res)

    def test_removing_lists_works(self):

        uris = [("http://localhost", 1, "etag", int(time.time()*1000),
                int(time.time() * 1000), 1),
            ("http://fogeignhost", 1, "ETAG", int(time.time()*1000),
             int(time.time() * 1000), 2),
        ]

        q = SQLiteMultipleHostUriQueue(":memory:")
        q.add_uris(uris)

        q.remove_uris(uris)

        cursor = q._connection.execute("SELECT * FROM queues WHERE queue=1")
        self.assertTrue(None is cursor.fetchone())

    def test_iterating_over_all_uris_works(self):

        uris = [("http://localhost", 1, "etag", int(time.time()*1000),
                int(time.time() * 1000), 1),
            ("http://foreignhost", 1, "ETAG", int(time.time()*1000),
             int(time.time() * 1000), 2),
        ]
        urls = ["http://localhost", "http://foreignhost"]

        q = SQLiteMultipleHostUriQueue(":memory:")
        q.add_uris(uris)

        uri = q.get_uri("http://foreignhost")
        self.assertEqual(uris[1], uri)

        self.assertRaises(UriNotFound, q.get_uri, "http://gibtsnuesch")

        for url in q.all_uris():
            self.assertTrue(url in urls)

    def test_queue_head_works(self):

        uris = [("http://localhost", 1, "etag", int(time.time()*1000),
                int(time.time() * 1000), 1),
            ("http://fogeignhost", 1, "ETAG", int(time.time()*1000),
             int(time.time() * 1001), 2),
        ]

        q = SQLiteMultipleHostUriQueue(":memory:")
        q.add_uris(uris)

        self.assertEqual(2, q.qsize())
        self.assertEqual(2, q.qsize(queue=1))

        (url1, queue1,  etag1, mod_date1, next_date1, prio1) = uris[0]
        (url2, queue2, etag2, mod_date2, next_date2, prio2) = uris[1]

        for uri_res in q.queue_head(1, n=1, offset=0):
            (url_res, queue_res,  etag_res, mod_date_res, next_date_res,
                prio_res) = uri_res
            self.assertEqual(url1, url_res)
            self.assertEqual(queue1, queue_res)
            self.assertEqual(etag1, etag_res)
            self.assertEqual(mod_date1, mod_date_res)
            self.assertEqual(prio1, prio_res)
            self.assertEqual(next_date1, next_date_res)

        for uri_res in q.queue_head(1, n=1, offset=1):
            (url_res, queue_res,  etag_res, mod_date_res, next_date_res,
                prio_res) = uri_res
            self.assertEqual(url2, url_res)
            self.assertEqual(queue2, queue_res)
            self.assertEqual(etag2, etag_res)
            self.assertEqual(mod_date2, mod_date_res)
            self.assertEqual(prio2, prio_res)
            self.assertEqual(next_date2, next_date_res)

        uris.append(("http://localhost/1", 1, "eTag", int(time.time()*1000),
                    int(time.time()*1002), 1))
        (url3, queue3, etag3, mod_date3, next_date3, prio3) = uris[2]
        q.add_uri(uris[2])

        self.assertEqual(3, q.qsize())
        self.assertEqual(3, q.qsize(queue=1))

        q.ignore_uri("http://localhost", 404)

        for uri_res in q.queue_head(1, n=1, offset=1):
            (url_res, queue_res,  etag_res, mod_date_res, next_date_res,
                prio_res) = uri_res
            self.assertEqual(url3, url_res)
            self.assertEqual(queue3, queue_res)
            self.assertEqual(etag3, etag_res)
            self.assertEqual(mod_date3, mod_date_res)
            self.assertEqual(prio3, prio_res)
            self.assertEqual(next_date3, next_date_res)

        uris.append(("http://localhost2/1", 2, "eTag", int(time.time()*1000),
                    int(time.time()*1002), 1))
        (url4, queue4, etag4, mod_date4, next_date4, prio4) = uris[3]
        q.add_uri(uris[3])

        self.assertEqual(4, q.qsize())
        self.assertEqual(2, q.qsize(queue=1))
        self.assertEqual(1, q.qsize(queue=2))

        for uri_res in q.queue_head(2, n=1, offset=0):
            (url_res, queue_res,  etag_res, mod_date_res, next_date_res,
                prio_res) = uri_res
            self.assertEqual(url4, url_res)
            self.assertEqual(queue4, queue_res)
            self.assertEqual(etag4, etag_res)
            self.assertEqual(mod_date4, mod_date_res)
            self.assertEqual(prio4, prio_res)
            self.assertEqual(next_date4, next_date_res)

    def test_that_queues_work(self):

        q = SQLiteMultipleHostUriQueue(':memory:')

        for queue in q.get_all_queues():
            self.assertFalse(True)

        qid1 = q.add_queue('test')
        
        for (queue, ident) in q.get_all_queues():
            self.assertEqual(qid1, queue)
            self.assertEqual('test', ident)

        qid2 = q.add_queue('test2')

        i = 0
        for (queue, ident) in q.get_all_queues():
            if i==0:
                self.assertEqual(qid1, queue)
                self.assertEqual('test', ident)
                i += 1
            else:
                self.assertEqual(qid2, queue)
                self.assertEqual('test2', ident)

        self.assertEqual(qid1, q.add_queue('test'))


if __name__ == '__main__':
    unittest.main()
