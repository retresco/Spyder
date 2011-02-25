#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_sqlite_queues.py 25-Jan-2011
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#

import unittest

import time

from spyder.core.sqlitequeues import SQLiteSingleHostUriQueue, UriNotFound


class SqliteQueuesTest(unittest.TestCase):

    def test_adding_works(self):

        uri = ("http://localhost", "etag", int(time.time()*1000),
                int(time.time() * 1000), 1)

        q = SQLiteSingleHostUriQueue(":memory:")
        q.add_uri(uri)

        self.assertEqual(1, len(q))

        cursor = q._connection.execute("SELECT * FROM queue")
        uri_res = cursor.fetchone()
        (url, etag, mod_date, next_date, prio) = uri
        (url_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(prio, prio_res)
        self.assertEqual(next_date, next_date_res)

        q.close()

    def test_updating_works(self):

        uri = ("http://localhost", "etag", int(time.time()*1000),
                int(time.time() * 1000), 1)

        q = SQLiteSingleHostUriQueue(":memory:")
        q.add_uri(uri)

        uri = ("http://localhost", "etag", int(time.time()*1000),
                int(time.time() * 1000), 2)

        q.update_uri(uri)

        cursor = q._connection.execute("SELECT * FROM queue")
        uri_res = cursor.fetchone()
        (url, etag, mod_date, next_date, prio) = uri
        (url_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(prio, prio_res)
        self.assertEqual(next_date, next_date_res)

    def test_adding_lists_works(self):

        uris = [("http://localhost", "etag", int(time.time()*1000),
                int(time.time() * 1010), 1),
        ]

        q = SQLiteSingleHostUriQueue(":memory:")
        q.add_uris(uris)

        cursor = q._connection.execute("SELECT * FROM queue")
        uri_res = cursor.fetchone()
        (url, etag, mod_date, next_date, prio) = uris[0]
        (url_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(prio, prio_res)
        self.assertEqual(next_date, next_date_res)

    def test_updating_lists_works(self):

        uris = [("http://localhost", "etag", int(time.time()*1000),
                int(time.time() * 1000), 1),
        ]

        q = SQLiteSingleHostUriQueue(":memory:")
        q.add_uris(uris)

        uris = [("http://localhost", "etag", int(time.time()*1000),
                int(time.time() * 1000), 2),
        ]

        q.update_uris(uris)

        cursor = q._connection.execute("SELECT * FROM queue")
        uri_res = cursor.fetchone()
        (url, etag, mod_date, next_date, prio) = uris[0]
        (url_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
        self.assertEqual(url, url_res)
        self.assertEqual(etag, etag_res)
        self.assertEqual(mod_date, mod_date_res)
        self.assertEqual(prio, prio_res)
        self.assertEqual(next_date, next_date_res)

    def test_removing_lists_works(self):

        uris = [("http://localhost", "etag", int(time.time()*1000),
                int(time.time() * 1000), 1),
            ("http://fogeignhost", "ETAG", int(time.time()*1000),
             int(time.time() * 1000), 2),
        ]

        q = SQLiteSingleHostUriQueue(":memory:")
        q.add_uris(uris)

        q.remove_uris(uris)

        cursor = q._connection.execute("SELECT * FROM queue")
        self.assertTrue(None is cursor.fetchone())

    def test_iterating_over_all_uris_works(self):

        uris = [("http://localhost", "etag", int(time.time()*1000),
                int(time.time() * 1000), 1),
            ("http://foreignhost", "ETAG", int(time.time()*1000),
             int(time.time() * 1000), 2),
        ]
        urls = ["http://localhost", "http://foreignhost"]

        q = SQLiteSingleHostUriQueue(":memory:")
        q.add_uris(uris)

        uri = q.get_uri("http://foreignhost")
        self.assertEqual(uris[1], uri)

        self.assertRaises(UriNotFound, q.get_uri, "http://gibtsnuesch")

        for url in q.all_uris():
            self.assertTrue(url in urls)

    def test_queue_head_works(self):

        uris = [("http://localhost", "etag", int(time.time()*1000),
                int(time.time() * 1000), 1),
            ("http://fogeignhost", "ETAG", int(time.time()*1000),
             int(time.time() * 1001), 2),
        ]

        q = SQLiteSingleHostUriQueue(":memory:")
        q.add_uris(uris)

        (url1, etag1, mod_date1, next_date1, prio1) = uris[0]
        (url2, etag2, mod_date2, next_date2, prio2) = uris[1]

        for uri_res in q.queue_head(n=1, offset=0):
            (url_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
            self.assertEqual(url1, url_res)
            self.assertEqual(etag1, etag_res)
            self.assertEqual(mod_date1, mod_date_res)
            self.assertEqual(prio1, prio_res)
            self.assertEqual(next_date1, next_date_res)

        for uri_res in q.queue_head(n=1, offset=1):
            (url_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
            self.assertEqual(url2, url_res)
            self.assertEqual(etag2, etag_res)
            self.assertEqual(mod_date2, mod_date_res)
            self.assertEqual(prio2, prio_res)
            self.assertEqual(next_date2, next_date_res)

        uris.append(("http://localhost/1", "eTag", int(time.time()*1000),
                    int(time.time()*1002), 1))
        (url3, etag3, mod_date3, next_date3, prio3) = uris[2]
        q.add_uri(uris[2])

        q.ignore_uri("http://localhost", 404)

        for uri_res in q.queue_head(n=1, offset=1):
            (url_res, etag_res, mod_date_res, next_date_res, prio_res) = uri_res
            self.assertEqual(url3, url_res)
            self.assertEqual(etag3, etag_res)
            self.assertEqual(mod_date3, mod_date_res)
            self.assertEqual(prio3, prio_res)
            self.assertEqual(next_date3, next_date_res)


if __name__ == '__main__':
    unittest.main()
