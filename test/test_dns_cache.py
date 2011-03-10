#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_dns_cache.py 25-Jan-2011
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

from spyder.core.dnscache import DnsCache
from spyder.core.settings import Settings


class DnsCacheTest(unittest.TestCase):

    def test_dns_cache(self):
        s = Settings()
        s.SIZE_DNS_CACHE = 1
        dns = DnsCache(s)
        self.assertEqual(('127.0.0.1', 80), dns["localhost:80"])
        self.assertEqual(('127.0.0.1', 81), dns["localhost:81"])
        self.assertTrue(1, len(dns._cache))

    def test_static_dns_mapping(self):
        s = Settings()
        s.STATIC_DNS_MAPPINGS = {"localhost:123": ("-1.-1.-1.-1", 123)}
        dns = DnsCache(s)
        self.assertEqual(("-1.-1.-1.-1", 123), dns["localhost:123"])
        self.assertEqual(('127.0.0.1', 80), dns["localhost:80"])
        self.assertTrue(1, len(dns._cache))


if __name__ == '__main__':
    unittest.main()
