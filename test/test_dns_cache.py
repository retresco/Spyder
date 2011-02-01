#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_dns_cache.py 25-Jan-2011
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

from spyder.core.dnscache import DnsCache


class DnsCacheTest(unittest.TestCase):

    def test_dns_cache(self):
        dns = DnsCache(1)
        self.assertEqual(('127.0.0.1', 80), dns["localhost:80"])
        self.assertEqual(('127.0.0.1', 81), dns["localhost:81"])
        self.assertTrue(1, len(dns._cache))


if __name__ == '__main__':
    unittest.main()
