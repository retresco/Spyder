#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# test_strip_session_ids.py 14-Apr-2011
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
#
import unittest

from spyder.core.constants import CURI_EXTRACTED_URLS
from spyder.core.settings import Settings
from spyder.processor.stripsessions import StripSessionIds
from spyder.thrift.gen.ttypes import CrawlUri


class StripSessionIdsTest(unittest.TestCase):

    def test_that_stripping_session_stuff_works(self):

        s = StripSessionIds(Settings())

        url = "http://preis.de/traeger/index.php?sid=8429fb3ae210a2a0e28800b7f48d90f2"

        self.assertEqual("http://preis.de/traeger/index.php?",
                s._remove_session_ids(url))

        url = "http://preis.de/traeger/index.php?jsessionid=8429fb3ae210a2a0e28800b7f48d90f2"

        self.assertEqual("http://preis.de/traeger/index.php?",
                s._remove_session_ids(url))

        url = "http://preis.de/traeger/index.php?phpsessid=8429fb3ae210a2a0e28800b7f48d90f2"

        self.assertEqual("http://preis.de/traeger/index.php?",
                s._remove_session_ids(url))

        url = "http://preis.de/traeger/index.php?aspsessionid=8429fb3ae210a2a0e28800b7f48d90f2"

        self.assertEqual("http://preis.de/traeger/index.php?",
                s._remove_session_ids(url))

    def test_that_with_uri_works(self):

        s = StripSessionIds(Settings())

        urls = ["http://preis.de/traeger/index.php?sid=8429fb3ae210a2a0e28800b7f48d90f2",
            "http://preis.de/traeger/index.php?jsessionid=8429fb3ae210a2a0e28800b7f48d90f2",
            "http://preis.de/traeger/index.php?phpsessid=8429fb3ae210a2a0e28800b7f48d90f2",
            "http://preis.de/traeger/index.php?aspsessionid=8429fb3ae210a2a0e28800b7f48d90f2",
        ]

        curi = CrawlUri()
        curi.optional_vars = { CURI_EXTRACTED_URLS: "\n".join(urls) }

        curi = s(curi)
        clean_urls = curi.optional_vars[CURI_EXTRACTED_URLS].split('\n')

        for u in clean_urls:
            self.assertEqual("http://preis.de/traeger/index.php?", u)
