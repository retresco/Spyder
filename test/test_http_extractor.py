#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_http_extractor.py 17-Mar-2011
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

from spyder.core.constants import CURI_EXTRACTED_URLS
from spyder.core.settings import Settings
from spyder.processor.httpextractor import HttpExtractor
from spyder.thrift.gen.ttypes import CrawlUri


class HttpExtractorTest(unittest.TestCase):

    def test_correct_extraction(self):

        s = Settings()

        curi = CrawlUri("http://localhost")
        curi.status_code = 302
        curi.rep_header = {"Location": "http://localhost/index.html"}
        curi.optional_vars = dict()

        xtor = HttpExtractor(s)
        curi = xtor(curi)

        self.assertTrue(CURI_EXTRACTED_URLS in curi.optional_vars)
        self.assertEquals("http://localhost/index.html",
                curi.optional_vars[CURI_EXTRACTED_URLS])

    def test_only_on_redirect(self):

        s = Settings()

        curi = CrawlUri("http://localhost")
        curi.status_code = 200
        curi.rep_header = {"Location": "http://localhost/index.html"}
        curi.optional_vars = dict()

        xtor = HttpExtractor(s)
        curi = xtor(curi)

        self.assertFalse(CURI_EXTRACTED_URLS in curi.optional_vars)

    def test_relative_links(self):

        s = Settings()

        curi = CrawlUri("http://localhost")
        curi.status_code = 303
        curi.rep_header = {"Location": "/index.html"}
        curi.optional_vars = dict()

        xtor = HttpExtractor(s)
        curi = xtor(curi)

        self.assertTrue(CURI_EXTRACTED_URLS in curi.optional_vars)
        self.assertEquals("http://localhost/index.html",
                curi.optional_vars[CURI_EXTRACTED_URLS])
