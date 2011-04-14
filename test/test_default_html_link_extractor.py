#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# test_default_html_link_extractor.py 21-Jan-2011
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

import unittest

from spyder.core.constants import CURI_EXTRACTED_URLS
from spyder.core.settings import Settings
from spyder.processor.htmllinkextractor import DefaultHtmlLinkExtractor
from spyder.thrift.gen.ttypes import CrawlUri


class HtmlLinkExtractorTest(unittest.TestCase):

    def test_that_content_type_restriction_works(self):
        xtor = DefaultHtmlLinkExtractor(Settings())

        curi = CrawlUri()
        curi.rep_header = dict()
        curi.rep_header["Content-Type"] = "text/html"
        self.assertTrue(xtor._restrict_content_type(curi))
        curi.rep_header["Content-Type"] = "pille/palle"
        self.assertFalse(xtor._restrict_content_type(curi))

    def test_link_extraction_works(self):

        src = "<a href='http://www.google.de' title='ups'> viel text</a>" + \
            "<a title='ups i did it again' href ='/relative.html'>und " + \
            "noch mehr!</a><a href='evenmorerelative.html'>"

        curi = CrawlUri()
        curi.rep_header = dict()
        curi.rep_header["Content-Type"] = "text/html; charset=utf-8"
        curi.url = "http://www.bmg.bund.de/test/"
        curi.content_body = src
        curi.optional_vars = dict()

        xtor = DefaultHtmlLinkExtractor(Settings())
        curi = xtor(curi)

        links = curi.optional_vars[CURI_EXTRACTED_URLS].split("\n")
        self.assertEqual("http://www.google.de", links[0])
        self.assertEqual("http://www.bmg.bund.de/relative.html", links[1])
        self.assertEqual("http://www.bmg.bund.de/test/evenmorerelative.html",
                links[2])

    def test_missing_encoding_works(self):
        src = "<a href='http://www.google.de' title='ups'> viel text</a>" + \
            "<a title='ups i did it again' href ='/relative.html'>und " + \
            "noch mehr!</a><a href='evenmorerelative.html'>"

        curi = CrawlUri()
        curi.rep_header = dict()
        curi.rep_header["Content-Type"] = "text/html"
        curi.url = "http://www.bmg.bund.de/test/"
        curi.content_body = src
        curi.optional_vars = dict()

        xtor = DefaultHtmlLinkExtractor(Settings())
        curi = xtor(curi)

        links = curi.optional_vars[CURI_EXTRACTED_URLS].split("\n")
        self.assertEqual("http://www.google.de", links[0])
        self.assertEqual("http://www.bmg.bund.de/relative.html", links[1])
        self.assertEqual("http://www.bmg.bund.de/test/evenmorerelative.html",
                links[2])


if __name__ == '__main__':
    unittest.main()
