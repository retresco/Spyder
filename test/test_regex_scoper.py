#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# test_regex_scoper.py 24-Jan-2011
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
from spyder.thrift.gen.ttypes import CrawlUri

from spyder.processor.scoper import *

class RegexScoperTest(unittest.TestCase):

    def test_regex_scoper(self):

        curi = CrawlUri()
        curi.optional_vars = dict()
        curi.optional_vars[CURI_EXTRACTED_URLS] = "\n".join([
            "http://www.google.de/index.html",
            "ftp://www.google.de/pillepalle.avi",
        ])

        settings = Settings()
        scoper = create_processor(settings)

        curi = scoper(curi)

        self.assertTrue("http://www.google.de/index.html" in
                curi.optional_vars[CURI_EXTRACTED_URLS])
        self.assertFalse("ftp://www.google.de/pillepalle.avi" in
                curi.optional_vars[CURI_EXTRACTED_URLS])


if __name__ == '__main__':
    unittest.main()
