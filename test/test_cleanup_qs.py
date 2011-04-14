#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# test_cleanup_qs.py 14-Apr-2011
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

from spyder.core.settings import Settings
from spyder.processor.cleanupquery import CleanupQueryString


class CleanupQueryStringTest(unittest.TestCase):

    def test_that_cleaning_qs_works(self):
        s = Settings()
        c = CleanupQueryString(s)

        self.assertEqual("http://test.com/t.html?p=a",
                c._cleanup_query_string("http://test.com/t.html?p=a#top"))

        self.assertEqual("http://test.com/t.html",
                c._cleanup_query_string("http://test.com/t.html?#top"))

        self.assertEqual("http://test.com/t.html?test=a",
                c._cleanup_query_string("http://test.com/t.html?test=a&"))
