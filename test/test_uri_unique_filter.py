#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# test_uri_unique_filter.py 31-Jan-2011
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

from spyder.core.uri_uniq import UniqueUriFilter

class UniqueUriFilterTest(unittest.TestCase):

    def test_unknown_uris(self):

        unique_filter = UniqueUriFilter('sha1')

        self.assertFalse(unique_filter.is_known("http://www.google.de",
                    add_if_unknown=True))
        self.assertFalse(unique_filter.is_known("http://www.yahoo.com",
                    add_if_unknown=True))
        self.assertTrue(unique_filter.is_known("http://www.google.de"))
        self.assertTrue(unique_filter.is_known("http://www.yahoo.com"))


if __name__ == '__main__':
    unittest.main()
