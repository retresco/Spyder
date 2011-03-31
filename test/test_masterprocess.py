#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_masterprocess.py 07-Feb-2011
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
import logging
import unittest

import sys

from spyder.core.settings import Settings
from spyder import masterprocess


class MasterProcessTest(unittest.TestCase):

    def test_create_frontier_works(self):

        handler = logging.StreamHandler(sys.stdout)
        s = Settings()
        s.FRONTIER_STATE_FILE = ":memory:"

        frontier = masterprocess.create_frontier(s, handler)

        self.assertTrue(frontier is not None)
