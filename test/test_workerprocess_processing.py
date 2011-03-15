#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# test_workerprocess_processing.py 18-Jan-2011
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

from spyder.core.constants import CURI_OPTIONAL_TRUE
from spyder.core.constants import CURI_EXTRACTION_FINISHED
from spyder.core.settings import Settings
from spyder.processor import limiter
from spyder.thrift.gen.ttypes import CrawlUri
from spyder import workerprocess


class WorkerProcessingUnittest(unittest.TestCase):

    def test_that_creating_processing_function_works(self):
        settings = Settings()
        processors = settings.SPYDER_SCOPER_PIPELINE
        processors.append('test_workerprocess')
        self.assertRaises(ValueError, workerprocess.create_processing_function,
                settings, processors)

        processors.pop()
        processors.append('test_workerprocess_unspec')
        self.assertRaises(ValueError, workerprocess.create_processing_function,
                settings, processors)

        processors.pop()
        processing = workerprocess.create_processing_function(settings,
                processors)

        curi = CrawlUri(optional_vars=dict())
        curi.effective_url = "http://127.0.0.1/robots.txt"
        curi2 = processing(curi)

        self.assertEqual(CURI_OPTIONAL_TRUE,
                curi2.optional_vars[CURI_EXTRACTION_FINISHED])
