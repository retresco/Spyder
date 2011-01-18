#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_workerprocess_processing.py 18-Jan-2011
#
# All programs in this directory and
# subdirectories are published under the GNU General Public License as
# described below.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at
# your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
# USA
#
# Further information about the GNU GPL is available at:
# http://www.gnu.org/copyleft/gpl.html
#
#

import unittest
from mockito import when

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
        processing = workerprocess.create_processing_function(settings,
                processors)

        curi = CrawlUri(optional_vars=dict())
        curi.effective_url = "http://127.0.0.1/robots.txt"
        curi2 = processing(curi)

        self.assertEqual(CURI_OPTIONAL_TRUE,
                curi2.optional_vars[CURI_EXTRACTION_FINISHED])
