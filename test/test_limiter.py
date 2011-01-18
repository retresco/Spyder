#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_limiter.py 18-Jan-2011
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

from spyder.core.constants import CURI_EXTRACTION_FINISHED, CURI_OPTIONAL_TRUE
from spyder.processor import limiter
from spyder.thrift.gen.ttypes import CrawlUri


class LimiterTestCase(unittest.TestCase):

    def test_create_processor_works(self):
        proc = limiter.create_processor(None)
        self.assertEqual(limiter.do_not_process_robots, proc)

    def test_do_not_process_robots_works(self):

        curi = CrawlUri()
        curi.effective_url = "http://127.0.0.1/robots.txt"
        curi.optional_vars = dict()

        for i in range(2):
            limiter.do_not_process_robots(curi)
            self.assertEqual(CURI_OPTIONAL_TRUE,
                    curi.optional_vars[CURI_EXTRACTION_FINISHED])
