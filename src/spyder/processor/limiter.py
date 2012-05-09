#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# limiter.py 18-Jan-2011
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
"""
A processor used for limiting the extraction and scoping processings.

Basically this will be used for ignoring any `robots.txt` for being processed.
"""

from spyder.core.constants import CURI_OPTIONAL_TRUE, CURI_EXTRACTION_FINISHED


class DefaultLimiter(object):
    """
    The default crawl limiter.
    """

    def __init__(self, settings):
        """
        Initialize the limiter with the given settings.
        """
        pass

    def __call__(self, curi):
        """
        Do the actual limiting.
        """
        return self._do_not_process_robots(curi)

    def _do_not_process_robots(self, curi):
        """
        Do not process `CrawlUris` if they are **robots.txt** files.
        """
        if CURI_EXTRACTION_FINISHED not in curi.optional_vars and \
            curi.effective_url.endswith("robots.txt"):
            curi.optional_vars[CURI_EXTRACTION_FINISHED] = CURI_OPTIONAL_TRUE

        return curi
