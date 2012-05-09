#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# cleanupquery.py 14-Apr-2011
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
"""
Processor to clean up the query string. At this point we want to strip any
trailing '?' or '&' and optionally remove any anchors from it.
"""
from spyder.core.constants import CURI_EXTRACTED_URLS


class CleanupQueryString(object):
    """
    The processor for cleaning up the query string.
    """

    def __init__(self, settings):
        """
        Initialize me.
        """
        self._remove_anchors = settings.REMOVE_ANCHORS_FROM_LINKS

    def __call__(self, curi):
        """
        Remove any obsolete stuff from the query string.
        """
        if CURI_EXTRACTED_URLS not in curi.optional_vars:
            return curi

        urls = []
        for raw_url in curi.optional_vars[CURI_EXTRACTED_URLS].split('\n'):
            urls.append(self._cleanup_query_string(raw_url))

        curi.optional_vars[CURI_EXTRACTED_URLS] = "\n".join(urls)
        return curi

    def _cleanup_query_string(self, raw_url):
        """
        """
        url = raw_url
        if self._remove_anchors:
            begin = raw_url.find("#")
            if begin > -1:
                url = raw_url[:begin]

        if len(url) == 0:
            return raw_url

        while url[-1] == '?' or url[-1] == '&':
            return url[:-1]

        return url
