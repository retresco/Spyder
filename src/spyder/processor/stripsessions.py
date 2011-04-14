#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# stripsessions.py 14-Apr-2011
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
Processor to strip all session ids from the extracted URLs. It should be placed
at the very end of the scoper chain in order to process only those URLs that
are relevant for the crawl.

It basically searches for

   sid=
   jsessionid=
   phpsessionid=
   aspsessionid=
"""
from spyder.core.constants import CURI_EXTRACTED_URLS


def create_processor(settings):
    return StripSessionIds(settings)


class StripSessionIds(object):
    """
    The processor for removing session information from the query string.
    """

    def __init__(self, settings):
        """
        Initialize me.
        """
        self._session_params = ['jsessionid=', 'phpsessid=',
            'aspsessionid=', 'sid=']

    def __call__(self, curi):
        """
        Main method stripping the session stuff from the query string.

        @param curi: :class:`CrawlUri`
        @return: the updated curi
        """
        if CURI_EXTRACTED_URLS not in curi.optional_vars:
            return curi

        urls = []
        for raw_url in curi.optional_vars[CURI_EXTRACTED_URLS].split('\n'):
            urls.append(self._remove_session_ids(raw_url))

        curi.optional_vars[CURI_EXTRACTED_URLS] = "\n".join(urls)
        return curi

    def _remove_session_ids(self, raw_url):
        """
        Remove the session information.

        @param raw_url: The potentially dirty URL
        @return: The clean URL
        """
        for session in self._session_params:
            url = raw_url.lower()
            begin = url.find(session)
            while begin > -1:
                end = url.find('&', begin)
                if end == -1:
                    raw_url = raw_url[:begin]
                else:
                    raw_url = "%s%s" % (raw_url[:begin], raw_url[end:])
                url = raw_url.lower()
                begin = url.find(session)

        return url
