#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# httpextractor.py 17-Mar-2011
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
"""
Link extractor for detecting links in HTTP codes.

The main use case for this are HTTP redirects, e.g. In the case of a redirect
the HTTP status code ``30X`` is present and the ``Location`` header indicates
the new location.
"""
from urlparse import urlparse

from spyder.core.constants import CURI_EXTRACTED_URLS
from spyder.processor.htmllinkextractor import adapt_relative_link


def create_processor(settings):
    return HttpExtractor(settings)


class HttpExtractor(object):
    """
    The processor for extracting links from ``HTTP`` headers.
    """

    def __init__(self, settings):
        """
        Initialize the extractor.
        """
        pass

    def __call__(self, curi):
        """
        Perform the URL extraction in case of a redirect code.

        I.e. if ``300 <= curi.status_code < 400``, then search for any
        HTTP ``Location`` header and append the given URL to the list of
        extracted URLs.
        """

        if 300 <= curi.status_code < 400 and curi.rep_header and \
            "Location" in curi.rep_header:

            link = curi.rep_header["Location"]

            if link.find("://") == -1:
                # a relative link. this is bad behaviour, but yeah, you know...
                parsed_url = urlparse(curi.url)
                link = "%s://%s%s" % (parsed_url.scheme, parsed_url.netloc,
                    adapt_relative_link(link, parsed_url.path))

            if not hasattr(curi, "optional_vars"):
                curi.optional_vars = dict()

            if not CURI_EXTRACTED_URLS in curi.optional_vars:
                curi.optional_vars[CURI_EXTRACTED_URLS] = link
            else:
                curi.optional_vars[CURI_EXTRACTED_URLS] += "\n" + link

        return curi
