#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# htmlextractor.py 21-Jan-2011
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# under the License.
# All programs in this directory and
# subdirectories are published under the GNU General Public License as
# described below.
#
#
"""
Module for the default HTML Link extractor.

Most of the regular expressions have been adopted from Heritrix. See:
Heritrix 3:
    modules/src/main/java/org/archive/modules/extractor/ExtractorHTML.java
"""

import re

from urlparse import urlparse

from spyder.core.constants import CURI_EXTRACTED_URLS

# Maximum number of chars an element name may have
MAX_ELEMENT_REPLACE = "MAX_ELEMENT_REPLACE"

# Pattern for extracting relevant tags from HTML
#
# This pattern extracts:
#  1: <script>...</script>
#  2: <style>...</style>
#  3: <meta...>
#  4: any other open tag with at least one attribute
#     (eg matches "<a href='boo'>" but not "</a>" or "<br>")
#
# Groups in this pattern:
#
#  1: script src=foo>boo</script
#  2: just the script open tag
#  3: style type=moo>zoo</style
#  4: just the style open tag
#  5: entire other tag, without '<' '>'
#  6: element
#  7: meta
#  8: !-- comment --
RELEVANT_TAG_EXTRACTOR = "<(?:((script[^>]*)>[^(</script)]*</script)" + "|" + \
    "((style[^/]*)>[^(</style)]*</style)" + "|" + \
    "(((meta)|(?:\\w{1,MAX_ELEMENT_REPLACE}))\\s+[^>]*)" + "|" + \
    "(!--.*?--))>"


# The simpler pattern to extract links from tags
#
# Groups in this expression:
#
#  1: the attribute name
#  2: href | src
#  3: the url in quotes
LINK_EXTRACTOR = "(\w+)[^>]*?(?:(href|src))\s*=\s*" + \
    "(?:(\"[^\"]+\"|'[^']+'))"


class DefaultHtmlLinkExtractor(object):
    """
    The default extractor for Links from HTML pages.
    """

    def __init__(self, settings):
        """
        Initialize the regular expressions.
        """
        max_size = settings.REGEX_LINK_XTRACTOR_MAX_ELEMENT_LENGTH
        self._tag_extractor = re.compile(
                RELEVANT_TAG_EXTRACTOR.replace(MAX_ELEMENT_REPLACE,
                    str(max_size)), re.I|re.S)

        self._link_extractor = re.compile(LINK_EXTRACTOR, re.I|re.S)

    def __call__(self, curi):
        """
        Actually extract links from the html content.

        But: only if the content type allows us to do so.
        """
        if not self._restrict_content_type(curi):
            return curi

        content = curi.content_body

        # iterate over all tags
        for tag in self._tag_extractor.finditer(content):

            if tag.start(8) > 0:
                # a html comment, ignore
                continue

            elif tag.start(7) > 0:
                # a meta tag
                curi = self._process_meta(curi, content[tag.start(5):tag.end(5)])

            elif tag.start(5) > 0:
                # generic <whatever tag
                element_name = content[tag.start(6):tag.end(6)]
                element = content[tag.start(5):tag.end(5)]
                curi = self._process_generic_tag(curi, element_name, element)

            elif tag.start(1) > 0:
                # <script> tag
                # TODO no script handling so far
                pass

            elif tag.start(3) > 0:
                # <style> tag
                # TODO no tag handling so far
                pass

        return curi

    def _process_generic_tag(self, curi, element_name, element):
        """
        Process a generic tag.

        This can be anything but `meta`, `script` or `style` tags.
        """
        if "a" == element_name:
            curi = self._extract_links(curi, element)

        return curi
        
    def _extract_links(self, curi, element):
        """
        Extract links from an element, e.g. href="" attributes.
        """
        parsed_url = urlparse(curi.url)
        links = []
        for link in self._link_extractor.finditer(element):
            l = link.group(3)[1:-1]
            if "://" not in l:
                # a relative link
                if l.startswith('/'):
                    l = parsed_url.scheme + "://" + parsed_url.netloc + l
                else:
                    if not parsed_url.path.endswith('/'):
                        l = parsed_url.scheme + "://" + \
                            parsed_url.netloc + parsed_url.path + '/' + \
                            l
                    else:
                        l = parsed_url.scheme + "://" + \
                            parsed_url.netloc + parsed_url.path + l
            links.append(l)

        if not CURI_EXTRACTED_URLS in curi.optional_vars:
            curi.optional_vars[CURI_EXTRACTED_URLS] = links
        else:
            curi.optional_vars[CURI_EXTRACTED_URLS].extend(links)

        return curi

    def _process_meta(self, curi, meta_tag):
        """
        Process a meta tag.
        """
        return curi

    def _restrict_content_type(self, curi):
        """
        Decide based on the `CrawlUri`s Content-Type whether we want to process
        it.
        """
        allowed = ["text/html", "application/xhtml", "text/vnd.wap.wml",
            "application/vnd.wap.wml", "application/vnd.wap.xhtm"]
        return curi.rep_header["Content-Type"].lower() in allowed


def create_processor(settings):
    """
    Create a new :class:`DefaultHtmlLinkExtractor`.
    """
    return DefaultHtmlLinkExtractor(settings)
