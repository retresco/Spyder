#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# sink.py 02-Feb-2011
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
A sink of :class:`CrawlUri`.
"""


class AbstractCrawlUriSink(object):
    """
    Abstract sink. Only overwrite the methods you are interested in.
    """

    def process_successful_crawl(self, curi):
        """
        We have crawled a uri successfully. If there are newly extracted links,
        add them alongside the original uri to the frontier.
        """
        pass

    def process_not_found(self, curi):
        """
        The uri we should have crawled was not found, i.e. HTTP Error 404. Do
        something with that.
        """
        pass

    def process_redirect(self, curi):
        """
        There have been too many redirects, i.e. in the default config there
        have been more than 3 redirects.
        """
        pass

    def process_server_error(self, curi):
        """
        There has been a server error, i.e. HTTP Error 50x. Maybe we should try
        to crawl this uri again a little bit later.
        """
        pass
