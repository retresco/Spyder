#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# queueassignment.py 14-Mar-2011
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
A collection of queue assignment classes.
"""
from urlparse import urlparse

from spyder.core.frontier import PROTOCOLS_DEFAULT_PORT


class HostBasedQueueAssignment(object):
    """
    This class will assign URLs to queues based on the hostnames.
    """

    def __init__(self, dnscache):
        """
        Initialize the assignment class.
        """
        self._dns_cache = dnscache

    def get_identifier(self, url):
        """
        Get the identifier for this url.
        """
        parsed_url = urlparse(url)
        return parsed_url.hostname


class IpBasedQueueAssignment(HostBasedQueueAssignment):
    """
    This class will assign urls to queues based on the server's IP address.
    """

    def __init__(self, dnscache):
        """
        Call the parent only.
        """
        HostBasedQueueAssignment.__init__(self, dnscache)

    def get_identifier(self, url):
        """
        Get the identifier for this url.
        """
        parsed_url = urlparse(url)

        # dns resolution and caching
        port = parsed_url.port
        if not port:
            port = PROTOCOLS_DEFAULT_PORT[parsed_url.scheme]

        (ip, port) = self._dns_cache["%s:%s" % (parsed_url.hostname, port)]

        return "%s" % (ip,)
