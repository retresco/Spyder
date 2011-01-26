#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# dnscache.py 24-Jan-2011
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
A very simple dns cache.

Currently dns resolution is blocking style but this should get a nonblocking
version.
"""

import socket

from brownie.caching import LRUCache as LRUDict


class DnsCache(object):
    """
    A simple DNS Cache.
    """

    def __init__(self, max_size=10):
        """
        Initialize the cache.
        """
        self._cache = LRUDict(maxsize=max_size)

    def __getitem__(self, host_port_string):
        """
        Retrieve the item from the cache or resolve the hostname and store the
        result in the cache.

        Returns a tuple of `(ip, port)`. At the moment we only support IPv4 but
        this will probably change in the future.
        """
        if host_port_string not in self._cache:
            (hostname, port) = host_port_string.split(":")
            infos = socket.getaddrinfo(hostname, port, 0, 0, socket.SOL_TCP)
            for (family, socktype, proto, canoname, sockaddr) in infos:
                if len(sockaddr) == 2:
                    # IPv4 (which we prefer)
                    self._cache[host_port_string] = sockaddr

        return self._cache[host_port_string]
