#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# test_queue_assignment.py 31-Mar-2011
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
import unittest

from spyder.core.settings import Settings
from spyder.core.dnscache import DnsCache
from spyder.core.queueassignment import HostBasedQueueAssignment
from spyder.core.queueassignment import IpBasedQueueAssignment

class HostBasedQueueAssignmentTest(unittest.TestCase):

    def test_host_based_assignment(self):

        s = Settings()
        dns = DnsCache(s)
        assign = HostBasedQueueAssignment(dns)

        url = "http://www.google.com/pille/palle"
        self.assertEqual("www.google.com", assign.get_identifier(url))



class IpBasedQueueAssignmentTest(unittest.TestCase):

    def test_ip_based_assignment(self):

        s = Settings()
        dns = DnsCache(s)
        assign = IpBasedQueueAssignment(dns)

        url = "http://localhost:12345/this"
        self.assertEqual("127.0.0.1", assign.get_identifier(url))

if __name__ == '__main__':
    unittest.main()
