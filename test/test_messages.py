#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_messages.py 14-Jan-2011
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

from spyder.core.messages import DataMessage, MgmtMessage
from spyder.core.messages import serialize_crawl_uri, deserialize_crawl_uri
from spyder.thrift.gen.ttypes import CrawlUri

class TestMessages(unittest.TestCase):

    def test_that_serialization_works(self):
    
        curi = CrawlUri(url="http://localhost")

        serialized = serialize_crawl_uri(curi)
        deserialized = deserialize_crawl_uri(serialized)

        self.assertEqual(curi, deserialized)

    def test_that_data_messages_work(self):
        identity = "me myself and i"
        curi = CrawlUri(url="http://localhost")
        serialized = serialize_crawl_uri(curi)

        msg = DataMessage([identity, serialized])

        self.assertEqual(identity, msg.identity)
        self.assertEqual(curi, msg.curi)
        self.assertEqual([identity, serialized], msg.serialize())
        self.assertEqual(msg, DataMessage(msg.serialize()))

    def test_that_mgmt_messages_work(self):
        topic = "me"
        identity = "myself"
        data = "and i"

        msg = MgmtMessage([topic, identity, data])

        self.assertEqual(topic, msg.topic)
        self.assertEqual(identity, msg.identity)
        self.assertEqual(data, msg.data)
        self.assertEqual([topic, identity, data], msg.serialize())
        self.assertEqual(msg, MgmtMessage(msg.serialize()))

    def test_that_construction_works(self):
        msg = DataMessage(identity="me")
        self.assertEqual("me", msg.identity)
        self.assertEqual(None, msg.curi)

        msg = DataMessage(curi="bla")
        self.assertEqual("bla", msg.curi)
        self.assertEqual(None, msg.identity)


if __name__ == '__main__':
    unittest.main()
