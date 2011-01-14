#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# test_messages.py 14-Jan-2011
#
# All programs in this directory and
# subdirectories are published under the GNU General Public License as
# described below.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or (at
# your option) any later version.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307
# USA
#
# Further information about the GNU GPL is available at:
# http://www.gnu.org/copyleft/gpl.html
#
#

import unittest

from spyder.core.messages import DataMessage, MgmtMessage
from spyder.core.messages import serialize_crawl_uri, deserialize_crawl_uri
from spyder.thrift.gen.ttypes import CrawlUri

class TestMessages(unittest.TestCase):

    def test_that_serialization_works(self):
    
        curi = CrawlUri(url="http://localhost", host_identifier="127.0.0.1")

        serialized = serialize_crawl_uri(curi)
        deserialized = deserialize_crawl_uri(serialized)

        self.assertEqual(curi, deserialized)

    def test_that_data_messages_work(self):
        identity = "me myself and i"
        curi = CrawlUri(url="http://localhost", host_identifier="127.0.0.1")
        serialized = serialize_crawl_uri(curi)

        msg = DataMessage([identity, serialized])

        self.assertEqual(identity, msg.identity)
        self.assertEqual(curi, msg.curi)
        self.assertEqual([identity, serialized], msg.serialize())
        self.assertEqual(msg, DataMessage(msg.serialize()))

    def test_that_mgmt_messages_work(self):
        key = "me"
        identity = "myself"
        data = "and i"

        msg = MgmtMessage([key, identity, data])

        self.assertEqual(key, msg.key)
        self.assertEqual(identity, msg.identity)
        self.assertEqual(data, msg.data)
        self.assertEqual([key, identity, data], msg.serialize())
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
