#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# messages.py 14-Jan-2011
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
#
#
"""
Definitions of messages that are being sent via ZeroMQ Sockets.

Plus some (de-)serialization helpers.
"""

from datetime import datetime
import pytz

from  thrift import TSerialization

from spyder.thrift.gen.ttypes import CrawlUri


class DataMessage(object):
    """
    Envelope class describing `data` messages.
    """

    def __init__(self, message=None, identity=None, curi=None):
        """
        Construct a new message.
        """
        if message is not None:
            self.identity = message[0]
            self.serialized_curi = message[1]
            self.curi = deserialize_crawl_uri(message[1])
        elif identity is not None or curi is not None:
            self.identity = identity
            self.curi = curi
        else:
            self.identity = self.curi = None

    def serialize(self):
        """
        Return a new message envelope from the class members.
        """
        return [self.identity, serialize_crawl_uri(self.curi)]

    def __eq__(self, other):
        return (self.identity == other.identity
            and self.curi == other.curi)


class MgmtMessage(object):
    """
    Envelope class describing `management` messages.
    """

    def __init__(self, message=None, topic=None, identity=None, data=None):
        """
        Construct a new message and if given parse the serialized message.
        """
        if message is not None:
            self.topic = message[0]
            self.identity = message[1]
            self.data = message[2]
        elif topic is not None or identity is not None or data is not None:
            self.topic = topic
            self.identity = identity
            self.data = data
        else:
            self.topic = self.identity = self.data = None

    def serialize(self):
        """
        Return a new message envelope from the class members.
        """
        return [self.topic, self.identity, self.data]

    def __eq__(self, other):
        return (self.topic == other.topic
            and self.identity == other.identity
            and self.data == other.data)


def deserialize_crawl_uri(serialized):
    """
    Deserialize a `CrawlUri` that has been serialized using Thrift.
    """
    return TSerialization.deserialize(CrawlUri(), serialized)


def serialize_crawl_uri(crawl_uri):
    """
    Serialize a `CrawlUri` using Thrift.
    """
    return TSerialization.serialize(crawl_uri)


SERVER_TIME_FORMAT = "%a, %d %b %Y %H:%M:%S %Z"
GMT = pytz.timezone('GMT')


def serialize_date_time(date_time):
    """
    Create a string of the datetime.
    """
    return GMT.localize(date_time).strftime(SERVER_TIME_FORMAT)


def deserialize_date_time(date_string):
    """
    Read a string as a datetime.
    """
    return datetime.strptime(date_string, SERVER_TIME_FORMAT)
