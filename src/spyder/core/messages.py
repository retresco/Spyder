#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# messages.py 14-Jan-2011
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

    def __init__(self, message=None, key=None, identity=None, data=None):
        """
        Construct a new message and if given parse the serialized message.
        """
        if message is not None:
            self.key = message[0]
            self.identity = message[1]
            self.data = message[2]
        elif key is not None or identity is not None or data is not None:
            self.key = key
            self.identity = identity
            self.data = data
        else:
            self.key = self.identity = self.data = None

    def serialize(self):
        """
        Return a new message envelope from the class members.
        """
        return [self.key, self.identity, self.data]

    def __eq__(self, other):
        return (self.key == other.key
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


def serialize_date_time(dt):
    """
    Create a string of the datetime.
    """
    return GMT.localize(dt).strftime(SERVER_TIME_FORMAT)


def deserialize_date_time(date_string):
    """
    Read a string as a datetime.
    """
    return datetime.strptime(date_string, SERVER_TIME_FORMAT)
