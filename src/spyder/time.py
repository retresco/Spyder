#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# time.py 15-Feb-2011
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
Time related utilities.
"""
from datetime import datetime

import pytz

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
