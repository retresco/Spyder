#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# constants.py 10-Jan-2011
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
Serveral constants mainly for ZeroMQ topics and messages.
"""

# general topic for spyder related management tasks
ZMQ_SPYDER_MGMT = 'spyder.'

ZMQ_SPYDER_MGMT_WORKER = ZMQ_SPYDER_MGMT + 'worker.'

ZMQ_SPYDER_MGMT_WORKER_QUIT = 'quit'.encode()
ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK = 'quit.ack'.encode()

# constants used in the optional_vars map of CrawlUris
CURI_OPTIONAL_TRUE = "1".encode()
CURI_OPTIONAL_FALSE = "0".encode()

# username and password fields
CURI_SITE_USERNAME = "username".encode()
CURI_SITE_PASSWORD = "password".encode()

# extraction finished field
CURI_EXTRACTION_FINISHED = "extraction_finished".encode()

# extracted urls field
CURI_EXTRACTED_URLS = "extracted_urls".encode()
