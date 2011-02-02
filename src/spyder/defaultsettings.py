#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# settings.py 10-Jan-2011
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
Module for the default spyder settings.
"""

# simple settings

# Fetch Processor
USER_AGENT = "Mozilla/5.0 (compatible; spyder/0.1; " + \
    "+http://github.com/retresco/spyder)"
MAX_CLIENTS = 10
MAX_SIMULTANEOUS_CONNECTIONS = 1
FOLLOW_REDIRECTS = True
MAX_REDIRECTS = 3
USE_GZIP = True


# Filename storing the frontier state
FRONTIER_STATE_FILE = "./state.db"
# The number of simultaneous hosts to crawl
FRONTIER_SIMULTANEOUS_HOSTS = 100
# The number of URIs to keep inside the HEAP
FRONTIER_HEAP_SIZE = 500
# Minimum number of URIs in the HEAP
FRONTIER_HEAP_MIN = 100
# Size of the DNS Cache
FRONTIER_SIZE_DNS_CACHE = 1000

# The number of priority levels where URIs are being assigned to (lowest means
# highest priority)
PRIORITIZER_NUM_PRIORITIES = 10
# default priority for new urls
PRIORITIZER_DEFAULT_PRIORITY = 1

# The pipeline of link extractors
SPYDER_EXTRACTOR_PIPELINE = [
    'spyder.processor.limiter',
    'spyder.processor.htmllinkextractor',
]

# Default HTML Extractor settings
# maximum number of chars an element name may have
REGEX_LINK_XTRACTOR_MAX_ELEMENT_LENGTH = 64


# The pipeline of scope processors
SPYDER_SCOPER_PIPELINE = [
    'spyder.processor.limiter',
    'spyder.processor.scoper',
]

# List of positive regular expressions for the crawl scope
REGEX_SCOPE_POSITIVE = [
    "http://[^/]+/.*\.html",
]

# List of negative regular expressions for the crawl scope
REGEX_SCOPE_NEGATIVE = [
    "ftp://[^/]+/.*\.avi",
]


#
# improved settings
# only edit if you are usually working behind a nuclear power plant's control
# panel

# ZeroMQ Master Push
ZEROMQ_MASTER_PUSH = "ipc:///tmp/spyder-zmq-master-push.sock"
ZEROMQ_MASTER_PUSH_HWM = 10

# ZeroMQ Fetcher
ZEROMQ_WORKER_PROC_FETCHER_PULL = ZEROMQ_MASTER_PUSH
ZEROMQ_WORKER_PROC_FETCHER_PUSH = "inproc://processing/fetcher/push"
ZEROMQ_WORKER_PROC_FETCHER_PUSH_HWM = 10

# ZeroMQ Extractor
ZEROMQ_WORKER_PROC_EXTRACTOR_PULL = ZEROMQ_WORKER_PROC_FETCHER_PUSH
ZEROMQ_WORKER_PROC_EXTRACTOR_PUSH = "inproc://processing/extractor/push"
ZEROMQ_WORKER_PROC_EXTRACTOR_PUSH_HWM = 10

# ZeroMQ Scoper
ZEROMQ_WORKER_PROC_SCOPER_PULL = ZEROMQ_WORKER_PROC_EXTRACTOR_PUSH
ZEROMQ_WORKER_PROC_SCOPER_PUB = "ipc:///tmp/spyder-zmq-master-sub.sock"

# ZeroMQ Master Sub
ZEROMQ_MASTER_SUB = ZEROMQ_WORKER_PROC_SCOPER_PUB

# ZeroMQ Management Sockets
ZEROMQ_MGMT_MASTER = "ipc:///tmp/spyder-zmq-mgmt-master.sock"
ZEROMQ_MGMT_WORKER = "ipc:///tmp/spyder-zmq-mgmt-worker.sock"
