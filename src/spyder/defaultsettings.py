#
# Copyright (c) 2011 Daniel Truemper truemped@googlemail.com
#
# settings.py 10-Jan-2011
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
Module for the default spyder settings.
"""
import logging

import pytz
from datetime import timedelta


# simple settings
LOG_LEVEL_MASTER = logging.DEBUG
LOG_LEVEL_WORKER = logging.DEBUG


# my local timezone
LOCAL_TIMEZONE = pytz.timezone('Europe/Berlin')


# Fetch Processor
USER_AGENT = "Mozilla/5.0 (compatible; spyder/0.1; " + \
    "+http://github.com/retresco/spyder)"
MAX_CLIENTS = 10
MAX_SIMULTANEOUS_CONNECTIONS = 1
FOLLOW_REDIRECTS = False
MAX_REDIRECTS = 3
USE_GZIP = True

# Proxy configuration. Both PROXY_HOST and PROXY_PORT must be set! 
# PROXY_USERNAME and PROXY_PASSWORD are optional
PROXY_HOST = ''
PROXY_PORT = None
PROXY_USERNAME = ''
PROXY_PASSWORD = ''


#
# static dns mappings. Mapping has to be like this:
#    "hostname:port" => ("xxx.xxx.xxx.xxx", port)
#
STATIC_DNS_MAPPINGS = dict()
# Size of the DNS Cache.
SIZE_DNS_CACHE = 1000


# Callback for Master processes.
MASTER_CALLBACK = None
# Interval for the periodic updater (surviving times where nothing is to be
# crawled)
MASTER_PERIODIC_UPDATE_INTERVAL = 60 * 1000


# Frontier implementation to use
FRONTIER_CLASS = 'spyder.core.frontier.SingleHostFrontier'
# Filename storing the frontier state
FRONTIER_STATE_FILE = "./state.db"
# checkpointing interval (uris added/changed)
FRONTIER_CHECKPOINTING = 1000
# The number of URIs to keep inside the HEAP
FRONTIER_HEAP_SIZE = 500
# Minimum number of URIs in the HEAP
FRONTIER_HEAP_MIN = 100
# Download duration times this factor throttles the spyder
FRONTIER_CRAWL_DELAY_FACTOR = 4
# Minimum delay to wait before connecting the host again (s)
FRONTIER_MIN_DELAY = 5

# Number of simultaneously active queues
FRONTIER_ACTIVE_QUEUES = 100
# Number of URLs to be processed in one queue before it is put on hold
FRONTIER_QUEUE_BUDGET = 50
# Punishment of server errors with the queue
FRONTIER_QUEUE_BUDGET_PUNISH = 5


# Name of the prioritizer class to use
PRIORITIZER_CLASS = 'spyder.core.prioritizer.SimpleTimestampPrioritizer'
# The number of priority levels where URIs are being assigned to (lowest means
# highest priority)
PRIORITIZER_NUM_PRIORITIES = 10
# default priority for new urls
PRIORITIZER_DEFAULT_PRIORITY = 1
# Default crawl delta for known urls
PRIORITIZER_CRAWL_DELTA = timedelta(days=1)


# Name of the queue selector to use
QUEUE_SELECTOR_CLASS = 'spyder.core.queueselector.BiasedQueueSelector'


# Name of the queue assignment class to use
QUEUE_ASSIGNMENT_CLASS = 'spyder.core.queueassignment.HostBasedQueueAssignment'


# The pipeline of link extractors
SPYDER_EXTRACTOR_PIPELINE = [
    'spyder.processor.limiter.DefaultLimiter',
    'spyder.processor.htmllinkextractor.DefaultHtmlLinkExtractor',
    'spyder.processor.httpextractor.HttpExtractor',
]


# Default HTML Extractor settings
# maximum number of chars an element name may have
REGEX_LINK_XTRACTOR_MAX_ELEMENT_LENGTH = 64


# The pipeline of scope processors
SPYDER_SCOPER_PIPELINE = [
    'spyder.processor.scoper.RegexScoper',
    'spyder.processor.stripsessions.StripSessionIds',
    'spyder.processor.cleanupquery.CleanupQueryString',
]

# List of positive regular expressions for the crawl scope
REGEX_SCOPE_POSITIVE = [
]

# List of negative regular expressions for the crawl scope
REGEX_SCOPE_NEGATIVE = [
]


# List of 404 redirects
HTTP_EXTRACTOR_404_REDIRECT = [
]


# Whether to remove anchors from extracted urls.
REMOVE_ANCHORS_FROM_LINKS = True


# define a parent directory for unix sockets that will be created
PARENT_SOCKET_DIRECTORY = "/tmp"

#
# improved settings
# only edit if you are usually working behind a nuclear power plant's control
# panel

# ZeroMQ Master Push
ZEROMQ_MASTER_PUSH = "ipc://%s/spyder-zmq-master-push.sock" % \
    PARENT_SOCKET_DIRECTORY
ZEROMQ_MASTER_PUSH_HWM = 10

# ZeroMQ Fetcher
ZEROMQ_WORKER_PROC_FETCHER_PULL = ZEROMQ_MASTER_PUSH
ZEROMQ_WORKER_PROC_FETCHER_PUSH = "inproc://processing/fetcher/push"
ZEROMQ_WORKER_PROC_FETCHER_PUSH_HWM = 10

# ZeroMQ Extractor
ZEROMQ_WORKER_PROC_EXTRACTOR_PULL = ZEROMQ_WORKER_PROC_FETCHER_PUSH
ZEROMQ_WORKER_PROC_EXTRACTOR_PUB = "ipc://%s/spyder-zmq-master-sub.sock" % \
    PARENT_SOCKET_DIRECTORY
ZEROMQ_WORKER_PROC_EXTRACTOR_PUB_HWM = 10

# ZeroMQ Master Sub
ZEROMQ_MASTER_SUB = ZEROMQ_WORKER_PROC_EXTRACTOR_PUB

# ZeroMQ Management Sockets
ZEROMQ_MGMT_MASTER = "ipc://%s/spyder-zmq-mgmt-master.sock" % \
    (PARENT_SOCKET_DIRECTORY,)
ZEROMQ_MGMT_WORKER = "ipc://%s/spyder-zmq-mgmt-worker.sock" % \
    (PARENT_SOCKET_DIRECTORY,)

# ZeroMQ logging socket
ZEROMQ_LOGGING = "ipc://%s/spyder-logging.sock" % (PARENT_SOCKET_DIRECTORY,)
