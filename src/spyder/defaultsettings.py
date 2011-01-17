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

# ZeroMQ

ZEROMQ_WORKER_SOCKET = "ipc:///tmp/spyder-zmq-worker.sock"


# Fetch Processor
USER_AGENT = "Mozilla/5.0 (compatible; spyder/0.1; " + \
    "+http://github.com/retresco/spyder)"
MAX_CLIENTS = 10
MAX_SIMULTANEOUS_CONNECTIONS = 1
FOLLOW_REDIRECTS = True
MAX_REDIRECTS = 1
USE_GZIP = True
