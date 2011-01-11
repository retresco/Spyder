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


ZMQ_SPYDER_TOPIC = 'spyder.'

ZMQ_SPYDER_MGMT_WORKER = ZMQ_SPYDER_TOPIC + 'worker.'

ZMQ_SPYDER_MGMT_WORKER_QUIT = [ZMQ_SPYDER_MGMT_WORKER, 'quit'.encode()]

ZMQ_SPYDER_MGMT_WORKER_QUIT_ACK = [ZMQ_SPYDER_MGMT_WORKER, 'quit.ack'.encode()]
