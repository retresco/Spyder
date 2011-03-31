#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# logging.py 04-Feb-2011
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
A simple pyzmq logging mixin.
"""

import logging


class LoggingMixin:
    """
    Simple mixin for adding logging methods to a class.
    """

    def __init__(self, pub_handler, log_level):
        """
        Initialize the logger.
        """
        self._logger = logging.getLogger()
        self._logger.addHandler(pub_handler)
        self._logger.setLevel(log_level)
