#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# prioritizer.py 01-Feb-2011
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
#
"""
URL prioritizers will calculate priorities of new URLs and the recrawling
priority.
"""


class SimpleTimestampPrioritizer(object):
    """
    A simple prioritizer where the priority is based on the timestamp of the
    next scheduled crawl of the URL.
    """

    def __init__(self, settings):
        """
        Initialize the number of available priorities and the priority delta
        between the priorities.
        """
        self._priorities = settings.PRIORITIZER_NUM_PRIORITIES
        self._default_priority = settings.PRIORITIZER_DEFAULT_PRIORITY
        self._delta = settings.PRIORITIZER_CRAWL_DELTA

    def calculate_priority(self, curi):
        """
        Calculate the new priority based on the :class:`CrawlUri`s current.

        This should return a tuple of
            (prio_level, prio)
        """
        if curi.current_priority:
            prio_level = min(curi.current_priority + 1, self._priorities)
        else:
            prio_level = 0
        prio = self._delta * prio_level
        return (prio_level, prio)
