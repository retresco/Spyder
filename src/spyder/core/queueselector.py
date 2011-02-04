#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# queueselector.py 25-Jan-2011
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
A random queue selector.

Based on the number of queues (i.e. `FrontEnd queues`) return a number of the
queue with a bias towards lower numbered queues.
"""

import random


class BiasedQueueSelector(object):
    """
    The default queue selector based on radom selection with bias towards lower
    numbered queues.
    """

    def __init__(self, number_of_queues):
        """
        Initialize the queue selector with the number of available queues.
        """
        self._weights = [1 / (float(i) * number_of_queues)
            for i in range(1, number_of_queues + 1)]
        self._sum_weights = sum(self._weights)
        self._enumerate_weights = [(i, w) for i, w in enumerate(self._weights)]

    def get_queue(self):
        """
        Return the next queue to use.
        """
        random_weight = random.random() * self._sum_weights
        for (i, weight) in self._enumerate_weights:
            random_weight -= weight
            if random_weight < 0:
                return i
