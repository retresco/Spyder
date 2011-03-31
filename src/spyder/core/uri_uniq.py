#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# uri_uniq.py 31-Jan-2011
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
A simple filter for unique uris.
"""

import hashlib


class UniqueUriFilter(object):
    """
    A simple filter for unique uris. This is used to keep the frontier clean.
    """

    def __init__(self, hash_method, depth=3):
        """
        Create a new unique uri filter using the specified `hash_method`.

        `depth` is used to determine the number of nested dictionaries to use.
        Example: using `depth=2` the dictionary storing all hash values use the
        first 2 bytes as keys, i.e. if the hash value is `abc` then

          hashes[a][b] = [c,]

        This should reduce the number of lookups within a dictionary.
        """
        self._hash = hash_method
        self._depth = depth
        self._hashes = dict()

    def is_known(self, url, add_if_unknown=False):
        """
        Test whether the given `url` is known. If not, store it from now on.
        """
        hash_method = hashlib.new(self._hash)
        hash_method.update(url)
        hash_value = hash_method.hexdigest()

        dictionary = self._hashes
        for i in range(0, self._depth):
            if hash_value[i] in dictionary:
                dictionary = dictionary[hash_value[i]]
            else:
                # unknown dict, add it now
                if i == self._depth - 1:
                    dictionary[hash_value[i]] = []
                else:
                    dictionary[hash_value[i]] = dict()
                dictionary = dictionary[hash_value[i]]

        # now dictionary is the list at the deepest level
        if hash_value[self._depth:] in dictionary:
            return True
        else:
            # since we still are here, only the nested list does not
            # contain the given rest. Now we know it
            if add_if_unknown:
                dictionary.append(hash_value[self._depth:])
            return False
