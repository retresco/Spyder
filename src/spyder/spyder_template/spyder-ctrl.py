#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# spyder.py 02-Feb-2011
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

import sys

import spyder

try:
    import settings
except ImportError:
    print >> sys.stderr, \
        """Cannot find settings.py in the directory containing %s""" % __file__
    sys.exit(1)


if __name__ == "__main__":
    spyder.spyder_management(settings)
