#
# Copyright (c) 2010 Daniel Truemper truemped@googlemail.com
#
# import_util.py 07-Feb-2011
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
A custom import method for importing modules or classes from a string.
"""


def custom_import(module):
    """
    A custom import method to import a module.
    see: stackoverflow.com: 547829/how-to-dynamically-load-a-python-class
    """
    mod = __import__(module)
    components = module.split('.')
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def import_class(classstring):
    """
    Import a class using a `classstring`. This string is split by `.` and the
    last part is interpreted as class name.
    """
    (module_name, _sep, class_name) = classstring.rpartition('.')
    module = custom_import(module_name)
    return getattr(module, class_name)
