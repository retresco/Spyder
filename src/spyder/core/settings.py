#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
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
Simple class for working with settings.

Adopted from the Django based settings system.
"""

from spyder import defaultsettings


class Settings(object):
    """
    Class for handling spyder settings.
    """

    def __init__(self, settings=None):
        """
        Initialize the settings.
        """

        # load the default settings
        for setting in dir(defaultsettings):
            if setting == setting.upper():
                setattr(self, setting, getattr(defaultsettings, setting))

        # now override with user settings
        if settings is not None:
            for setting in dir(settings):
                if setting == setting.upper():
                    setattr(self, setting, getattr(settings, setting))
