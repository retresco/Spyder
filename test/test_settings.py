#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# test_settings.py 10-Jan-2011
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

import unittest


class SettingsTest(unittest.TestCase):

    def test_loading_default_settings_works(self):

        from spyder import defaultsettings
        from spyder.core.settings import Settings

        settings = Settings()
        self.assertEqual(defaultsettings.ZEROMQ_MGMT_MASTER,
                settings.ZEROMQ_MGMT_MASTER)


    def test_loading_custom_settings_works(self):

        from spyder import defaultsettings
        from spyder.core.settings import Settings

        import test_settings_settings
        settings = Settings(test_settings_settings)

        self.assertEqual(test_settings_settings.ZEROMQ_MGMT_WORKER,
                settings.ZEROMQ_MGMT_WORKER)


if __name__ == '__main__':
    unittest.main()
