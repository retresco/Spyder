#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# setup.py 04-Jan-2011
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

from setuptools import setup, find_packages
import re

__version__ = re.search( "__version__\s*=\s*'(.*)'", open('src/spyder/__init__.py').read(), re.M).group(1)
assert __version__

long_description = open("README.rst").read()
assert long_description

tests_require = ['mockito>=0.5.1', 'coverage>=3.4']

setup(
    name = "spyder",
    version = __version__,
    description = "A python spider",
    long_description = long_description,
    author = "Daniel Truemper",
    author_email = "truemped@googlemail.com",
    url = "",
    license = "Apache 2.0",
    package_dir = { '' : 'src' },
    packages = find_packages('src'),
    include_package_data = True,
    test_suite = 'nose.collector',
    install_requires = [
        'pyzmq>=2.0.10',
        'tornado>=1.1',
        'thrift>=0.5.0',
        'pycurl>=7.19.0',
        'pytz>=2010o',
        'brownie>=0.4.1',
    ],
    tests_require = tests_require,
    extras_require = {'test': tests_require},
    entry_points = {
        'console_scripts' : [
            'spyder = spyder:spyder_admin_main',
        ]
    },
    classifiers = [
        'Intended Audience :: Developers',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python :: 2.6',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Indexing/Search',
    ]
)
