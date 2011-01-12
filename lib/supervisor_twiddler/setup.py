__revision__ = '$Id: setup.py,v 1.3 2007/10/04 18:17:31 chrism Exp $'

from ez_setup import use_setuptools
use_setuptools()

import os
import sys
import string

version, extra = string.split(sys.version, ' ', 1)
maj, minor = string.split(version, '.', 1)

if not maj[0] >= '2' and minor[0] >= '3':
    msg = ("supervisor requires Python 2.3 or better, you are attempting to "
           "install it using version %s.  Please install with a "
           "supported version" % version)

from setuptools import setup, find_packages
here = os.path.abspath(os.path.normpath(os.path.dirname(__file__)))

DESC = """\
supervisor_twiddler is an RPC extension for the supervisor2 package that
facilitates manipulation of supervisor's configuration and state in ways 
that are not normally accessible at runtime."""

CLASSIFIERS = [
    'Development Status :: 5 - Production/Stable',
    'Environment :: No Input/Output (Daemon)',
    'Intended Audience :: System Administrators',
    'License :: OSI Approved :: BSD License',
    'Natural Language :: English',
    'Operating System :: POSIX',
    'Topic :: System :: Boot',
    'Topic :: System :: Systems Administration',
    ]

version_txt = os.path.join(here, 'src/supervisor_twiddler/version.txt')
supervisor_twiddler_version = open(version_txt).read().strip()

dist = setup(
    name = 'supervisor_twiddler',
    version = supervisor_twiddler_version,
    license = 'License :: OSI Approved :: BSD License',
    url = 'http://maintainable.com/software/supervisor_twiddler',
    description = "supervisor_twiddler RPC extension for supervisor2",
    long_description= DESC,
    classifiers = CLASSIFIERS,
    author = "Mike Naberezny",
    author_email = "mike@maintainable.com",
    maintainer = "Mike Naberezny",
    maintainer_email = "mike@maintainable.com",
    package_dir = {'':'src'},
    packages = find_packages(os.path.join(here, 'src')),
    install_requires = ['supervisor>=3.0a3'],
    include_package_data = True,
    zip_safe = False,
    namespace_packages = ['supervisor_twiddler'],
    test_suite = 'supervisor_twiddler.tests'
    )
