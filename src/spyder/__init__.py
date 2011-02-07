#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# __init__.py 07-Jan-2011
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
The Spyder.
"""

import os
import shutil
import stat
import sys

from spyder.core.settings import Settings


__version__ = '0.1-dev'


__all__ = ["core", "processor", "defaultsettings", "spyder_template", "thrift",
        "workerprocess"]


def copy_skeleton_dir(destination):
    """
    Copy the skeleton directory (spyder_template) to a new directory.
    """
    if not os.path.exists(destination):
        os.makedirs(destination)
    template_dir = os.path.join(__path__[0], 'spyder_template')
    wanted_files = [".keep", "logging.conf"]

    for root, subdirs, files in os.walk(template_dir):
        relative = root[len(template_dir) + 1:]
        if relative:
            os.mkdir(os.path.join(destination, relative))

        for subdir in subdirs:
            if subdir.startswith('.'):
                subdirs.remove(subdir)

        for filename in files:
            if (not filename.endswith('.py') and \
                filename not in wanted_files) or \
                filename == "__init__.py":

                continue

            path_old = os.path.join(root, filename)
            path_new = os.path.join(destination, relative, filename)
            fp_old = open(path_old, 'r')
            fp_new = open(path_new, 'w')
            fp_new.write(fp_old.read())
            fp_old.close()
            fp_new.close()

            try:
                shutil.copymode(path_old, path_new)
                if sys.platform.startswith('java'):
                    # On Jython there is no os.access()
                    return
                if not os.access(path_new, os.W_OK):
                    st_new = os.stat(path_new)
                    new_perm = stat.S_IMODE(st_new.st_mode) | stat.S_IWUSR
                    os.chmod(path_new, new_perm)
            except OSError:
                sys.stderr.write("Could not set permission bits on %s" %
                    path_new)


def spyder_admin_main():
    """
    Method for creating new environments for Spyders.
    """
    if len(sys.argv) != 2 or "start" != sys.argv[1]:
        sys.stderr.write(
"""Usage: 'spyder start'
    to start a new spyder in the current directory\n""")
        sys.exit(1)

    copy_skeleton_dir(os.getcwd())


def spyder_management(settings):
    """
    Start new master/worker/logsink processes.
    """

    from spyder import logsink
    import spyder.workerprocess as worker
    import spyder.masterprocess as master

    effective_settings = Settings(settings)

    args = [a.lower() for a in sys.argv]

    if "master" in args:
        args.remove("master")
        master.main(effective_settings)
    elif "worker" in args:
        worker.main(effective_settings)
    elif "logsink" in args:
        logsink.main(effective_settings)
    else:
        print >> sys.stderr, """Usage: spyder-ctrl [master|worker|logsink]

'master'\t\tstart a master process.
'worker'\t\tstart a worker process.
'logsink'\t\tstart a sink for logmessages.
"""
        sys.exit(1)
