#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# __init__.py 07-Jan-2011
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
"""
The Spyder.
"""

import os
import shutil
import stat
import sys

import spyder
from spyder.core.settings import Settings


__version__ = '0.0-dev'


__all__ = ["core", "processor", "defaultsettings", "spyder_template", "thrift",
        "workerprocess"]


def copy_skeleton_dir(destination):
    """
    Copy the skeleton directory (spyder_template) to a new directory.
    """
    if not os.path.exists(destination):
        os.makedirs(destination)
    template_dir = os.path.join(spyder.__path__[0], 'spyder_template')
    wanted_files = [".keep", "logging.conf"]

    for root, subdirs, files in os.walk(template_dir):
        relative = root[len(template_dir) + 1:]
        if relative:
            os.mkdir(os.path.join(destination, relative))

        for subdir in subdirs:
            if subdir.startswith('.'):
                subdirs.remove(subdir)

        for f in files:
            if not f.endswith('.py') and f not in wanted_files:
                continue
            path_old = os.path.join(root, f)
            path_new = os.path.join(destination, relative, f)
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
                    st = os.stat(path_new)
                    new_permissions = stat.S_IMODE(st.st_mode) | stat.S_IWUSR
                    os.chmod(path_new, new_permissions)
            except OSError:
                sys.stderr.write("Could not set permission bits on %s" %
                    path_new)


def spyder_admin_main(args=None):
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
