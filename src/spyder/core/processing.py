#
# Copyright (c) 2008 Daniel Truemper truemped@googlemail.com
#
# processing.py 10-Jan-2011
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


class Command(object):
    """
    Base class for spyder processors.
    """

    def execute(self, context):
        """
        Execut the command with the context.
        """
        return context



class Chain(object):
    """
    A chain of commands.
    """

    def __init__(self, commands):
        """
        Construct a chain of commands.
        """
        if type([]) == type(commands):
            self.commands = commands
        else:
            pass
            # TODO


    def execute(self, context):
        """
        Execute this chain.
        """
        for cmd in self.commands:
            context = cmd.execute(context)

        return context

