import os

from supervisor.options import UnhosedConfigParser
from supervisor.options import ProcessGroupConfig
from supervisor.supervisord import SupervisorStates
from supervisor.xmlrpc import Faults as SupervisorFaults
from supervisor.xmlrpc import RPCError
from supervisor.http import NOT_DONE_YET
import supervisor.loggers

API_VERSION = '0.1'

class Faults:
    STILL_RUNNING = 220

class TwiddlerNamespaceRPCInterface:
    """ A supervisor rpc interface that facilitates manipulation of 
    supervisor's configuration and state in ways that are not 
    normally accessible at runtime.
    """
    def __init__(self, supervisord):
        self.supervisord = supervisord

    def _update(self, text):
        self.update_text = text # for unit tests, mainly

        state = self.supervisord.get_state()

        if state == SupervisorStates.SHUTDOWN:
            raise RPCError(SupervisorFaults.SHUTDOWN_STATE)

        # XXX fatal state
        
    # RPC API methods

    def getAPIVersion(self):
        """ Return the version of the RPC API used by supervisor_twiddler

        @return int version version id
        """
        self._update('getAPIVersion')
        return API_VERSION

    def getGroupNames(self):
        """ Return an array with the names of the process groups.
        
        @return array                Process group names
        """
        self._update('getGroupNames')
        return self.supervisord.process_groups.keys()

    def log(self, message, level=supervisor.loggers.LevelsByName.INFO):
        """ Write an arbitrary message to the main supervisord log.  This is 
            useful for recording information about your twiddling.
        
        @param  string      message      Message to write to the log
        @param  string|int  level        Log level name (INFO) or code (20)
        @return boolean                  Always True unless error
        """
        self._update('log')

        if isinstance(level, str):
            level = getattr(supervisor.loggers.LevelsByName, 
                            level.upper(), None)

        if supervisor.loggers.LOG_LEVELS_BY_NUM.get(level, None) is None:
            raise RPCError(SupervisorFaults.INCORRECT_PARAMETERS)
        
        self.supervisord.options.logger.log(level, message)
        return True

    def addGroup(self, name, priority):
        """ Add a new, empty process group.
        
        @param string   name         Name for the new process group
        @param integer  priority     Group priority (same as supervisord.conf)
        @return boolean              Always True unless error
        """
        self._update('addGroup')
        
        # check group_name does not already exist
        if self.supervisord.process_groups.get(name) is not None:
            raise RPCError(SupervisorFaults.BAD_NAME, name)

        # check priority is sane
        try:
            int(priority)
        except ValueError, why:
            raise RPCError(SupervisorFaults.INCORRECT_PARAMETERS, why[0])            

        # make a new group with no process configs
        options = self.supervisord.options
        config = ProcessGroupConfig(options, name, priority, [])
        group = config.make_group()

        # add new process group
        self.supervisord.process_groups[name] = group
        return True

    def addProgramToGroup(self, group_name, program_name, program_options):
        """ Add a new program to an existing process group.  Depending on the
            numprocs option, this will result in one or more processes being
            added to the group.

        @param string  group_name       Name of an existing process group
        @param string  program_name     Name of the new process in the process table
        @param struct  program_options  Program options, same as in supervisord.conf
        @return boolean                 Always True unless error
        """
        self._update('addProgramToGroup')
        
        group = self._getProcessGroup(group_name)

        # make configparser instance for program options
        section_name = 'program:%s' % program_name
        parser = self._makeConfigParser(section_name, program_options)

        # make process configs from parser instance
        options = self.supervisord.options
        try:
            new_configs = options.processes_from_section(parser, section_name, group_name)
        except ValueError, why:
            raise RPCError(SupervisorFaults.INCORRECT_PARAMETERS, why)

        # check new process names don't already exist in the config
        for new_config in new_configs:
            for existing_config in group.config.process_configs:
                if new_config.name == existing_config.name:
                    raise RPCError(SupervisorFaults.BAD_NAME, new_config.name)

        # add process configs to group
        group.config.process_configs.extend(new_configs)

        for new_config in new_configs:
            # the process group config already exists and its after_setuid hook 
            # will not be called again to make the auto child logs for this process.
            new_config.create_autochildlogs()

            # add process instance
            group.processes[new_config.name] = new_config.make_process(group)

        return True

    def removeProcessFromGroup(self, group_name, process_name):
        """ Remove a process from a process group.  When a program is added with
            addProgramToGroup(), one or more processes for that program is added
            to the group.  This method removes individual processes (named by the 
            numprocs and process_name options), not programs.

        @param string group_name    Name of an existing process group
        @param string process_name  Name of the process to remove from group
        @return boolean             Always return True unless error
        """
        self._update('removeProcessFromGroup')

        group = self._getProcessGroup(group_name)

        # check process exists and is running
        process = group.processes.get(process_name)
        if process is None:
            raise RPCError(SupervisorFaults.BAD_NAME, process_name)
        if process.pid:
            raise RPCError(Faults.STILL_RUNNING, process_name)

        group.transition()

        # del process config from group
        for index, config in enumerate(group.config.process_configs):
            if config.name == process_name:
                del group.config.process_configs[index]
        
        # del process
        del group.processes[process_name]

        return True

    def _getProcessGroup(self, name):
        """ Find a process group by its name """
        group = self.supervisord.process_groups.get(name)
        if group is None:
            raise RPCError(SupervisorFaults.BAD_NAME, 'group: %s' % name)
        return group

    def _makeConfigParser(self, section_name, options):
        """ Populate a new UnhosedConfigParser instance with a 
        section built from an options dict.
        """
        config = UnhosedConfigParser()
        try:
            config.add_section(section_name)
            for k, v in dict(options).items():
                config.set(section_name, k, v)
        except (TypeError, ValueError):
            raise RPCError(SupervisorFaults.INCORRECT_PARAMETERS)        
        return config

def make_twiddler_rpcinterface(supervisord, **config):
    return TwiddlerNamespaceRPCInterface(supervisord)
