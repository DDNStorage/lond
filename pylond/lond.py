# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Launch LOND commands
"""
import sys
import readline
import traceback
import socket
import os

from pylcommon import utils
from pylcommon import clog
from pylcommon import ssh_host
from pylcommon import watched_io
from pylond import definition
from pylond import definition_helper


def c_command_path(cmd_name):
    """
    Return the path of the command
    """
    command = sys.argv[0]
    if "/" in command:
        directory = os.path.dirname(command)
        fpath = directory + "/src/" + cmd_name
        if utils.is_exe(fpath):
            return fpath

    return cmd_name


def usage():
    """
    Print usage string
    """
    utils.eprint("\n"
                 "%s [options]\n"
                 "%s <command> [args...]\n"
                 "\n"
                 "  options:\n"
                 "    [-h|--help]   this help\n"
                 "\n"
                 "  commands (non interactive mode):\n"
                 "    fetch    fetch dir from global Lustre to on demand Lustre\n"
                 "    sync     sync dir from on demand Lustre to global Lustre\n"
                 % (sys.argv[0], sys.argv[0]))


class LondCommand(object):
    """
    Config command
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self, command, function, options):
        self.lc_command = command
        self.lc_function = function
        # Options of CommandOptions type
        self.lc_options = options


LOND_COMMNAD_HELP = "h"


def lond_command_help(log, args):
    # pylint: disable=unused-argument
    """
    Print the help string
    """
    log.cl_stdout("""commands:
   h                    print this menu
   q                    quit""")

    return 0

LOND_COMMANDS = {}
LOND_COMMANDS[LOND_COMMNAD_HELP] = \
    LondCommand(LOND_COMMNAD_HELP, lond_command_help,
                definition_helper.CommandOptions())


LOND_COMMNAD_FETCH = "fetch"


def lond_command_fetch(log, args):
    # pylint: disable=unused-argument
    """
    Fetch the file from global Lustre to on demand Lustre
    """
    command = c_command_path("lond_fetch")
    for arg in args[1:]:
        command += " --%s fetch " % definition.LOND_OPTION_PROGNAME + arg

    hostname = socket.gethostname()
    host = ssh_host.SSHHost(hostname, local=True)

    args = {}
    args[watched_io.WATCHEDIO_LOG] = log
    args[watched_io.WATCHEDIO_HOSTNAME] = host.sh_hostname
    stdout_fd = watched_io.watched_io_open("/dev/null",
                                           watched_io.log_watcher_info_simplified,
                                           args)
    stderr_fd = watched_io.watched_io_open("/dev/null",
                                           watched_io.log_watcher_error_simplified,
                                           args)
    log.cl_debug("start to run command [%s] on host [%s]",
                 command, host.sh_hostname)
    retval = host.sh_run(log, command, stdout_tee=stdout_fd,
                         stderr_tee=stderr_fd, return_stdout=False,
                         return_stderr=False, timeout=None, flush_tee=True)
    stdout_fd.close()
    stderr_fd.close()

    return retval.cr_exit_status

LOND_COMMANDS[LOND_COMMNAD_FETCH] = \
    LondCommand(LOND_COMMNAD_FETCH, lond_command_fetch,
                definition.LOND_FETCH_OPTIONS)


class LondInteract(object):
    """
    Lond interactive interfaces
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, log):
        self.li_candidates = []
        self.li_cstr_candidates = []
        self.li_log = log

    def li_command_dict(self):
        """
        Return the command dict
        """
        # pylint: disable=no-self-use
        command_dict = LOND_COMMANDS
        return command_dict

    def li_completer(self, text, state):
        # pylint: disable=too-many-branches,unused-argument
        # pylint: disable=too-many-nested-blocks
        """
        The complete function of the input completer
        """
        response = None
        if state == 0:
            # This is the first time for this text,
            # so build a match list.
            origline = readline.get_line_buffer()
            begin = readline.get_begidx()
            end = readline.get_endidx()
            being_completed = origline[begin:end]
            words = origline.split()
            if not words:
                self.li_candidates = sorted(self.li_command_dict().keys())
            else:
                try:
                    if begin == 0:
                        # first word
                        candidates = self.li_command_dict().keys()
                    else:
                        # later word
                        first = words[0]
                        command = self.li_command_dict()[first]
                        options = command.lc_options
                        candidates = options.cos_candidates()

                    if being_completed:
                        # match options with portion of input
                        # being completed
                        self.li_candidates = []
                        for candidate in candidates:
                            if not candidate.startswith(being_completed):
                                continue
                            self.li_candidates.append(candidate)
                        if len(words) > 1:
                            paths = options.cos_complete_path(words[-1])
                            if paths is not None:
                                self.li_candidates += paths
                    else:
                        # matching empty string so use all candidates
                        self.li_candidates = candidates
                except (KeyError, IndexError):
                    self.li_candidates = []
        try:
            response = self.li_candidates[state]
        except IndexError:
            response = None
        return response

    def li_command(self, log, cmd_line):
        """
        Run a command
        """
        # pylint: disable=broad-except,no-self-use
        log.cl_result.cr_clear()
        log.cl_abort = False
        args = cmd_line.split()
        assert len(args) > 0
        command = args[0]
        if command not in LOND_COMMANDS:
            log.cl_stderr('unknown command [%s]', command)
            return -1
        else:
            lcommand = LOND_COMMANDS[command]
        try:
            return lcommand.lc_function(log, args)
        except Exception, err:
            log.cl_stderr("failed to run command %s, exception: "
                          "%s, %s",
                          args, err, traceback.format_exc())
            return -1

    def li_loop(self, cmdline=None):
        """
        Loop and execute the command
        """
        # pylint: disable=unused-variable
        log = self.li_log

        readline.parse_and_bind("tab: complete")
        readline.parse_and_bind("set editing-mode vi")
        # This enables completer of options with prefix "-" or "--"
        # becase "-" is one of the delimiters by default
        readline.set_completer_delims(" \t\n")
        readline.set_completer(self.li_completer)
        ret = 0
        while True:
            if cmdline is None:
                try:
                    prompt = '$ (h for help): '
                    log.cl_debug(prompt)
                    cmd_line = raw_input(prompt)
                except (KeyboardInterrupt, EOFError):
                    log.cl_debug("keryboard interrupt recieved")
                    log.cl_info("")
                    log.cl_info("Type q to exit")
                    continue
                log.cl_debug("input: %s", cmd_line)
                cmd_line = cmd_line.strip()
                if len(cmd_line) == 0:
                    continue
            else:
                cmd_line = cmdline

            args = cmd_line.split()
            if args[0] == 'q':
                break
            else:
                ret = self.li_command(log, cmd_line)

            # The server told us to quit the connection
            if log.cl_abort:
                break
            # not interactive mode
            if cmdline is not None:
                break

        readline.set_completer(None)
        return ret


def main():
    """
    Run LOND commands
    """
    reload(sys)
    sys.setdefaultencoding("utf-8")
    argc = len(sys.argv)
    if argc < 2:
        cmdline = None
    elif argc == 2:
        # lond -h
        # lond --help
        # lond command
        if sys.argv[1] == "-h" or sys.argv[1] == "--help":
            usage()
            sys.exit(0)
        cmdline = sys.argv[1]
    else:
        cmdline = ""
        for arg_index in range(1, argc):
            if cmdline != "":
                cmdline += " "
            cmdline += sys.argv[arg_index]
    log = clog.get_log(simple_console=True)
    interact = LondInteract(log)
    ret = interact.li_loop(cmdline)
    sys.exit(ret)
