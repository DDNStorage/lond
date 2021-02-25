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

from pylcommon import utils
from pylcommon import clog
from pylcommon import watched_io
from pylcommon import lustre
from pylond import definition
from pylond import definition_helper
from pylond import copytoold_client
from pylond import common


def simple_usage():
    """
    Print simple usage string
    """
    utils.eprint("  commands:\n"
                 "    fetch     fetch dirs from global Lustre to local Lustre\n"
                 "    stat      show the lond status of dirs or files\n"
                 "    sync      sync dirs from local Lustre to global Lustre\n"
                 "    unlock    unlock global Lustre dirs or files\n")


def usage():
    """
    Print usage string
    """
    utils.eprint("Usage: %s [options]\n"
                 "or     %s <command> -h|--help\n"
                 "or     %s <command> [args...]\n"
                 "  options:\n"
                 "    -h|--help   print this help"
                 % (sys.argv[0], sys.argv[0], sys.argv[0]))
    simple_usage()


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


LOND_COMMANDS = {}


def lond_command_common(command, interact, log, args):
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    # pylint: disable=too-many-return-statements,unused-argument
    """
    Command command to run
    """
    hostname = socket.gethostname()
    host = lustre.LustreServerHost(hostname, local=True)

    command = common.c_command_path("lond_%s" % command)
    command += " --%s %s" % (definition.LOND_OPTION_PROGNAME, command)
    for arg in args[1:]:
        command += " " + arg

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


LOND_COMMNAD_FETCH = "fetch"


def lond_command_fetch(interact, log, args):
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    # pylint: disable=too-many-return-statements,unused-argument
    """
    Fetch the file from global Lustre to on demand Lustre
    """
    hostname = socket.gethostname()
    host = lustre.LustreServerHost(hostname, local=True)

    start_copytool = True
    sources = []
    dest = None
    real_args = args[1:]
    for arg_index, arg in enumerate(real_args):
        if arg == "-h" or arg == "--help":
            start_copytool = False
        if arg_index == len(real_args) - 1:
            dest = arg
        else:
            sources.append(arg)

    if start_copytool:
        if dest is None:
            log.cl_error("please specify the dest directory")
            return -1

        if not sources:
            log.cl_error("please specify one or more source directories")
            return -1

        source_fsnames = []
        for source in sources:
            client_name = host.lsh_getname(log, source)
            if client_name is None:
                log.cl_error("failed to get the Lustre client name of [%s]", source)
                return -1
            fsname = lustre.get_fsname_from_service_name(client_name)
            if fsname is None:
                log.cl_error("failed to get the Lustre fsname from client name [%s]",
                             client_name)
                return -1
            if fsname not in source_fsnames:
                source_fsnames.append(fsname)

        dest_client_name = host.lsh_getname(log, dest)
        if dest_client_name is None:
            log.cl_error("failed to get the Lustre client name of [%s]", dest)
            return -1
        dest_fsname = lustre.get_fsname_from_service_name(dest_client_name)
        if dest_fsname is None:
            log.cl_error("failed to get the Lustre fsname from client name [%s]",
                         dest_fsname)
            return -1

        if dest_fsname in source_fsnames:
            log.cl_error("fetching directory to the same Lustre file system [%s] is not allowed",
                         dest_fsname)
            return -1

        cclient = copytoold_client.CopytooldClient("tcp://localhost:3003")
        for source_fsname in source_fsnames:
            log.cl_info("starting copytool from [%s] to [%s]", source_fsname, dest_fsname)
            ret = cclient.cc_send_start_request(log, source_fsname, dest_fsname)
            if ret:
                log.cl_error("failed to start copytool from [%s] to [%s]",
                             source_fsname, dest_fsname)
                log.cl_error("please run [systemctl start lond_copytoold && "
                             "systemctl enable lond_copytoold.service] to "
                             "start the lond_copytoold service")
                cclient.cc_fini()
                return -1
            log.cl_info("started copytool from [%s] to [%s]", source_fsname, dest_fsname)
        cclient.cc_fini()

    return lond_command_common(LOND_COMMNAD_FETCH, interact, log, args)

LOND_COMMANDS[LOND_COMMNAD_FETCH] = \
    LondCommand(LOND_COMMNAD_FETCH, lond_command_fetch,
                definition.LOND_FETCH_OPTIONS)


LOND_COMMNAD_UNLOCK = "unlock"


def lond_command_unlock(interact, log, args):
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    # pylint: disable=too-many-return-statements,unused-argument
    """
    Unlock the file on global Lustre
    """
    return lond_command_common(LOND_COMMNAD_UNLOCK, interact, log, args)


LOND_COMMANDS[LOND_COMMNAD_UNLOCK] = \
    LondCommand(LOND_COMMNAD_UNLOCK, lond_command_unlock,
                definition.LOND_UNLOCK_OPTIONS)


LOND_COMMNAD_STAT = "stat"


def lond_command_stat(interact, log, args):
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    # pylint: disable=too-many-return-statements,unused-argument
    """
    Unlock the file on global Lustre
    """
    return lond_command_common(LOND_COMMNAD_STAT, interact, log, args)

LOND_COMMANDS[LOND_COMMNAD_STAT] = \
    LondCommand(LOND_COMMNAD_STAT, lond_command_stat,
                definition.LOND_STAT_OPTIONS)


LOND_COMMNAD_SYNC = "sync"


def lond_command_sync(interact, log, args):
    # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    # pylint: disable=too-many-return-statements,unused-argument
    """
    sync the directory on on-demand Lustre to global Lustre
    """
    return lond_command_common(LOND_COMMNAD_SYNC, interact, log, args)


LOND_COMMANDS[LOND_COMMNAD_SYNC] = \
    LondCommand(LOND_COMMNAD_SYNC, lond_command_sync,
                definition.LOND_SYNC_OPTIONS)


def lond_command_help(interact, log, args):
    # pylint: disable=unused-argument
    """
    Print the help string
    """
    if len(args) <= 1:
        simple_usage()
        utils.eprint("\n    h         print this help\n"
                     "    h <cmd>   print help of command <cmd>\n"
                     "    <cmd> -h  print help of command <cmd>\n"
                     "\n"
                     "    q         quit")
        return 0
    cmd_line = "%s -h" % args[1]
    return interact.li_command(log, cmd_line)


LOND_COMMNAD_HELP = "h"
LOND_HELP_OPTIONS = definition_helper.CommandOptions()
for lond_command in LOND_COMMANDS.values():
    option = definition_helper.CommandOption(lond_command.lc_command, False)
    LOND_HELP_OPTIONS.cos_add_option(option)

LOND_COMMANDS[LOND_COMMNAD_HELP] = \
    LondCommand(LOND_COMMNAD_HELP, lond_command_help,
                LOND_HELP_OPTIONS)


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
                            paths = options.cos_complete_path(being_completed)
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
        assert args
        command = args[0]
        if command not in LOND_COMMANDS:
            log.cl_stderr('unknown command [%s]', command)
            return -1
        else:
            lcommand = LOND_COMMANDS[command]
        try:
            return lcommand.lc_function(self, log, args)
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
                if not cmd_line:
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
