# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com
"""
Copytool manager daemon
"""
import os
import sys
import traceback
import socket
import zmq
import yaml

# Local libs
from pylcommon import cmd_general
from pylcommon import utils
from pylcommon import ssh_host
from pylond import copytoold_pb2
from pylond import common

COPYTOOLD_WORKER_NUMBER = 3
COPYTOOLD_DEFAULT_PORT = 3003
COPYTOOLD_CONFIG = "/etc/lond_copytoold.conf"
COPYTOOLD_LOG_DIR = "/var/log/lond/copytoold"
COPYTOOLD_PORT_STR = "port"
COPYTOOL_COMMAND = "lond_copytool"


def start_copytool(log, host, source, dest):
    """
    Start a copytool from source to dest
    """
    command = "%s %s %s --daemon" % (COPYTOOL_COMMAND, source, dest)
    pgrep_command = "pgrep -f -x '%s'" % command
    retval = host.sh_run(log, pgrep_command)
    if retval.cr_exit_status == 0:
        copytool_processes = retval.cr_stdout.splitlines()
        if copytool_processes:
            log.cl_info("copytool from [%s] to [%s] is already running as "
                        "process %s",
                        source, dest, copytool_processes)
            return 0

    retval = host.sh_run(log, command)
    if retval.cr_exit_status != 0:
        log.cl_error("failed to run command, command = [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     command,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
        return -1

    retval = host.sh_run(log, pgrep_command)
    if retval.cr_exit_status == 0:
        copytool_processes = retval.cr_stdout.splitlines()
        if copytool_processes:
            log.cl_info("copytool from [%s] to [%s] is started as process %s",
                        source, dest, copytool_processes)
            return 0
        else:
            log.cl_error("unexpected output of command, command = [%s], "
                         "ret = [%d], stdout = [%s], stderr = [%s]",
                         pgrep_command,
                         retval.cr_exit_status,
                         retval.cr_stdout,
                         retval.cr_stderr)
    else:
        log.cl_error("failed to run command, command = [%s], "
                     "ret = [%d], stdout = [%s], stderr = [%s]",
                     pgrep_command,
                     retval.cr_exit_status,
                     retval.cr_stdout,
                     retval.cr_stderr)
    return -1


class CopytoolDaemon(object):
    """
    This server that listen and handle requests from console
    """
    # pylint: disable=too-few-public-methods,too-many-instance-attributes
    def __init__(self, log, workspace, server_port):
        self.cd_log = log
        self.cd_running = True
        self.cd_url_client = "tcp://*:" + str(server_port)
        self.cd_url_worker = "inproc://workers"
        self.cd_context = zmq.Context.instance()
        self.cd_client_socket = self.cd_context.socket(zmq.ROUTER)
        self.cd_client_socket.bind(self.cd_url_client)
        self.cd_worker_socket = self.cd_context.socket(zmq.DEALER)
        self.cd_worker_socket.bind(self.cd_url_worker)
        self.cd_workspace = workspace
        for worker_index in range(COPYTOOLD_WORKER_NUMBER):
            log.cl_info("starting worker thread [%d]", worker_index)
            utils.thread_start(self.cd_worker_thread, (worker_index, ))

    def cd_fini(self):
        """
        Finish server
        """
        self.cd_running = False
        self.cd_client_socket.close()
        self.cd_worker_socket.close()
        self.cd_context.term()

    def cd_worker_thread(self, worker_index):
        """
        Worker routine
        """
        # pylint: disable=too-many-nested-blocks,too-many-locals
        # pylint: disable=too-many-branches,too-many-statements
        # Socket to talk to dispatcher
        name = "thread_worker_%s" % worker_index
        thread_workspace = self.cd_workspace + "/" + name
        if not os.path.exists(thread_workspace):
            ret = utils.mkdir(thread_workspace)
            if ret:
                self.cd_log.cl_error("failed to create directory [%s] on local host",
                                     thread_workspace)
                return -1
        elif not os.path.isdir(thread_workspace):
            self.cd_log.cl_error("[%s] is not a directory", thread_workspace)
            return -1
        log = self.cd_log.cl_get_child(name, resultsdir=thread_workspace)

        log.cl_info("starting worker thread [%s]", worker_index)
        dispatcher_socket = self.cd_context.socket(zmq.REP)
        dispatcher_socket.connect(self.cd_url_worker)
        hostname = socket.gethostname()
        host = ssh_host.SSHHost(hostname, local=True)

        while self.cd_running:
            try:
                request_message = dispatcher_socket.recv()
            except zmq.ContextTerminated:
                log.cl_info("worker thread [%s] exiting because context has "
                            "been terminated", worker_index)
                break

            cmessage = copytoold_pb2.CopytooldMessage
            request = cmessage()
            request.ParseFromString(request_message)
            log.cl_debug("received request with type [%s]", request.cm_type)
            reply = cmessage()
            reply.cm_protocol_version = cmessage.CPV_ZERO
            reply.cm_errno = cmessage.CE_NO_ERROR

            if request.cm_type == cmessage.CMT_START_REQUEST:
                source = request.cm_start_request.csr_source
                dest = request.cm_start_request.csr_dest
                log.cl_info("received a start request of copytool from [%s] to [%s]",
                            source, dest)
                ret = start_copytool(log, host, source, dest)
                if ret:
                    reply.cm_errno = cmessage.CE_OPERATION_FAILED
                reply.cm_type = cmessage.CMT_START_REPLY
            else:
                reply.cm_type = cmessage.CMT_GENERAL
                reply.cm_errno = cmessage.CE_NO_TYPE
                log.cl_error("received a request with type [%s] that "
                             "is not supported",
                             request.cm_type)

            reply_message = reply.SerializeToString()
            dispatcher_socket.send(reply_message)
            log.cl_info("send reply to a start request of copytool from [%s] to [%s]",
                        source, dest)
        dispatcher_socket.close()
        log.cl_info("worker thread [%s] exited", worker_index)

    def cd_loop(self):
        """
        Proxy the server
        """
        # pylint: disable=bare-except
        try:
            zmq.proxy(self.cd_client_socket, self.cd_worker_socket)
        except:
            self.cd_log.cl_info("got exception when running proxy, exiting")


def copytoold_do_loop(log, workspace, config, config_fpath):
    """
    Server routine
    """
    # pylint: disable=unused-argument
    if config is not None:
        server_port = utils.config_value(config, COPYTOOLD_PORT_STR)
        if server_port is None:
            log.cl_info("no [%s] is configured, using port [%s]",
                        COPYTOOLD_PORT_STR, COPYTOOLD_DEFAULT_PORT)
            server_port = COPYTOOLD_DEFAULT_PORT
    else:
        log.cl_info("starting copytoold service using port [%s]",
                    COPYTOOLD_DEFAULT_PORT)
        server_port = COPYTOOLD_DEFAULT_PORT

    copytool_daemon = CopytoolDaemon(log, workspace, server_port)
    copytool_daemon.cd_loop()
    copytool_daemon.cd_fini()
    return 0


def copytoold_loop(log, workspace, config_fpath):
    """
    Start copytoold holding the configure lock
    """
    # pylint: disable=bare-except
    if config_fpath is not None:
        config_fd = open(config_fpath)
        ret = 0
        try:
            config = yaml.load(config_fd)
        except:
            log.cl_error("not able to load [%s] as yaml file: %s", config_fpath,
                         traceback.format_exc())
            ret = -1
        config_fd.close()
        if ret:
            return -1
    else:
        config = None

    try:
        ret = copytoold_do_loop(log, workspace, config, config_fpath)
    except:
        ret = -1
        log.cl_error("exception: %s", traceback.format_exc())

    return ret


def usage():
    """
    Print usage string
    """
    utils.eprint("Usage: %s <config_file>" %
                 sys.argv[0])


def main():
    """
    Start clownfish server
    """
    # pylint: disable=global-statement
    global COPYTOOL_COMMAND
    COPYTOOL_COMMAND = common.c_command_path(COPYTOOL_COMMAND)
    cmd_general.main(COPYTOOLD_CONFIG, COPYTOOLD_LOG_DIR, copytoold_loop)
