#!/usr/bin/python -u
# Copyright (c) 2018-2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: Gu Zheng <gzheng@ddn.com>
"""
Lustre On Demand
"""

import traceback
import logging
import copy
import sys
import os
import re
import getopt
from multiprocessing import Pool as LodWorkPool
import yaml
from pylcommon import utils
from pylcommon import ssh_host

VALID_OPTS = ("initialize", "start", "stop", "status", "stage_in", "stage_out")
VALID_NET_TYPE_PATTERN = ("tcp*", "o2ib*")
LOD_CONFIG = "/etc/lod.conf"
LOD_LOG_DIR = "/var/log/lod/"

LOD_NODES = "nodes"
LOD_NET = "net"
LOD_DEVICE = "device"
LOD_MDT_DEVICE = "mdt_device"
LOD_OST_DEVICE = "ost_device"
LOD_FSNAME = "fsname"
LOD_MOUNTPOINT = "mountpoint"
LOD_MDS = "mds"
LOD_OSS = "oss"
LOD_CLIENTS = "clients"
LOD_DEFAULT_FSNAME = "fslod"
LOD_DEFAULT_NET = "tcp"
LOD_DEFAULT_MOUNTPOINT = "/mnt/lustre_lod"

SOURCE = None
SOURCE_LIST = None
DEST = None
OPERATOR = None
MPIRUN = None

"""
TODO
1. Failed? cleanup all
"""


def usage():
    """
    Print usage string
    """
    utils.eprint("""Usage: lod <-c/--config config_file> <-h/--help>
<-d/--dry-run> <-n/--node c01,c[02-04]> [start/stop/initialize]""")
    utils.eprint("	--config config_file :use specified config instead of default '/etc/lod.conf'")
    utils.eprint("	-d/--dry-run :*dry* run, don't do real job")
    utils.eprint("	-n/--node :, run lod with specified node list")
    utils.eprint("	-m/--mdtdevs :mdt device")
    utils.eprint("	-o/--ostdevs :ost device")
    utils.eprint("	-f/--fsname :lustre instance fsname")
    utils.eprint("	-i/--inet :networks interface, e.g. tcp0, o2ib01")
    utils.eprint("	-p/--mountpoint :mountpoint on client")
    utils.eprint("	-h/--help :show usage")


def lod_wrapper_action(node, action):
    """
    Wrapper function
    """
    # pylint: disable=bare-except
    try:
        func = node.__getattribute__(action)
    except:
        logging.error("Not [%s] function of %s", action, node.__class__)
        return -1
    return func()


def lod_parallel_do_action(nodes, action):
    """
    Parallel do actions
    """
    workpool = LodWorkPool()
    results = dict()
    for node in nodes:
        results[node] = workpool.apply_async(lod_wrapper_action, (node, action))
    workpool.close()
    workpool.join()
    return results.items()


def str_pattern_valid(sname, pattern_group):
    """
    Whether the sname matches the pattern in pattern_group
    """
    for patt_str in pattern_group:
        pattern = re.compile(patt_str)
        match = re.match(pattern, sname)
        if match is not None:
            return True
    return False


class LodConfig(object):
    """
    Lod config instance
    """
    # pylint: disable=too-many-instance-attributes,too-few-public-methods
    def __init__(self, node_list, device, mdt_device, ost_device, mds_list=None,
                 oss_list=None, client_list=None, net=LOD_DEFAULT_NET,
                 fsname=LOD_DEFAULT_FSNAME, mountpoint=LOD_DEFAULT_MOUNTPOINT):
        # pylint: disable=too-many-arguments
        self.lc_node_list = node_list
        self.lc_fsname = fsname
        self.lc_net = net
        self.lc_device = device
        if mdt_device is not None:
            self.lc_mdt_device = mdt_device
        else:
            self.lc_mdt_device = self.lc_device
        if ost_device is not None:
            self.lc_ost_device = ost_device
        else:
            self.lc_ost_device = self.lc_device
        self.lc_mds_list = mds_list
        self.lc_oss_list = oss_list
        self.lc_client_list = client_list
        self.lc_mountpoint = mountpoint

    def adjust_and_validate(self):
        """
        Validate lod config
        """
        # pylint: disable=too-many-return-statements,too-many-branches
        if self.lc_mdt_device is None or len(self.lc_mdt_device) == 0:
            logging.error("mdt_device is None or Empty.")
            return False
        if self.lc_ost_device is None or len(self.lc_ost_device) == 0:
            logging.error("device is None or Empty.")
            return False

        if self.lc_net is None:
            logging.debug("net-type isn't set, use [%s] as default.", LOD_DEFAULT_NET)
            self.lc_net = LOD_DEFAULT_NET
        elif not str_pattern_valid(self.lc_net, VALID_NET_TYPE_PATTERN):
            logging.error("Invalid net-type found [%s], expected tcp/o2ib.", self.lc_net)
            return False

        if self.lc_fsname is None:
            logging.debug("fsname isn't set, use [%s] as default.", LOD_DEFAULT_FSNAME)
            self.lc_fsname = LOD_DEFAULT_FSNAME

        if self.lc_mountpoint is None:
            logging.debug("mountpoint isn't set, use [%s] as default.", LOD_DEFAULT_MOUNTPOINT)
            self.lc_mountpoint = LOD_DEFAULT_MOUNTPOINT

        if self.lc_node_list is None or len(self.lc_node_list) < 1:
            logging.error("Invalid node list %s.", self.lc_node_list)
            return False

        # if mds_list is None, choose the first node for node list as mds
        if self.lc_mds_list is None or len(self.lc_mds_list) == 0:
            self.lc_mds_list = copy.deepcopy(self.lc_node_list)

        # if oss_list is None, will use all the nodes
        if self.lc_oss_list is None or len(self.lc_oss_list) == 0:
            self.lc_oss_list = copy.deepcopy(self.lc_node_list)

        # if client_list is None, will use all the nodes
        if self.lc_client_list is None or len(self.lc_client_list) == 0:
            self.lc_client_list = copy.deepcopy(self.lc_node_list)

        # mds_list, oss_list, clent_list should be contained in node_list
        if not set(self.lc_mds_list) <= set(self.lc_node_list):
            logging.error("mds_list not contained in node_list, %s/%s.",
                          self.lc_mds_list, self.lc_node_list)
            return False

        if not set(self.lc_oss_list) <= set(self.lc_node_list):
            logging.error("oss_list not contained in node_list, %s/%s.",
                          self.lc_oss_list, self.lc_node_list)
            return False

        if not set(self.lc_client_list) <= set(self.lc_node_list):
            logging.error("client_list not contained in node_list, %s/%s.",
                          self.lc_client_list, self.lc_node_list)
            return False

        # TIPS:mds_list and oss_list should can be mixed
        # mds: vm1, vm2
        # oss: vm1, vm2
        return True


class Lod(object):
    """
    Lod Node
    """
    def __init__(self, lod_config):
        self.lod_initialized = False
        self.lod_mounted = False
        self.lod_used = False
        self.lod_mds_nodes = list()
        self.lod_oss_nodes = list()
        self.lod_client_nodes = list()
        self.lod_dcp_nodes = list()
        mdt_device = lod_config.lc_mdt_device
        ost_device = lod_config.lc_ost_device
        net = lod_config.lc_net
        fsname = lod_config.lc_fsname
        mountpoint = lod_config.lc_mountpoint

        if (len(lod_config.lc_mds_list) == 0 or len(lod_config.lc_oss_list) == 0 or
                len(lod_config.lc_client_list) == 0):
            logging.error("mds_list/oss_list/client_list are invalid, %s/%s/%s.",
                          lod_config.lc_mds_list, lod_config.lc_oss_list,
                          lod_config.lc_client_list)
            return None

        mgs_node = lod_config.lc_mds_list[0]
        for i, mds in enumerate(lod_config.lc_mds_list):
            self.lod_mds_nodes.append(LodMds(mds, mdt_device, mgs_node, i, fsname, net=net))

        if len(self.lod_mds_nodes) < 1:
            logging.error("No mds node found, request at least 1, %s.", self.lod_mds_nodes)
            return None

        for i, oss in enumerate(lod_config.lc_oss_list):
            self.lod_oss_nodes.append(LodOss(oss, ost_device, mgs_node, i, fsname, net=net))

        if len(self.lod_oss_nodes) < 1:
            logging.error("No oss node found, request at least 1, %s.", self.lod_oss_nodes)
            return None

        for i, client in enumerate(lod_config.lc_client_list):
            self.lod_client_nodes.append(LodClient(client, mgs_node, fsname, mountpoint, net=net))

        if len(self.lod_client_nodes) < 1:
            logging.error("No client node found, request at least 1, %s.",
                          self.lod_client_nodes)
            return None

    def stage_in(self):
        """
        Stage in operation
        """
        return self.do_cp()

    def stage_out(self):
        """
        Stage out operation
        """
        return self.do_cp()

    def do_cp(self):
        """
        Copy @SOURCE file or files in SOURCE_LIST to @DEST
        if SOURCE is set, means single file, use cp instead if dcp not installed
        elif SOURCE_LIST is set, dcp is required.
        """
        if DEST is None or SOURCE is None:
            logging.error("pelease specify source and destination")
            return -1

        # take first client node to run copy job
        cp_node = self.lod_client_nodes[0]
        cp_command = ""
        no_dcp = False

        # if dcp node list is None, detect all client and store them into lisr
        if len(self.lod_dcp_nodes) == 0:
            for node in self.lod_client_nodes:
                if node.has_command("dcp") and node.has_command("mpirun"):
                    self.lod_dcp_nodes.append(node)

        if len(self.lod_dcp_nodes) == 0:
            if SOURCE_LIST is not None:
                logging.error("No valid dcp and mpi-runtime found on nodes %s, exit",
                              [node.ln_hostname for node in self.lod_client_nodes])
                return -1
            no_dcp = True

        source_items = " ".join(SOURCE.strip().split(","))

        if no_dcp:
            cp_command = "rsync --delete --timeout=1800 -az %s %s" % (source_items, DEST)
        else:
            hostname_list = ",".join([node.ln_hostname for node in self.lod_dcp_nodes])
            mpirun_prefix = "mpirun -np 4 " if MPIRUN is None else MPIRUN
            if SOURCE_LIST is None:
                cp_command = ("%s --host %s dcp --sparse --preserve %s %s" %
                              (mpirun_prefix, hostname_list, source_items, DEST))
            else:
                cp_command = ("%s --host %s dcp --sparse --preserve --input %s %s %s" %
                              (mpirun_prefix, hostname_list, SOURCE_LIST, source_items, DEST))

        logging.debug("run command [%s]", cp_command)

        ret = cp_node.run(cp_command)
        if ret:
            logging.error("failed to command [%s] on node [%s]", cp_command,
                          cp_node.ln_hostname)
            logging.error(traceback.format_exc())
        return ret

    def do_action(self, action):
        """
        Perform the action (initialize, start, stop, status)
        """
        assert isinstance(action, str), "action must be a string"
        if action in VALID_OPTS:
            func = getattr(self, action, None)
            if func is not None:
                return func()
            else:
                logging.error("can't find valid function of action [%s], exit.", action)
                return -1
        else:
            logging.error("invalid action [%s], expected one of %s.", action, VALID_OPTS)
            return -1

    def show_topology(self):
        """
        Show lod topology
        Operator: start
        MDS: mds0 mds1 ... mdsX
            mdt0,mgs: /dev/vda	---> mds0
                mdt1: /dev/vdb	---> mds1
        OSS: oss0 oss1 ...ossX
                ost0: /dev/vdx	---> oss0
                ost1: /dev/vdx	---> oss1
                ost2: /dev/vdx	---> oss2
        CLIENTS:
            client01	/mnt/lod
            client02	/mnt/lod
        FSNAME: fsname
        NET: tcp
        MOUNTPOINT: /mnt/lustre
        """
        utils.eprint("Operator: %s" % OPERATOR)
        if OPERATOR in ("stage_in", "stage_out"):
            if SOURCE is not None:
                utils.eprint("Source: %s" % SOURCE)
            elif SOURCE_LIST is not None:
                utils.eprint("Sourcelist: %s" % SOURCE_LIST)
            utils.eprint("Destination: %s" % DEST)
            return

        utils.eprint("MDS: %s" % [mds_node.ln_hostname for mds_node in self.lod_mds_nodes])
        for mds_node in self.lod_mds_nodes:
            if mds_node.lm_index == 0:
                mdt_name = "mdt0,mgs"
            else:
                mdt_name = "mdt%d" % mds_node.lm_index
            utils.eprint("	%10s: %s	---> %s" %
                         (mdt_name, mds_node.lm_mdt, mds_node.ln_hostname))

        utils.eprint("OSS: %s" % [oss_node.ln_hostname for oss_node in self.lod_oss_nodes])
        for oss_node in self.lod_oss_nodes:
            utils.eprint("	%10s: %s	---> %s" %
                         ("ost%d" % oss_node.lo_index,
                          oss_node.lo_ost, oss_node.ln_hostname))

        utils.eprint("Clients:")
        for client_node in self.lod_client_nodes:
            utils.eprint("	%s	%s" % (client_node.ln_hostname, client_node.ln_mountpoint))

        utils.eprint("Fsname: %s" % self.lod_client_nodes[0].ln_fsname)
        utils.eprint("Net: %s" % self.lod_client_nodes[0].ln_net)
        utils.eprint("Mountpoint: %s" % self.lod_client_nodes[0].ln_mountpoint)

    def initialize(self, force=False):
        """
        Similar to normal lustre format
        """
        if self.lod_initialized and not force:
            return 0

        results = lod_parallel_do_action(self.lod_mds_nodes, "mkfs")
        for mds, ret in results:
            if ret.get() < 0:
                logging.error("failed to format device[%s] on [%s].",
                              mds.lm_mdt, mds.ln_hostname)
                return -1

        results = lod_parallel_do_action(self.lod_oss_nodes, "mkfs")
        for oss, ret in results:
            if ret.get() < 0:
                logging.error("failed to format device[%s] on [%s].",
                              oss.lo_ost, oss.ln_hostname)
                return -1
        self.lod_initialized = True
        return 0

    def start(self):
        """
        Similar to normal lustre start routine
        """
        if not self.lod_initialized:
            ret = self.initialize()
            if ret:
                return -1

        for mds in self.lod_mds_nodes:
            if mds.mount():
                logging.error("failed to mount device[%s] on [%s].",
                              mds.lm_mdt, mds.ln_hostname)
                return -1

        results = lod_parallel_do_action(self.lod_oss_nodes, "mount")
        for oss, ret in results:
            if ret.get() < 0:
                logging.error("failed to mount device[%s] on [%s].",
                              oss.lo_ost, oss.ln_hostname)
                return -1

        results = lod_parallel_do_action(self.lod_client_nodes, "mount")
        for client, ret in results:
            if ret.get() < 0:
                logging.error("failed to mount lod client on [%s].", client.ln_hostname)
                return -1
        return 0

    def stop(self):
        """
        Similar to normal lustre umount routine
        """
        results = lod_parallel_do_action(self.lod_client_nodes, "umount")
        for client, ret in results:
            if ret.get() < 0:
                logging.error("failed to umount lod client on [%s].", client.ln_hostname)
                return -1

        results = lod_parallel_do_action(self.lod_oss_nodes, "umount")
        for oss, ret in results:
            if ret.get() < 0:
                logging.error("failed to mount lod ost device[%s] on [%s].",
                              oss.lo_ost, oss.ln_hostname)
                return -1

        for mds in self.lod_mds_nodes:
            if mds.umount():
                logging.error("failed to umount lod mdt device[%s] on [%s].",
                              mds.lm_mdt, mds.ln_hostname)
                return -1
        return 0


class LodNode(object):
    """
    Lod Node
    """
    def __init__(self, hostname, mgs, fsname,
                 mountpoint, net, args=None):
        # pylint: disable=too-many-arguments
        self.ln_hostname = hostname
        self.ln_mgs = mgs
        self.ln_fsname = fsname
        self.ln_mountpoint = mountpoint
        self.ln_net = net
        self.ln_args = args

    def is_up(self, timeout=60):
        """
        Whether this host is up now
        """
        ret = self.run("true", timeout=timeout)
        if ret != 0:
            return False
        return True

    def has_command(self, command):
        """
        Whether has command @cmd
        """
        ret = self.run("which %s" % command)
        return ret == 0

    def run(self, command, silent=False, login_name="root",
            timeout=120, stdout_tee=None,
            stderr_tee=None, stdin=None, return_stdout=True,
            return_stderr=True, quit_func=None, flush_tee=False):
        """
        Run a command on the host
        """
        # pylint: disable=too-many-arguments
        if not silent:
            logging.debug("starting [%s] on host [%s]", command,
                          self.ln_hostname)
        ret = ssh_host.ssh_run(self.ln_hostname, command, login_name=login_name,
                               timeout=timeout,
                               stdout_tee=stdout_tee, stderr_tee=stderr_tee,
                               stdin=stdin, return_stdout=return_stdout,
                               return_stderr=return_stderr, quit_func=quit_func,
                               flush_tee=flush_tee)
        if ret.cr_exit_status != 0:
            if not silent:
                logging.error("failed to run [%s] on host [%s]."
                              "ret = [%d], stdout = [%s], stderr = [%s]",
                              command, self.ln_hostname,
                              ret.cr_exit_status, ret.cr_stdout,
                              ret.cr_stderr)
            return -1
        elif not silent:
            logging.debug("Succeed ran [%s] on host [%s], ret = [%d], stdout = [%s], "
                          "stderr = [%s]",
                          command, self.ln_hostname, ret.cr_exit_status,
                          ret.cr_stdout, ret.cr_stderr)
        return 0

    def command_job(self, command, timeout=None, stdout_tee=None,
                    stderr_tee=None, stdin=None):
        """
        Return the command job on a host
        """
        # pylint: disable=too-many-arguments
        full_command = ssh_host.ssh_command(self.ln_hostname, command)
        job = utils.CommandJob(full_command, timeout, stdout_tee, stderr_tee,
                               stdin)
        return job

    def check_network_connection(self, remote_host):
        """
        Check whether the Internet connection works well
        """
        command = "ping -c 1 %s" % remote_host
        return self.run(command)

    def ping(self):
        """
        Check whether local host can ping this host
        """
        command = "ping -c 1 %s" % self.ln_hostname
        return utils.run(command)

    def umount(self):
        """
        Umount the file system of a device
        """
        ret = self.run("umount %s" % self.ln_mountpoint)
        if ret != 0:
            logging.debug("failed to do normal umount [%s] on host [%s], "
                          "try with -f again",
                          self.ln_mountpoint, self.ln_hostname)
            ret = self.run("umount -f %s" % self.ln_mountpoint)
            if ret != 0:
                logging.error("failed to force umount [%s] on host [%s]",
                              self.ln_mountpoint, self.ln_hostname)
                return -1
        return 0


class LodMds(LodNode):
    """
    Lod Mds
    """
    def __init__(self, hostname, mdt, mgs, index, fsname,
                 mountpoint="/mnt/lod_mdt", net="tcp", args=None):
        # pylint: disable=too-many-arguments
        super(LodMds, self).__init__(hostname, mgs, fsname,
                                     mountpoint, net, args)
        self.lm_mdt = mdt
        self.lm_index = index

    def mkfs(self):
        """
        Format
        """
        ret = self.run("test -b %s" % self.lm_mdt)
        if ret != 0:
            logging.error("device [%s] is not a device", self.lm_mdt)
            return -1

        if self.lm_index == 0:
            command = ("mkfs.lustre --fsname=%s --mdt --mgs --index=0 --reformat %s" %
                       (self.ln_fsname, self.lm_mdt))
        else:
            command = ("mkfs.lustre --fsname=%s --mdt --mgsnode=%s@%s --index=%d --reformat %s" %
                       (self.ln_fsname, self.ln_mgs, self.ln_net, self.lm_index, self.lm_mdt))
        return self.run(command)

    def mount(self):
        """
        Mount
        """
        ret = self.run("test -e %s" % self.ln_mountpoint, silent=True)
        if ret != 0:
            ret = self.run("mkdir -p %s" % self.ln_mountpoint)
            if ret != 0:
                logging.error("failed to create directory [%s]", self.ln_mountpoint)
                return -1
        else:
            ret = self.run("test -d %s" % self.ln_mountpoint)
            if ret != 0:
                logging.error("[%s] is not directory", self.ln_mountpoint)
                return -1

        command = "mount -t lustre %s %s" % (self.lm_mdt, self.ln_mountpoint)
        return self.run(command)


class LodOss(LodNode):
    """
    Lod Mds
    """
    def __init__(self, hostname, ost, mgs, index, fsname,
                 mountpoint="/mnt/lod_ost", net="tcp", args=None):
        # pylint: disable=too-many-arguments
        super(LodOss, self).__init__(hostname, mgs, fsname,
                                     mountpoint, net, args)
        self.lo_ost = ost
        self.lo_index = index

    def mkfs(self):
        """
        Format
        """
        ret = self.run("test -b %s" % self.lo_ost)
        if ret != 0:
            logging.error("device [%s] is not a device", self.lo_ost)
            return -1

        command = ("mkfs.lustre --fsname=%s --ost --mgsnode=%s@%s --index=%d --reformat %s" %
                   (self.ln_fsname, self.ln_mgs, self.ln_net, self.lo_index, self.lo_ost))
        return self.run(command)

    def mount(self):
        """
        Mount
        """
        ret = self.run("mkdir -p %s" % self.ln_mountpoint)
        if ret != 0:
            return ret

        command = "mount -t lustre %s %s" % (self.lo_ost, self.ln_mountpoint)
        return self.run(command)


class LodClient(LodNode):
    """
    Lod Mds
    """
    def __init__(self, hostname, mgs, fsname,
                 mountpoint="/mnt/lod_client", net="tcp", args=None):
        # pylint: disable=too-many-arguments
        super(LodClient, self).__init__(hostname, mgs, fsname,
                                        mountpoint, net, args="user_xattr,flock")

    def mount(self):
        """
        Mount
        """
        ret = self.run("mkdir -p %s" % self.ln_mountpoint)
        if ret != 0:
            return ret

        command = ("mount -t lustre -o %s %s@%s:/%s %s" %
                   (self.ln_args, self.ln_mgs, self.ln_net,
                    self.ln_fsname, self.ln_mountpoint))
        return self.run(command)


def lod_parse_config(config, slurm_nodes=None, fsname=None, mdtdevs=None, ostdevs=None,
                     inet=None, mountpoint=None):
    """
    Parse lod configuration
    """
    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    # take slurm nodes directly if found
    node_list = None
    client_list = None
    mds_list = None
    oss_list = None
    if slurm_nodes:
        logging.debug("Slurm node: %s.", slurm_nodes)
        node_list = slurm_nodes
    else:
        nodes = config.get(LOD_NODES)
        if nodes is None:
            logging.error("No *nodes* found in %s.", LOD_CONFIG)
            return None
        node_list = nodes.strip().split(',')

        mds = config.get(LOD_MDS)
        if mds is not None:
            mds_list = mds.strip().split(',')

        oss = config.get(LOD_OSS)
        if oss is not None:
            oss_list = oss.strip().split(',')

        clients = config.get(LOD_CLIENTS)
        if clients is not None:
            client_list = clients.strip().split(',')

    if inet is not None:
        net = inet
    elif config is not None:
        net = config.get(LOD_NET)
    else:
        net = LOD_DEFAULT_NET

    device = None
    if mdtdevs is not None:
        mdt_device = mdtdevs
    else:
        device = config.get(LOD_DEVICE)
        mdt_device = config.get(LOD_MDT_DEVICE)

    if ostdevs is not None:
        ost_device = ostdevs
    else:
        ost_device = config.get(LOD_OST_DEVICE)

    if fsname is not None:
        fs_name = fsname
    elif config is not None:
        fs_name = config.get(LOD_FSNAME)
    else:
        fs_name = LOD_DEFAULT_FSNAME

    if mountpoint:
        mount_point = mountpoint
    elif config is not None:
        mount_point = config.get(LOD_MOUNTPOINT)
    else:
        mount_point = LOD_DEFAULT_MOUNTPOINT

    return LodConfig(node_list, device, mdt_device, ost_device,
                     mds_list, oss_list, client_list,
                     net, fs_name, mount_point)


def devide_zero_prefix_index(number):
    """
    Split the zero prefix
    """
    if not number.startswith("0"):
        return None, int(number)
    i = 0

    while i < len(number):
        if number[i] != "0":
            break
        i += 1
    return number[0:i], 0 if i >= len(number) else int(number[i:])


def parse_slurm_node_string(node_string):
    """
    Parse the node string
    """
    # pylint: disable=unused-variable
    node_array = list()
    reg = re.compile(r"(?P<comname>[\w\-.]+)"
                     r"(?P<range>\[(?P<start>\d+)\-(?P<stop>\d+)\])?",
                     re.VERBOSE)
    for item in node_string.split(","):
        match = reg.match(item.strip())
        comname = match.group("comname")
        if not match.group("range"):
            node_array.append(comname)
        else:
            start_raw = match.group("start")
            stop_raw = match.group("stop")
            start_zeros, start_num = devide_zero_prefix_index(start_raw)
            stop_zeros, stop_num = devide_zero_prefix_index(stop_raw)
            node_array.append(comname + start_raw)
            start_index = start_num + 1
            while start_index <= stop_num:
                if start_zeros is not None and len(start_zeros) > 0:
                    # 9->10  99->100
                    if len(str(start_index)) > len(str(start_index - 1)):
                        start_zeros = start_zeros[-1]
                    node_array.append(comname + start_zeros + str(start_index))
                else:
                    node_array.append(comname + str(start_index))
                start_index += 1
    return node_array


def configure_logging():
    """
    Configure the logging settings
    """
    default_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] "
                                          "[%(filename)s:%(lineno)s] "
                                          "%(message)s",
                                          "%Y/%m/%d-%H:%M:%S")
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(default_formatter)

    logging.root.handlers = []
    logging.root.setLevel(logging.DEBUG)
    logging.root.addHandler(console_handler)


def main():
    """
    Run LOD (Lustre On Demand)
    """
    # pylint: disable=unused-variable,bare-except,too-many-locals
    # pylint: disable=global-statement,too-many-branches,too-many-statements
    reload(sys)
    sys.setdefaultencoding("utf-8")
    config_fpath = LOD_CONFIG

    reload(sys)
    sys.setdefaultencoding("utf-8")

    options, args = getopt.getopt(sys.argv[1:],
                                  "dc:f:m:n:o:i:p:s:S:D:M:h",
                                  ["dry-run",
                                   "config=",
                                   "fsname=",
                                   "mdtdevs=",
                                   "node=",
                                   "ostdevs=",
                                   "inet=",
                                   "mountpoint=",
                                   "source=",
                                   "sourcelist=",
                                   "destination=",
                                   "mpirun=",
                                   "help"])

    dry_run = False
    fsname = None
    mdtdevs = None
    ostdevs = None
    node_list = None
    inet = None
    mountpoint = None

    global SOURCE
    global SOURCE_LIST
    global DEST
    global OPERATOR
    global MPIRUN

    for opt, arg in options:
        if opt == '-d' or opt == "--dry-run" or opt == "-dry-run":
            dry_run = True
        elif opt == '-c' or opt == "--config" or opt == "-config":
            config_fpath = arg
        elif opt == '-f' or opt == "--fsname" or opt == "-fsname":
            fsname = arg
        elif opt == '-m' or opt == "--mdtdevs" or opt == "-mdtdevs":
            mdtdevs = arg
        elif opt == '-n' or opt == "--node" or opt == "-node":
            node_list = arg
        elif opt == '-o' or opt == "--ostdevs" or opt == "-ostdevs":
            ostdevs = arg
        elif opt == '-i' or opt == "--inet" or opt == "-inet":
            inet = arg
        elif opt == '-p' or opt == "--mountpoint" or opt == "-mountpoint":
            mountpoint = arg
        elif opt == '-s' or opt == "--source" or opt == "-source":
            SOURCE = arg
        elif opt == '-S' or opt == "--sourcelist" or opt == "-sourcelist":
            SOURCE_LIST = arg
        elif opt == '-D' or opt == "--destination" or opt == "-destination":
            DEST = arg
        elif opt == '-M' or opt == "--mpirun" or opt == "-mpirun":
            MPIRUN = arg
        elif opt == '-h' or opt == "--help" or opt == "-help":
            usage()
            sys.exit(1)

    if len(args) != 1:
        usage()
        sys.exit(1)

    OPERATOR = args[0]

    if OPERATOR not in VALID_OPTS:
        logging.error("Invalid operator [%s]", OPERATOR)
        usage()
        sys.exit(-1)

    configure_logging()

    # get node list from slurm if configure
    slurm_node_string = ""
    slurm_node_array = list()

    if "SLURM_JOB_NODELIST" in os.environ:
        slurm_node_string = os.environ["SLURM_JOB_NODELIST"]
        logging.error("SLURM_JOB_NODELIST: %s", slurm_node_string)
    elif "SLURM_NODELIST" in os.environ:
        slurm_node_string = os.environ["SLURM_NODELIST"]
        logging.error("SLURM_NODELIST: %s", slurm_node_string)

    if len(slurm_node_string) > 0:
        slurm_node_array = parse_slurm_node_string(slurm_node_string)
    elif node_list is not None and len(node_list) > 0:
        slurm_node_array = parse_slurm_node_string(node_list)

    config = None
    if slurm_node_array is None or len(slurm_node_array) == 0 or mdtdevs is None or ostdevs is None:
        try:
            with open(config_fpath) as config_fd:
                config = yaml.load(config_fd)
        except:
            logging.error("not able to load [%s] as yaml file,"
                          "please correct it.\n%s", config_fpath,
                          traceback.format_exc())
            sys.exit(-1)

    lod_conf = lod_parse_config(config, slurm_node_array,
                                fsname, mdtdevs, ostdevs,
                                inet, mountpoint)
    if lod_conf is None:
        sys.exit(-1)

    ret = lod_conf.adjust_and_validate()
    if not ret:
        logging.error("invalid configuration [%s], please correct it.", config_fpath)
        sys.exit(-1)

    lod = Lod(lod_conf)
    if lod is None:
        logging.error("failed to create LOD with configuration [%s], please correct it.",
                      config_fpath)
        sys.exit(-1)

    if dry_run:
        lod.show_topology()
        sys.exit(0)

    ret = lod.do_action(OPERATOR)
    if ret < 0:
        logging.error("failed to [%s] LOD.", OPERATOR)
        sys.exit(-1)


if __name__ == "__main__":
    main()
