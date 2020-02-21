#!/usr/bin/python -u
# Copyright (c) 2018-2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: Gu Zheng <gzheng@ddn.com>
# pylint: disable=too-many-lines
"""
Lustre On Demand
"""
import random
import string
import traceback
import logging
import copy
import sys
import os
import re
import getopt
from multiprocessing import Pool as LodWorkPool
from pylcommon import utils
from pylcommon import ssh_host

VALID_OPTS = ("initialize", "start", "stop", "status", "stage_in", "stage_out")
VALID_STAGE_OPTS = ("stage_in", "stage_out")
VALID_NET_TYPE_PATTERN = ("tcp*", "o2ib*")
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
    utils.eprint("""Usage: lod [-h/--help]
 [-d/--dry-run] -n/--node c01,c[02-04] [--mds c01] [--oss c[02-04]]
 --mdtdevs /dev/sda --ostdevs /dev/sdb --fsname mylod --mountpoint /mnt/lod
  start/stop/initialize""")
    utils.eprint("	-d/--dry-run :*dry* run, don't do real job")
    utils.eprint("	-n/--node :, run lod with specified node list, also means the clients")
    utils.eprint("	-T/--mds :MDS")
    utils.eprint("	-O/--oss :OSS")
    utils.eprint("	-I/--index :instance index")
    utils.eprint("	-m/--mdtdevs :mdt device")
    utils.eprint("	-o/--ostdevs :ost device")
    utils.eprint("	-f/--fsname :lustre instance fsname")
    utils.eprint("	-i/--inet :networks interface, e.g. tcp0, o2ib01")
    utils.eprint("	-p/--mountpoint :mountpoint on client")
    utils.eprint("	-h/--help :show usage\n")
    utils.eprint("""for stage_in/stage_out""")
    utils.eprint("lod --source=/foo/foo1 --destination /foo2/ stage_in")
    utils.eprint("lod --sourcelist=/foo/foo1_list --source=/foo_dir --destination /foo2/ stage_out")


def lod_generate_random_str(randomlength=16):
    """
    Generate random string
    """
    str_list = [random.choice(string.digits + string.ascii_letters) for _ in range(randomlength)]
    random_str = ''.join(str_list)
    return random_str


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
                 fsname=LOD_DEFAULT_FSNAME, mountpoint=LOD_DEFAULT_MOUNTPOINT,
                 index=None):
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
        self.lc_index = index

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
            self.lc_mds_list = [self.lc_node_list[0]]

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

        # change MDT OST format to [node]:[dev1],[dev2] ?
        # e.g. --mdt vm1:/dev/sda --ost vm1:/dev/sdb,/dev/sdc --ost vm2:/dev/sdc
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
        self.lod_index = lod_config.lc_index

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
                mgs: /dev/vdx	---> mds0
                mdt0: /dev/vda	---> mds0
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
        MOUNTPOINT: /mnt/lod
        """
        utils.eprint("Operator: %s" % OPERATOR)
        utils.eprint("MDS: %s" % [mds_node.ln_hostname for mds_node in self.lod_mds_nodes])
        for mds_node in self.lod_mds_nodes:
            if mds_node.lm_index == 0:
                utils.eprint("	%10s: %s	---> %s" %
                             ("mgs", mds_node.lm_mgt, mds_node.ln_hostname))
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

        for mds in list(reversed(self.lod_mds_nodes)):
            if mds.umount():
                logging.error("failed to umount lod mdt device[%s] on [%s].",
                              mds.lm_mdt, mds.ln_hostname)
                return -1
        return 0


class LodStage(object):
    """
    Lod Stage instance
    """
    def __init__(self, nodes, source, sourcelist, dest):
        self.ls_stage_nodes = nodes
        self.ls_stage_source = source
        self.ls_stage_sourcelist = sourcelist
        self.ls_stage_dest = dest

    def show(self):
        """
        Show details only if dryrun
        """
        if self.ls_stage_source is not None:
            utils.eprint("Source: %s" % self.ls_stage_source)
        elif self.ls_stage_sourcelist is not None:
            utils.eprint("Sourcelist: %s" % self.ls_stage_sourcelist)
        utils.eprint("Destination: %s" % self.ls_stage_dest)

    def stage_in(self):
        """
        Stage in operation
        """
        return self._do_cp()

    def stage_out(self):
        """
        Stage out operation
        """
        return self._do_cp()

    def _do_cp(self):
        """
        Copy @source file or files in sourcelist to @dest
        if source is set, means single file, use cp instead if dcp not installed
        elif sourcelist is set, dcp is required.
        """
        if self.ls_stage_dest is None or self.ls_stage_source is None:
            logging.error("pelease specify source and destination")
            return -1

        # take first client node to run copy job
        cp_node = self.ls_stage_nodes[0]

        no_dcp = False

        lod_dcp_nodes = list()
        # detect all client and store them into lisr
        for node in self.ls_stage_nodes:
            if node.has_command("dcp") and node.has_command("mpirun"):
                lod_dcp_nodes.append(node)

        if len(lod_dcp_nodes) == 0:
            if self.ls_stage_sourcelist is not None:
                logging.error("No valid dcp and mpi-runtime found on nodes %s, exit",
                              [node.ln_hostname for node in self.ls_stage_nodes])
                return -1
            no_dcp = True

        source_items = " ".join(self.ls_stage_source.strip().split(","))

        if no_dcp:
            cp_command = ("rsync --delete --timeout=1800 -az %s %s" %
                          (source_items, self.ls_stage_dest))
        else:
            hostname_list = ",".join([node.ln_hostname for node in lod_dcp_nodes])
            mpirun_prefix = "mpirun -np 4 " if MPIRUN is None else MPIRUN
            if self.ls_stage_sourcelist is None:
                cp_command = ("%s --host %s dcp --sparse --preserve %s %s" %
                              (mpirun_prefix, hostname_list, source_items,
                               self.ls_stage_dest))
            else:
                cp_command = ("%s --host %s dcp --sparse --preserve --input %s %s %s" %
                              (mpirun_prefix, hostname_list, self.ls_stage_sourcelist,
                               source_items, self.ls_stage_dest))

        logging.debug("run command [%s]", cp_command)

        ret = cp_node.run(cp_command)
        if ret:
            logging.error("failed to command [%s] on node [%s]",
                          cp_command, cp_node.ln_hostname)
            logging.error(traceback.format_exc())
        return ret


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
    def __init__(self, hostname, mdt, mgs, index, fsname, net="tcp", args=None):
        # pylint: disable=too-many-arguments
        random_hidden_path = "_".join((".mdt", fsname, str(index)))
        mountpoint = os.path.join("/mnt/", random_hidden_path)
        super(LodMds, self).__init__(hostname, mgs, fsname,
                                     mountpoint, net, args)
        self.lm_mdt = mdt
        self.lm_index = index
        self.lm_mgt = ""
        self.lm_mgt_mountpoint = ""
        if index == 0:
            self.lm_mgt = "/tmp/.lod_mgt_200M"
            self.lm_mgt_mountpoint = os.path.join("/mnt/", ".mgs_lod")

    def mkfs(self):
        """
        Format
        """
        ret = self.run("test -b %s" % self.lm_mdt)
        if ret != 0:
            logging.error("device [%s] is not a device", self.lm_mdt)
            return -1

        command = ("mkfs.lustre --fsname=%s --mdt --mgsnode=%s@%s --index=%d --reformat %s" %
                   (self.ln_fsname, self.ln_mgs, self.ln_net, self.lm_index, self.lm_mdt))
        return self.run(command)

    def mount(self):
        """
        Mount
        """
        # check MGS service, if no MGS service, make MGS combine with MDT0000
        if self.lm_index == 0:
            command = "lctl get_param mgs.MGS.uuid"
            ret = self.run(command, silent=True)
            if ret < 0:
                logging.info("starting MGS service on [%s]", self.ln_hostname)
                # setup mgt
                command = ("mkfs.lustre --mgs --reformat --device-size=200000 %s" %
                           self.lm_mgt)
                ret = self.run(command, silent=True)
                if ret < 0:
                    logging.error("failed to format mgt [%s]", self.lm_mgt)
                    return -1

                ret = self.run("test -e %s" % self.lm_mgt_mountpoint, silent=True)
                if ret != 0:
                    ret = self.run("mkdir -p %s" % self.lm_mgt_mountpoint)
                    if ret != 0:
                        logging.error("failed to create directory [%s]",
                                      self.lm_mgt_mountpoint)
                        return -1

                command = ("mount -t lustre -o loop %s %s" %
                           (self.lm_mgt, self.lm_mgt_mountpoint))
                ret = self.run(command, silent=True)
                # failed to mount?
                if ret < 0:
                    command = "lctl get_param mgs.MGS.uuid"
                    ret = self.run(command, silent=True)
                    # mgs already mount (maybe by other jobs)
                    if ret == 0:
                        pass
                    else:
                        logging.error("failed to mount mgt [/tmp/.lod_mgt_200M] to %s",
                                      self.lm_mgt_mountpoint)
                        return -1

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

    def umount(self):
        # umount mdt first, and if no others are using this MGT, umount it too
        ret = super(LodMds, self).umount()
        if ret < 0:
            return ret

        if self.lm_index > 0:
            return 0

        command = "lctl list_param mdt.*"
        ret = self.run(command, silent=True)
        if ret == 0:
            return 0

        logging.info("no MDT active on [%s], try to stop MGS service now.",
                     self.ln_hostname)

        command = "lctl get_param mgs.MGS.uuid"
        # if mgt already umount, skip it
        ret = self.run(command, silent=True)
        if ret < 0:
            return 0

        ret = self.run("umount -d %s" % self.lm_mgt_mountpoint)
        if ret != 0:
            logging.debug("failed to do normal umount [%s] on host [%s], "
                          "try with -f again",
                          self.lm_mgt_mountpoint, self.ln_hostname)
            ret = self.run("umount -d -f %s" % self.lm_mgt_mountpoint)
            if ret != 0:
                logging.error("failed to force umount [%s] on host [%s]",
                              self.lm_mgt_mountpoint, self.ln_hostname)
                return -1
        return 0


class LodOss(LodNode):
    """
    Lod Mds
    """
    def __init__(self, hostname, ost, mgs, index, fsname,
                 net="tcp", args=None):
        # pylint: disable=too-many-arguments
        random_hidden_path = "_".join((".ost", fsname, str(index)))
        mountpoint = os.path.join("/mnt/", random_hidden_path)
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


def lod_build_config(slurm_nodes, mds_list, oss_list, fsname, mdtdevs, ostdevs,
                     inet, mountpoint, index):
    """
    Build lod configuration for LOD instance
    """
    # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
    # take slurm nodes directly if found
    node_list = slurm_nodes
    client_list = slurm_nodes

    if slurm_nodes:
        logging.debug("Slurm node: %s.", slurm_nodes)

    if inet is not None:
        net = inet
    else:
        net = LOD_DEFAULT_NET

    device = None
    if mdtdevs is not None and len(mdtdevs) > 0:
        mdt_device = mdtdevs
    else:
        logging.error("no mdtdevs found")
        return None

    if ostdevs is not None and len(ostdevs) > 0:
        ost_device = ostdevs
    else:
        logging.error("no ostdevs found")
        return None

    if fsname is not None:
        fs_name = fsname
    else:
        fs_name = LOD_DEFAULT_FSNAME

    if mountpoint:
        mount_point = mountpoint
    else:
        mount_point = LOD_DEFAULT_MOUNTPOINT

    return LodConfig(node_list, device, mdt_device, ost_device,
                     mds_list, oss_list, client_list,
                     net, fs_name, mount_point, index)


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

    reload(sys)
    sys.setdefaultencoding("utf-8")

    options, args = getopt.getopt(sys.argv[1:],
                                  "dT:O:f:m:n:o:i:p:s:S:D:M:hI:",
                                  ["dry-run",
                                   "fsname=",
                                   "mds=",
                                   "oss=",
                                   "mdtdevs=",
                                   "node=",
                                   "ostdevs=",
                                   "inet=",
                                   "mountpoint=",
                                   "source=",
                                   "sourcelist=",
                                   "destination=",
                                   "mpirun=",
                                   "index=",
                                   "help"])

    dry_run = False
    fsname = None
    mdtdevs = None
    ostdevs = None
    node_list = None
    mds_list = None
    oss_list = None
    inet = None
    mountpoint = None
    index = lod_generate_random_str(24)
    source = None
    sourcelist = None
    dest = None

    global OPERATOR
    global MPIRUN

    for opt, arg in options:
        if opt in ('-d', '--dry-run'):
            dry_run = True
        elif opt in ('-f', '--fsname'):
            fsname = arg
        elif opt in ('-m', '--mdtdevs'):
            mdtdevs = arg
        elif opt in ('-T', '--mds'):
            mds_list = arg
        elif opt in ('-O', '--oss'):
            oss_list = arg
        elif opt in ('-n', '--node'):
            node_list = arg
        elif opt in ('-o', '--ostdevs'):
            ostdevs = arg
        elif opt in ('-i', '--inet'):
            inet = arg
        elif opt in ('-p', '--mountpoint'):
            mountpoint = arg
        elif opt in ('-s', '--source'):
            source = arg
        elif opt in ('-S', '--sourcelist'):
            sourcelist = arg
        elif opt in ('-D', '--destination'):
            dest = arg
        elif opt in ('-M', '--mpirun'):
            MPIRUN = arg
        elif opt in ('-h', '--help'):
            usage()
            sys.exit(1)
        else:
            logging.error("Invalid option [%s]",
                          opt)
            usage()
            sys.exit(1)

    OPERATOR = args[0]

    if str(OPERATOR).lower() not in VALID_OPTS:
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

    if slurm_node_array is None or len(slurm_node_array) == 0:
        logging.error("no nodes specified")
        sys.exit(-1)

    # do stage operator
    if OPERATOR.lower() in VALID_STAGE_OPTS:
        if source is None and sourcelist is None:
            logging.error("no source or sourcelist specified")
            sys.exit(-1)
        if dest is None:
            logging.error("no *dest* specified")
            sys.exit(-1)

        lod_nodes = list()
        for node in slurm_node_array:
            lod_node = LodNode(node, None, None, None, None, None)
            lod_nodes.append(lod_node)

        lod_stage = LodStage(lod_nodes, source, sourcelist, dest)

        if dry_run:
            lod_stage.show()
            return 0

        if OPERATOR.lower() == "stage_in":
            return lod_stage.stage_in()
        elif OPERATOR.lower() == "stage_out":
            return lod_stage.stage_out()
        else:
            logging.error("invalid operator [%s]", OPERATOR)
            usage()
            sys.exit(-1)

    if mdtdevs is None:
        logging.error("no MDT device specified")
        sys.exit(-1)
    if ostdevs is None:
        logging.error("no OST device specified")
        sys.exit(-1)
    if fsname is None:
        logging.error("no fsname specified")
        sys.exit(-1)
    if mountpoint is None:
        logging.error("no mountpoint specified")
        sys.exit(-1)

    lod_conf = lod_build_config(slurm_node_array, mds_list, oss_list,
                                fsname, mdtdevs, ostdevs,
                                inet, mountpoint, index)
    if lod_conf is None:
        sys.exit(-1)

    ret = lod_conf.adjust_and_validate()
    if not ret:
        logging.error("invalid lod options, please correct it.\n%s",
                      sys.argv[1:])
        sys.exit(-1)

    lod = Lod(lod_conf)
    if lod is None:
        logging.error("failed to create LOD, please correct options.\n%s",
                      sys.argv[1:])
        sys.exit(-1)

    if dry_run:
        lod.show_topology()
        sys.exit(0)

    ret = lod.do_action(OPERATOR.lower())
    if ret < 0:
        logging.error("failed to [%s] LOD.", OPERATOR)
        sys.exit(-1)
    return 0


if __name__ == "__main__":
    main()
