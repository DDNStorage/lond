# Copyright (c) 2019 DataDirect Networks, Inc.
# All Rights Reserved.
# Author: lixi@ddn.com

"""
Imported by defintion.py for some classes
"""
import os


class CommandOption(object):
    """
    An option of a C language command
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self, long_opt, has_arg, short_opt=None):
        self.co_short_opt = short_opt
        self.co_long_opt = long_opt
        self.co_has_arg = has_arg


class CommandArgument(object):
    """
    The stand-alone argument that a C language command uses
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self, is_dir_path=False):
        # Whether this argument is a file path
        self.ca_is_dir_path = is_dir_path


class CommandOptions(object):
    """
    All options of a C language command
    """
    # pylint: disable=too-few-public-methods,too-many-arguments
    def __init__(self):
        # Array of CommandOption
        self.cos_options = []
        # Array of CommandArgument
        self.cos_arguments = []
        self.cos_has_dir_path = False

    def cos_add_option(self, option):
        """
        Add a new option
        """
        self.cos_options.append(option)

    def cos_add_argument(self, argument):
        """
        Add a new argument
        """
        self.cos_arguments.append(argument)
        if argument.ca_is_dir_path:
            self.cos_has_dir_path = True

    def cos_candidates(self):
        """
        Return candidates for completers
        """
        candidates = []
        for option in self.cos_options:
            candidates.append("--" + option.co_long_opt)
            if option.co_short_opt is not None:
                candidates.append("-" + option.co_short_opt)

        if self.cos_has_dir_path:
            for fname in os.listdir(os.getcwd()):
                if os.path.isdir(fname):
                    candidates.append(fname + "/")

        return candidates

    def cos_complete_path(self, fpath):
        """
        Return candidates for completers
        """
        if not self.cos_has_dir_path:
            return None

        if ((not fpath.startswith("/")) and (not fpath.startswith("./")) and
                (not fpath.startswith("../"))):
            return None

        dirname = os.path.dirname(fpath)
        candidates = []
        for fname in os.listdir(dirname):
            if dirname == "/":
                dir_fpath = "/" + fname
            else:
                dir_fpath = dirname + "/" + fname
            if not os.path.isdir(dir_fpath):
                continue
            if dir_fpath.startswith(fpath):
                candidates.append(dir_fpath + "/")
        return candidates
