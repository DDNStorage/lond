/*
 * Copyright (c) 2019, DDN Storage Corporation.
 */
/*
 *
 * Tool for Lustre On Demand.
 *
 * Author: Li Xi <lixi@ddn.com>
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <getopt.h>
#include <linux/limits.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <attr/xattr.h>
#include <fcntl.h>
#include <libgen.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <ftw.h>
#include <uthash.h>
#include <inttypes.h>
#ifdef NEW_USER_HEADER
#include <linux/lustre/lustre_user.h>
#else
#include <lustre/lustre_user.h>
#endif
#include <lustre/lustreapi.h>
#include "definition.h"
#include "debug.h"
#include "cmd.h"
#include "lond.h"

struct dest_entry *dest_entry_table;

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s [-d] -k <key> <file>...\n"
		"  file: Lustre directory tree or regular file to unlock\n"
		"  key: lock key, use \"%s\" to unlock without checking key\n",
		prog, LOND_KEY_ANY);
}

/*
 * Assumptions:
 * 1) Files are all Lustre directories/files with any type.
 * 2) Files could be in different Lustre file system.
 * 3) Files could be unlocked or locked.
 * 4) No one else except LOND is using immutable flag.
 * 5) The tree of could mount another Lustre file system, but not other file system.
 */
int main(int argc, char *const argv[])
{
	int i;
	int rc;
	int rc2 = 0;
	const char *file;
	struct option long_opts[] = LOND_UNLOCK_OPTIONS;
	char *progname;
	char *key = NULL;
	char short_opts[] = "dhk:";
	struct stat file_sb;
	bool recursive = true;
	int c;

	progname = argv[0];
	while ((c = getopt_long(argc, argv, short_opts,
				long_opts, NULL)) != -1) {
		switch (c) {
		case OPT_PROGNAME:
			progname = optarg;
			break;
		case 'h':
			usage(progname);
			return 0;
		case 'k':
			key = optarg;
			if (!is_valid_key(key)) {
				LERROR("invalid key [%s]\n", key);
				return -EINVAL;
			}
			break;
		case 'd':
			recursive = false;
			break;
		default:
			LERROR("failed to parse option [%c]\n", c);
			usage(progname);
			return -EINVAL;
		}
	}

	if (key == NULL) {
		LERROR("please specify lock key by using [-k] option\n");
		usage(progname);
		return -EINVAL;
	}

	if (argc < optind + 1) {
		LERROR("need one or more Lustre files/directories as arguments\n");
		usage(progname);
		return -EINVAL;
	}

	for (i = optind; i < argc; i++) {
		file = argv[i];
		rc = lstat(file, &file_sb);
		if (rc) {
			LERROR("failed to unlock [%s] because stat failed: %s\n",
			       file, strerror(errno));
			rc2 = rc2 ? rc2 : rc;
			continue;
		}

		if (!recursive || S_ISREG(file_sb.st_mode)) {
			LINFO("unlocking inode [%s] with key [%s]\n", file,
			      key);
			rc = lond_inode_unlock(file, key, false);
			if (rc) {
				LERROR("failed to unlock file [%s] with key [%s]: %s\n",
				       file, key, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				continue;
			}
			LINFO("unlocked inode [%s] with key [%s]\n", file,
			      key);
		} else if (S_ISDIR(file_sb.st_mode)) {
			LINFO("unlocking directory tree [%s] with key [%s]\n",
			      file, key);
			rc = chdir(file);
			if (rc) {
				LERROR("failed to unlock tree [%s] with key [%s] because failed to chdir to it: %s\n",
				       file, key, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				continue;
			}

			rc = lond_tree_unlock(file, key, true);
			if (rc) {
				LERROR("failed to unlock tree [%s] with key [%s]: %s\n",
				       file, key, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				continue;
			}
			LINFO("unlocked directory tree [%s] with key [%s]\n",
			      file, key);
		} else {
			LINFO("[%s] is not locked\n", file);
		}
	}

	return rc2;
}
