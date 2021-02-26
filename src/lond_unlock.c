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
		"  key: lock key, use \"%s\" to unlock without checking key\n"
		"  -d: only unlock directory itslef, not its sub-tree recursively\n",
		prog, LOND_KEY_ANY);
}

static int hex_char2int(char c)
{
	if (c >= '0' && c <= '9')
		return c - '0';
	else if (c >= 'A' && c <= 'F')
		return c - 'A' + 10;
	else if (c >= 'a' && c <= 'f')
		return c - 'a' + 10;
	else
		return -EINVAL;
}

static int lond_string2key(const char *key_str, struct lond_key *key)
{
	int i;
	int high;
	int low;

	if (strlen(key_str) != LOND_KEY_STRING_SIZE - 1) {
		LERROR("invalid key length of [%s], expected %d, got %d\n",
		       key_str, LOND_KEY_STRING_SIZE - 1, strlen(key_str));
		return -EINVAL;
	}

	if (LOND_KEY_ARRAY_LENGH * 2 != strlen(key_str)) {
		LERROR("unexpected length of key string [%d], expected %d\n",
		       strlen(key_str), LOND_KEY_ARRAY_LENGH * 2);
		return -EINVAL;
	}

	for (i = 0; i < LOND_KEY_ARRAY_LENGH; i++) {
		high = hex_char2int(key_str[2 * i]);
		low = hex_char2int(key_str[2 * i + 1]);
		if (high < 0 || low < 0) {
			LERROR("invalid key [%s]\n", key_str);
			return -EINVAL;
		}
		key->lk_key[i] = (char)(high * 16 + low);
	}
	return 0;
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
	int c;
	int i;
	int rc;
	int rc2 = 0;
	const char *file;
	struct option long_opts[] = LOND_UNLOCK_OPTIONS;
	char *progname;
	char short_opts[] = "dhk:";
	struct stat file_sb;
	bool recursive = true;
	struct lond_key key;
	const char *key_str = NULL;
	bool any_key = false;
	char *cwd;
	char cwd_buf[PATH_MAX + 1];
	int cwdsz = sizeof(cwd_buf);

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
			key_str = optarg;
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

	if (key_str == NULL) {
		LERROR("please specify lock key by using [-k] option\n");
		usage(progname);
		return -EINVAL;
	}

	if (argc < optind + 1) {
		LERROR("need one or more Lustre files/directories as arguments\n");
		usage(progname);
		return -EINVAL;
	}

	if (strcmp(key_str, LOND_KEY_ANY) == 0) {
		any_key = true;
	} else {
		rc = lond_string2key(key_str, &key);
		if (rc) {
			LERROR("invalid key [%s]\n", key_str);
			return -EINVAL;
		}
	}

	cwd = getcwd(cwd_buf, cwdsz);
	if (cwd == NULL) {
		LERROR("failed to get cwd: %s\n", strerror(errno));
		return -errno;
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
			      key_str);
			rc = lond_inode_unlock(file, any_key, &key, false);
			if (rc) {
				LERROR("failed to unlock file [%s] with key [%s]: %s\n",
				       file, key_str, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				continue;
			}
			LINFO("unlocked inode [%s] with key [%s]\n", file,
			      key_str);
		} else if (S_ISDIR(file_sb.st_mode)) {
			LINFO("unlocking directory tree [%s] with key [%s]\n",
			      file, key_str);
			rc = chdir(file);
			if (rc) {
				LERROR("failed to unlock tree [%s] with key [%s] because failed to chdir to it: %s\n",
				       file, key_str, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				continue;
			}

			rc = lond_tree_unlock(".", any_key, &key, true);
			if (rc) {
				LERROR("failed to unlock tree [%s] with key [%s]: %s\n",
				       file, key_str, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				continue;
			}

			LINFO("unlocked directory tree [%s] with key [%s]\n",
			      file, key_str);

			rc = chdir(cwd);
			if (rc) {
				LERROR("failed to chdir to [%s]: %s\n",
				       cwd, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				break;
			}
		} else {
			LINFO("[%s] is not locked\n", file);
		}
	}

	return rc2;
}
