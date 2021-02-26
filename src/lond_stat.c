/*
 *
 * Stat dir or file on globbal Lustre.
 *
 * Author: Li Xi <lixi@ddn.com>
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <attr/xattr.h>
#include <unistd.h>
#include <string.h>
#include <lustre/lustreapi.h>
#include "definition.h"
#include "debug.h"
#include "lond.h"

struct dest_entry *dest_entry_table;

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s [-d] <file>...\n"
		"  file: Lustre directory tree or regular file to stat\n"
		"  -d: only unlock directory itslef, not its sub-tree recursively\n",
		prog);
}

/*
 * Assumptions:
 * 1) Files are all Lustre directories/files with any type.
 * 2) Files could be in different Lustre file system.
 * 3) Files could be unlocked or locked.
 * 4) No one else except LOND is using immutable flag.
 * 5) The tree shall not mount another Lustre or any other type of file system.
 */
int main(int argc, char *const argv[])
{
	int i;
	int rc;
	int rc2 = 0;
	const char *file;
	struct option long_opts[] = LOND_STAT_OPTIONS;
	char *progname;
	char short_opts[] = "dh";
	struct stat file_sb;
	bool recursive = true;
	char fsname[MAX_OBD_NAME + 1];
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
		case 'd':
			recursive = false;
			break;
		default:
			LERROR("failed to parse option [%c]\n", c);
			usage(progname);
			return -EINVAL;
		}
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
			LERROR("failed to lond_stat [%s] because stat failed: %s\n",
			       file, strerror(errno));
			rc2 = rc2 ? rc2 : rc;
			continue;
		}

		rc = llapi_search_fsname(file, fsname);
		if (rc == -ENODEV) {
			LERROR("[%s] is not a Lustre directory\n", file);
			return rc;
		} else if (rc) {
			LERROR("failed to find the Lustre fsname of [%s]: %s\n",
			       file, strerror(-rc));
			return rc;
		}

		if (!recursive || S_ISREG(file_sb.st_mode)) {
			rc = lond_inode_stat(file, NULL, file_sb.st_mode);
			if (rc) {
				LERROR("failed to lond stat file [%s]: %s\n",
				       file, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				continue;
			}
		} else if (S_ISDIR(file_sb.st_mode)) {
			rc = chdir(file);
			if (rc) {
				LERROR("failed to lond stat directory tree [%s] because failed to chdir to it: %s\n",
				       file, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				continue;
			}

			rc = lond_tree_stat(".", true);
			if (rc) {
				LERROR("failed to lond stat tree [%s]: %s\n",
				       file, strerror(errno));
				rc2 = rc2 ? rc2 : rc;
				continue;
			}
		} else {
			LINFO("[%s] is not locked\n", file);
		}
	}

	return rc2;
}
