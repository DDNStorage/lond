/*
 * Copyright (c) 2019, DDN Storage Corporation.
 */
/*
 *
 * Sync dir from on-demand Lustre to globbal Lustre.
 *
 * Author: Li Xi <lixi@ddn.com>
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/types.h>
#include <sys/stat.h>
#include <attr/xattr.h>
#include <unistd.h>
#include <string.h>
#include "definition.h"
#include "debug.h"
#include "lond.h"
#include "cmd.h"

struct dest_entry *dest_entry_table;

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s <source> [dest]\n"
		"  source: local Lustre directory to sync from\n"
		"  dest: global Lustre directory to sync to\n",
		prog);
}

static int lond_sync(const char *source, const char *dest)
{
	int rc;
	char source_fsname[MAX_OBD_NAME + 1];
	char dest_fsname[MAX_OBD_NAME + 1];
	char cmd[PATH_MAX];
	int cmdsz = sizeof(cmd);

	rc = lustre_directory2fsname(source, source_fsname);
	if (rc) {
		LERROR("failed to get the fsname of [%s]\n",
		       source);
		return rc;
	}

	rc = check_lustre_root(source_fsname, source);
	if (rc < 0) {
		LERROR("failed to check whether directory [%s] is the root of file system [%s]\n",
		       source, source_fsname);
		return rc;
	} else if (rc == 0) {
		LERROR("directory [%s] shound't be synced to [%s] because it is the root of file system [%s]\n",
		       source, dest, source_fsname);
		return rc;
	}

	if (dest == NULL) {
		LERROR("support not finished yet\n");
		rc = -EINVAL;
		return rc;
	}

	rc = lustre_directory2fsname(dest, dest_fsname);
	if (rc) {
		LERROR("failed to get the fsname of [%s]\n",
		       dest);
		return rc;
	}

	if (strcmp(source_fsname, dest_fsname) == 0) {
		LERROR("syncing inside the same file system [%s] dosn't make any sense\n",
		       source_fsname);
		return -EINVAL;
	}

	snprintf(cmd, cmdsz, "cp -a '%s' '%s'", source, dest);
	rc = command_run(cmd, cmdsz);
	if (rc) {
		LERROR("failed to sync from [%s] to [%s], rc = %d\n",
		       source, dest, rc);
		return rc;
	}
	return 0;
}

/*
 * Assumptions:
 * 1) Dirs are all on Lustre.
 * 2) Dirs could be in different Lustre file system.
 * 3) Dirs could be synced from global Lustre or not.
 * 5) The dir trees shall not mount another Lustre or any other type of file system.
 */
int main(int argc, char *const argv[])
{
	int rc;
	const char *source;
	const char *dest = NULL;
	struct option long_opts[] = LOND_STAT_OPTIONS;
	char *progname;
	char short_opts[] = "h";
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
		default:
			LERROR("failed to parse option [%c]\n", c);
			usage(progname);
			return -EINVAL;
		}
	}

	if (argc <= optind) {
		LERROR("please specify the local Lustre directory to sync from\n");
		usage(progname);
		return -EINVAL;
	} else if (argc == optind + 1) {
		source = argv[optind];
		dest = NULL;
	} else {
		source = argv[optind];
		dest = argv[optind + 1];
	}

	rc = lond_sync(source, dest);
	if (rc) {
		if (dest != NULL)
			LERROR("failed to sync from [%s] to [%s]\n",
			       source, dest);
		else
			LERROR("failed to sync from [%s] to its source directory on global Lustre\n",
			       source, dest);
		return rc;
	}

	return rc;
}
