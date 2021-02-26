/*
 *
 * Tool for scanning the MDT and print list of matched files.
 *
 * Author: Li Xi <lixi@ddn.com>
 */
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include "debug.h"

int command_run(char *cmd, int cmdsz)
{
	char log[] = "/tmp/command_run_logXXXXXX";
	int fd = -1, rc;

	if ((cmdsz - strlen(cmd)) < 6) {
		LERROR("command buffer overflow: %.*s...\n",
		       cmdsz, cmd);
		return -1;
	}

	LDEBUG("cmd: %s\n", cmd);
	fd = mkstemp(log);
	if (fd >= 0) {
		close(fd);
		strcat(cmd, " >");
		strcat(cmd, log);
	}
	strcat(cmd, " 2>&1");

	/* Can't use popen because we need the rv of the command */
	rc = system(cmd);
	if (rc && (fd >= 0)) {
		char buf[128];
		FILE *fp;

		fp = fopen(log, "r");
		if (fp) {
			while (fgets(buf, sizeof(buf), fp) != NULL)
				printf("   %s", buf);

			fclose(fp);
		}
	}
	if (fd >= 0)
		remove(log);
	return rc;
}

int command_read(char *cmd, char *buf, int len)
{
	FILE *fp;
	int read;

	fp = popen(cmd, "r");
	if (!fp)
		return -errno;

	read = fread(buf, 1, len, fp);
	pclose(fp);

	if (read == 0)
		return -ENOENT;

	/* strip trailing newline */
	if (buf[read - 1] == '\n')
		buf[read - 1] = '\0';

	return 0;
}
