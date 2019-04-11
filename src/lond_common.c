/*
 * Copyright (c) 2019, DDN Storage Corporation.
 */
/*
 *
 * Common functions for Lustre On Demand.
 *
 * Author: Li Xi <lixi@ddn.com>
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <attr/xattr.h>
#include <ftw.h>
#include <string.h>
#include <linux/limits.h>
#include "definition.h"
#include "debug.h"
#include "cmd.h"
#include "lond.h"

const char *nftw_key;
bool nftw_ignore_error;
int nftw_errno;

static int check_inode_is_immutable(const char *fpath, bool *immutable)
{
	int i;
	int rc;
	char cmd[PATH_MAX];
	int cmdsz = sizeof(cmd);
	char output[PATH_MAX];
	int output_sz = sizeof(output);

	snprintf(cmd, cmdsz, "lsattr -d '%s'", fpath);
	rc = command_read(cmd, output, output_sz - 1);
	if (rc) {
		LERROR("failed to run command [%s], rc = %d\n",
		       cmd, rc);
		return rc;
	}

	*immutable = false;
	for (i = 0; i < strlen(output); i++) {
		if (output[i] == ' ')
			break;
		if (output[i] == 'i') {
			*immutable = true;
			break;
		}
	}
	return 0;
}


/*
 * Print the EPERM reason of an inode,
 * If the inode is locked by myself, return 0. Otherwise, negative value.
 */
static int lond_lock_eperm_reason(const char *fpath, const char *key)
{
	int rc;
	bool immutable = false;
	char xattr_key[LOND_KEY_LENGH + 1];
	char full_fpath[PATH_MAX];

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}

	rc = check_inode_is_immutable(fpath, &immutable);
	if (rc) {
		LERROR("failed to check whether file [%s] is immutable\n",
		       fpath);
		return rc;
	}

	if (!immutable) {
		LERROR("file [%s] is not immutable as expected\n", full_fpath);
		return -EPERM;
	}

	rc = getxattr(fpath, XATTR_NAME_LOND_KEY, xattr_key,
		      sizeof(xattr_key));
	if (rc < 0) {
		LERROR("failed to get LOND xattr of [%s]: %s\n",
		       full_fpath, strerror(errno));
		LERROR("file [%s] can't be locked because it is immutable\n",
		       full_fpath);
		LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
		       LOND_KEY_ANY, full_fpath, full_fpath);
		return -errno;
	}
	xattr_key[LOND_KEY_LENGH] = '\0';

	if (strcmp(xattr_key, key) == 0)
		return 0;

	if (is_valid_key(xattr_key)) {
		LERROR("file [%s] has already been locked with key [%s]\n",
		       full_fpath, xattr_key);
		LERROR("to unlock, try [lond unlock -k %s %s]\n",
		       xattr_key, full_fpath);
	} else {
		LERROR("file [%s] has already been locked with invalid key [%s]\n",
		       full_fpath, xattr_key);
		LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
		       LOND_KEY_ANY, full_fpath);
	}
	return -EBUSY;
}

/*
 * Steps to lock an inode:
 *
 * 1) Set the xattr;
 * 2) chattr +i;
 * 3) If 2) fails because the flag already exists, ignore the failure.
 * 4) Check whether the xattr has expected value, if not, return failure
 */
int lond_inode_lock(const char *fpath, const char *key)
{
	int rc;
	int rc2;
	char cmd[PATH_MAX];
	int cmdsz = sizeof(cmd);
	char xattr_key[LOND_KEY_LENGH + 1];
	char full_fpath[PATH_MAX];

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}

	rc = setxattr(fpath, XATTR_NAME_LOND_KEY, key, LOND_KEY_LENGH,
		      0);
	if (rc) {
		rc2 = -errno;
		if (errno == EPERM) {
			rc = lond_lock_eperm_reason(fpath, key);
			if (rc == 0)
				return 0;
		}
		LERROR("failed to set lock key of [%s] to [%s]: %s\n",
		       full_fpath, key, strerror(errno));
		return rc2;
	}

	snprintf(cmd, cmdsz, "chattr +i '%s'", fpath);
	rc = command_run(cmd, cmdsz);
	if (rc) {
		LERROR("failed to set immutable flag of [%s], rc = %d\n",
		       full_fpath, rc);
		return rc;
	}

	rc = getxattr(fpath, XATTR_NAME_LOND_KEY, xattr_key,
		      sizeof(xattr_key));
	if (rc < 0) {
		LERROR("failed to get LOND xattr of [%s]: %s\n",
		       full_fpath, strerror(errno));
		return rc;
	}
	xattr_key[LOND_KEY_LENGH] = '\0';

	if (strcmp(xattr_key, key) != 0) {
		LERROR("race of inode [%s] from another lock, expected key [%s], got key [%s]\n",
		       full_fpath, key, xattr_key);
		return -1;
	}
	LDEBUG("set immutable flag [%s]\n", full_fpath);
	return 0;
}

/*
 * Steps to unlock an inode:
 *
 * 1) If immutable flag is not set, the inode should not be locked or already
 *    been unlocked.
 * 2) Read the xattr.
 * 3) If the xattr matches the key, chattr -i
 */
int lond_inode_unlock(const char *fpath, const char *key,
		      bool ignore_used_by_other)
{
	int rc;
	char cmd[PATH_MAX];
	int cmdsz = sizeof(cmd);
	bool immutable = false;
	char xattr_key[LOND_KEY_LENGH + 1];
	char full_fpath[PATH_MAX];

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}
	LDEBUG("unlocking inode [%s]\n", full_fpath);

	rc = check_inode_is_immutable(fpath, &immutable);
	if (rc) {
		LERROR("failed to check whether file [%s] is immutable\n",
		       full_fpath);
		return rc;
	}

	if (!immutable) {
		LDEBUG("inode [%s] is not immutable, skipping unlocking\n",
		       full_fpath);
		return 0;
	}

	rc = getxattr(fpath, XATTR_NAME_LOND_KEY, xattr_key,
		      sizeof(xattr_key));
	if (rc < 0) {
		LERROR("failed to get LOND xattr of [%s]: %s\n",
		       full_fpath, strerror(errno));
		return rc;
	}
	xattr_key[LOND_KEY_LENGH] = '\0';

	if ((strcmp(key, LOND_KEY_ANY) != 0) &&
	    (strcmp(xattr_key, key) != 0)) {
		if (ignore_used_by_other) {
			LDEBUG("inode [%s] is being locked with key [%s] not [%s]\n",
			       full_fpath, xattr_key, key);
			return 0;
		} else {
			LERROR("inode [%s] is being locked with key [%s] not [%s]\n",
			       full_fpath, xattr_key, key);
			return -1;
		}
	}

	snprintf(cmd, cmdsz, "chattr -i '%s'", fpath);
	rc = command_run(cmd, cmdsz);
	if (rc) {
		LERROR("failed to clear immutable flag of [%s], rc = %d\n",
		       full_fpath, rc);
		return rc;
	}
	LDEBUG("cleared immutable flag of inode [%s]\n", full_fpath);
	return 0;
}

/* The function of nftw() to unlock file */
static int nftw_unlock_fn(const char *fpath, const struct stat *sb,
			  int tflag, struct FTW *ftwbuf)
{
	int rc;

	/* Only set regular files and directories to immutable */
	if (!S_ISREG(sb->st_mode) && !S_ISDIR(sb->st_mode))
		return 0;

	/* Lock the inode first before copying to dest */
	rc = lond_inode_unlock(fpath, nftw_key, true);
	if (rc) {
		nftw_errno = rc;
		if (!nftw_ignore_error) {
			LERROR("failed to unlock file [%s], aborting\n", fpath);
			return rc;
		} else {
			LERROR("failed to unlock file [%s], continue unlocking\n",
			       fpath);
			return 0;
		}
	}
	return 0;
}

/* Return a full path of a possibly relative path */
int get_full_fpath(const char *fpath, char *full_fpath, size_t buf_size)
{
	int rc;
	char *cwd;
	char cwd_buf[PATH_MAX];
	int cwdsz = sizeof(cwd_buf);
	int fpath_length = strlen(fpath);

	if (fpath_length <= 0) {
		LERROR("unexpected path length [%d]\n", strlen(fpath));
		return -EINVAL;
	}

	if (fpath[0] == '/') {
		if (buf_size < fpath_length + 1) {
			LERROR("not enough buffer to generate full path of [%s]\n",
			       fpath);
			return -EINVAL;
		}
		strncpy(full_fpath, fpath, buf_size);
		return 0;
	}

	cwd = getcwd(cwd_buf, cwdsz);
	if (cwd == NULL) {
		LERROR("failed to get cwd: %s\n", strerror(errno));
		return -errno;
	}

	rc = snprintf(full_fpath, buf_size, "%s/%s", cwd, fpath);
	if (rc >= buf_size) {
		LERROR("not enough buffer to generate full path of [%s/%s]\n",
		       cwd_buf, fpath);
		return -EINVAL;
	}

	return 0;
}

int lond_tree_unlock(const char *fpath, const char *key,
		     bool ignore_error)
{
	int rc;
	int flags = FTW_PHYS;
	char full_fpath[PATH_MAX];

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}

	/*
	 * There is no way to transfer the argument into nftw_unlock_fn,
	 * thus use global variable to do that. This means, this is not
	 * thread-safe.
	 */
	nftw_key = key;
	nftw_ignore_error = ignore_error;
	nftw_errno = 0;
	rc = nftw(fpath, nftw_unlock_fn, 32, flags);
	if (rc) {
		LERROR("failed to unlock directory tree [%s] with key [%s]\n",
		       full_fpath, key);
		return rc;
	}

	if (nftw_errno) {
		LERROR("got error when unlocking directory tree [%s] with key [%s]\n",
		       full_fpath, key);
		rc = nftw_errno;
	} else {
		LINFO("unlocked directory tree [%s] with key [%s]\n",
		      full_fpath, key);
	}
	return rc;
}

static const char alphabet[] =
"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/* Return a random int smaller than n */
static int random_int(int n)
{
	return rand() % n;
}

/* Fill the string with random */
static void randomize_string(char *string, int buf_size)
{
	int i;

	LASSERT(buf_size > 1);
	for (i = 0; i < buf_size - 1; i++)
		string[i] = alphabet[random_int(strlen(alphabet))];
	string[buf_size - 1] = '\0';
}

int generate_key(char *key, int buf_size)
{
	if (buf_size <= LOND_KEY_LENGH) {
		LERROR("failed to generated lock key because of short buffer, expected [%d], got [%d]\n",
		       LOND_KEY_LENGH + 1, buf_size);
		return -1;
	}
	randomize_string(key, buf_size);
	return 0;
}

bool is_valid_key(const char *key)
{
	int i;

	if (strcmp(key, LOND_KEY_ANY) == 0)
		return true;

	if (strlen(key) != LOND_KEY_LENGH) {
		LERROR("invalid lock key length, expected [%d], got [%d]\n",
		       LOND_KEY_LENGH, strlen(key));
		return false;
	}

	for (i = 0; i < LOND_KEY_LENGH; i++) {
		if (key[i] >= 'a' && key[i] <= 'z')
			continue;
		if (key[i] >= 'A' && key[i] <= 'Z')
			continue;
		if (key[i] >= '0' && key[i] <= '9')
			continue;
		LERROR("invalid lock char [%c] in key [%s]\n",
		       key[i], key);
		return false;
	}

	return true;
}
