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
#include "list.h"

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

/* Return negative value if failed to read */
static int lond_read_key(const char *fpath, char *xattr_key, size_t buf_size,
			 bool *has_xattr)
{
	int rc;

	*has_xattr = false;
	rc = getxattr(fpath, XATTR_NAME_LOND_KEY, xattr_key, buf_size);
	if (rc >= 0) {
		xattr_key[LOND_KEY_LENGH] = '\0';
		*has_xattr = true;
	} else if (errno != ENOATTR) {
		return -errno;
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
	bool has_xattr = false;

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

	rc = lond_read_key(fpath, xattr_key, LOND_KEY_LENGH + 1, &has_xattr);
	if (rc) {
		LERROR("failed to get lond key of immutable inode [%s]: %s\n",
		       full_fpath, strerror(errno));
		return rc;
	}

	if (!has_xattr) {
		LERROR("immutable inode [%s] doesn't have any lond key\n",
		       full_fpath);
		LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
		       LOND_KEY_ANY, full_fpath, full_fpath);
		return -ENOATTR;
	}

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
	bool has_xattr = false;

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

	rc = lond_read_key(fpath, xattr_key, LOND_KEY_LENGH + 1, &has_xattr);
	if (rc) {
		LERROR("failed to get lond key of immutable inode [%s]: %s\n",
		       full_fpath, strerror(errno));
		return rc;
	}

	if (!has_xattr) {
		LERROR("race of inode [%s], expected key [%s], got no key\n",
		       full_fpath, key, xattr_key);
		LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
		       LOND_KEY_ANY, full_fpath, full_fpath);
		return -ENOATTR;
	}

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
	bool has_xattr = false;

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

	if (strcmp(key, LOND_KEY_ANY) != 0) {
		rc = lond_read_key(fpath, xattr_key, LOND_KEY_LENGH + 1,
				   &has_xattr);
		if (rc) {
			LERROR("failed to get lond key of immutable inode [%s]: %s\n",
			       full_fpath, strerror(errno));
			return rc;
		}

		if (!has_xattr) {
			LERROR("immutable inode [%s] doesn't have any lond key\n",
			       full_fpath);
			LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
			       LOND_KEY_ANY, full_fpath, full_fpath);
			return -ENOATTR;
		}

		if (!is_valid_key(xattr_key)) {
			LERROR("inode [%s] is being locked with invalid key [%s]\n",
			       full_fpath, xattr_key);
			LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
			       LOND_KEY_ANY, full_fpath, full_fpath);
			return -1;
		}

		if (strcmp(xattr_key, key) != 0) {
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
		LDEBUG("invalid lock key length, expected [%d], got [%d]\n",
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
		LDEBUG("invalid lock char [%c] in key [%s]\n",
		       key[i], key);
		return false;
	}

	return true;
}

LOND_LIST_HEAD(nftw_stat_stack);

struct lond_stat_entry {
	/* Linked into stack */
	struct lond_list_head			lse_linkage;
	/* The inode full path */
	char					lse_path[PATH_MAX];
	/* Whether this inode is immutable */
	bool					lse_immutable;
	/* Whether this inode has xattr of lond key */
	bool					lse_has_xattr;
	/* Key of this entry */
	char					lse_key[LOND_KEY_LENGH + 1];
};

static void stat_stack_push(struct lond_list_head *stack_list,
			    struct lond_stat_entry *entry)
{
	lond_list_add(&entry->lse_linkage, stack_list);
}

static struct lond_stat_entry *stat_stack_pop(struct lond_list_head *stack_list)
{
	struct lond_stat_entry *top;

	if (lond_list_empty(stack_list)) {
		LERROR("stack is empty\n");
		return NULL;
	}
	top = lond_list_entry(stack_list->next, struct lond_stat_entry,
			      lse_linkage);
	lond_list_del(&top->lse_linkage);
	return top;
}

static struct lond_stat_entry *stat_stack_top(struct lond_list_head *stack_list)
{
	struct lond_stat_entry *top;

	if (lond_list_empty(stack_list))
		return NULL;

	top = lond_list_entry(stack_list->next, struct lond_stat_entry,
			      lse_linkage);
	return top;
}

static void stat_stack_free(struct lond_list_head *stack_list)
{
	struct lond_stat_entry *entry, *n;

	lond_list_for_each_entry_safe(entry, n, stack_list,
				      lse_linkage) {
		lond_list_del_init(&entry->lse_linkage);
		free(entry);
	}
}

static void print_inode_stat(const char *full_fpath, mode_t mode,
			     bool immutable, bool has_xattr,
			     const char *xattr_key,
			     struct lond_stat_entry *parent)
{
	const char *type;

	if (S_ISDIR(mode))
		type = "directory";
	else
		type = "file";

	if (!immutable) {
		if (parent && parent->lse_immutable) {
			if (!parent->lse_has_xattr)
				LERROR("%s [%s] is not locked by lond, but its parent is immutable with no lock key\n",
				       type, full_fpath);
			else if (!is_valid_key(parent->lse_key))
				LERROR("%s [%s] is not locked by lond, but its parent is locked with invalid key [%s]\n",
				       type, full_fpath, parent->lse_key);
			else
				LERROR("%s [%s] is not locked by lond, but its parent is locked with key [%s]\n",
				       type, full_fpath, parent->lse_key);
		} else {
			LINFO("%s [%s] is not locked by lond\n", type,
			      full_fpath);
		}
		return;
	}

	if (!has_xattr) {
		LERROR("%s [%s] has no lock key but is immutable, please run [lond unlock -d -k %s %s] to cleanup\n",
		       type, full_fpath, LOND_KEY_ANY, full_fpath);
		return;
	}

	if (is_valid_key(xattr_key)) {
		if ((!parent) || (!parent->lse_immutable))
			LINFO("%s [%s] is locked with key [%s]\n",
			      type, full_fpath, xattr_key);
		else if (strcmp(xattr_key, parent->lse_key) != 0)
			LERROR("%s [%s] is locked with key [%s], but its parent is locked with key [%s]\n",
			       type, full_fpath, xattr_key, parent->lse_key);
	} else {
		LERROR("%s [%s] is locked with invalid key [%s], please run [lond unlock -d -k %s %s] to cleanup\n",
		       type, full_fpath, xattr_key, LOND_KEY_ANY, full_fpath);
	}
}

/* Update the stat stack during the scanning process */
static int stat_stack_update(struct lond_list_head *stack_list,
			     const char *fpath, const char *full_fpath,
			     mode_t mode, bool immutable, bool has_xattr,
			     const char *xattr_key)
{
	struct lond_stat_entry *entry;
	struct lond_stat_entry *top;
	struct lond_stat_entry *parent = NULL;
	char parent_path[PATH_MAX + 1];

	top = stat_stack_top(stack_list);
	/* This is the root directory to scan, just print its status */
	if (top == NULL)
		print_inode_stat(full_fpath, mode, immutable, has_xattr,
				 xattr_key, NULL);

	entry = calloc(sizeof(*entry), 1);
	if (entry == NULL) {
		LERROR("failed to allocate memory\n");
		return -ENOMEM;
	}

	strncpy(entry->lse_path, fpath, sizeof(entry->lse_path));
	entry->lse_immutable = immutable;
	if (immutable) {
		entry->lse_has_xattr = has_xattr;
		strncpy(entry->lse_key, xattr_key, sizeof(entry->lse_key));
	}

	if (top == NULL) {
		stat_stack_push(stack_list, entry);
		return 0;
	}

	/* pop the stack until find the parent directory of this inode */
	while (!lond_list_empty(stack_list)) {
		top = stat_stack_top(stack_list);
		if (top == NULL)
			break;

		/* Need to append / to parent path to avoid mismatch */
		snprintf(parent_path, PATH_MAX + 1, "%s/", top->lse_path);
		if (strncmp(fpath, parent_path, strlen(parent_path)) == 0) {
			parent = top;
			break;
		}
		stat_stack_pop(stack_list);
	}

	if (parent == NULL) {
		LERROR("can not find parent directory of inode [%s] in the stack\n",
		       full_fpath);
		return -1;
	}

	if (entry->lse_immutable != parent->lse_immutable ||
	    entry->lse_has_xattr != parent->lse_has_xattr ||
	    (strcmp(entry->lse_key, parent->lse_key) != 0)) {
		print_inode_stat(full_fpath, mode, immutable, has_xattr,
				 xattr_key, parent);
	}

	stat_stack_push(stack_list, entry);
	return 0;
}

int lond_inode_stat(const char *fpath, struct lond_list_head *stack_list,
		    mode_t mode)
{
	int rc;
	bool immutable = false;
	char xattr_key[LOND_KEY_LENGH + 1];
	char full_fpath[PATH_MAX];
	bool has_xattr = false;

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}
	LDEBUG("stating inode [%s]\n", full_fpath);

	rc = check_inode_is_immutable(fpath, &immutable);
	if (rc) {
		LERROR("failed to check whether file [%s] is immutable\n",
		       full_fpath);
		return rc;
	}

	xattr_key[0] = '\0';
	if (immutable) {
		rc = lond_read_key(fpath, xattr_key, LOND_KEY_LENGH + 1,
				   &has_xattr);
		if (rc) {
			LERROR("failed to get lond key of immutable inode [%s]: %s\n",
			       full_fpath, strerror(errno));
			return rc;
		}
	}

	if (stack_list) {
		rc = stat_stack_update(stack_list, fpath, full_fpath,
				       mode, immutable, has_xattr, xattr_key);
		if (rc) {
			LERROR("failed to update the stat stack\n");
			return rc;
		}
	} else {
		print_inode_stat(full_fpath, mode, immutable, has_xattr,
				 xattr_key, NULL);
	}

	return rc;
}

/* The function of nftw() to stat file */
static int nftw_stat_fn(const char *fpath, const struct stat *sb,
			int tflag, struct FTW *ftwbuf)
{
	int rc;

	/* Only need to stat directory and regular file */
	if (!S_ISREG(sb->st_mode) && !S_ISDIR(sb->st_mode))
		return 0;

	rc = lond_inode_stat(fpath, &nftw_stat_stack, sb->st_mode);
	if (rc) {
		nftw_errno = rc;
		if (!nftw_ignore_error) {
			LERROR("failed to stat file [%s], aborting\n", fpath);
			return rc;
		} else {
			LERROR("failed to stat file [%s], continue\n",
			       fpath);
			return 0;
		}
	}
	return 0;
}

/*
 * Scanning process example:
 * /lustre/.
 * /lustre/./dir0
 * /lustre/./dir0/dir0_1
 * /lustre/./dir0/dir0_1/dir0_1_0
 * /lustre/./dir0/dir0_1/dir0_2_0
 * /lustre/./dir0/dir0_0
 * /lustre/./dir0/dir0_0/dir0_0_0
 * /lustre/./dir0/dir0_2
 * /lustre/./dir1
 * /lustre/./dir1/dir1_1
 * /lustre/./dir1/dir1_0
 */
int lond_tree_stat(const char *fpath, bool ignore_error)
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
	 * There is no way to transfer the argument into nftw_x_fn,
	 * thus use global variable to do that. This means, this is not
	 * thread-safe.
	 */
	nftw_ignore_error = ignore_error;
	nftw_errno = 0;
	LOND_INIT_LIST_HEAD(&nftw_stat_stack);
	rc = nftw(fpath, nftw_stat_fn, 32, flags);
	stat_stack_free(&nftw_stat_stack);
	if (rc) {
		LERROR("failed to stat directory tree [%s]\n",
		       full_fpath);
		return rc;
	}

	if (nftw_errno)
		rc = nftw_errno;
	return rc;
}
