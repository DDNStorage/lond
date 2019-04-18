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
#include <lustre/lustreapi.h>
#include "definition.h"
#include "debug.h"
#include "cmd.h"
#include "lond.h"
#include "list.h"

struct nftw_private nftw_private;

static int check_inode_is_immutable(const char *fpath, bool *immutable)
{
	int i;
	int rc;
	char cmd[PATH_MAX + 1];
	int cmdsz = sizeof(cmd);
	char output[PATH_MAX + 1];
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

static void parse_global_xattr(struct lond_xattr *lond_xattr)
{
	int rc;
	struct lond_global_xattr *disk = &lond_xattr->u.lx_global;
	char *key_str = lond_xattr->lx_key_str;

	if (disk->lgx_magic != LOND_MAGIC) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "invalid magic [0x%x], expected [0x%x]",
			 disk->lgx_magic, LOND_MAGIC);
		return;
	}

	if (disk->lgx_version != LOND_VERSION) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "invalid version [%d], expected [%d]",
			 disk->lgx_version, LOND_VERSION);
		return;
	}

	rc = lond_key_get_string(&disk->lgx_key, key_str,
				 sizeof(lond_xattr->lx_key_str));
	if (rc) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "short buffer");
		LERROR("failed to get the string of key\n");
		return;
	}
	lond_xattr->lx_is_valid = true;
}

/* Return negative value if failed to read */
static int lond_read_global_xattr(const char *fpath,
				  struct lond_xattr *lond_xattr)
{
	int rc;
	struct lond_global_xattr *disk = &lond_xattr->u.lx_global;

	memset(lond_xattr, 0, sizeof(*lond_xattr));
	rc = getxattr(fpath, XATTR_NAME_LOND_GLOBAL, disk, sizeof(*disk));
	if (rc == sizeof(*disk)) {
		parse_global_xattr(lond_xattr);
		return 0;
	} else if (rc < 0 && errno == ENOATTR) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "no xattr of %s", XATTR_NAME_LOND_GLOBAL);
		return 0;
	} else if (rc < 0) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "errno %d when reading xattr %s",
			 errno, XATTR_NAME_LOND_GLOBAL);
		return rc;
	} else {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "short read of xattr %s",
			 XATTR_NAME_LOND_GLOBAL);
		return 0;
	}
	return 0;
}

static void parse_local_xattr(struct lond_xattr *lond_xattr)
{
	int rc;
	struct lond_local_xattr *disk = &lond_xattr->u.lx_local;
	char *key_str = lond_xattr->lx_key_str;

	if (disk->llx_magic != LOND_MAGIC) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "invalid magic [0x%x], expected [0x%x]",
			 disk->llx_magic, LOND_MAGIC);
		return;
	}

	if (disk->llx_version != LOND_VERSION) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "invalid version [%d], expected [%d]",
			 disk->llx_version, LOND_VERSION);
		return;
	}

	rc = lond_key_get_string(&disk->llx_key, key_str,
				 sizeof(lond_xattr->lx_key_str));
	if (rc) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "short buffer");
		LERROR("failed to get the string of key\n");
		return;
	}
	lond_xattr->lx_is_valid = true;
}

/* Return negative value if failed to read */
int lond_read_local_xattr(const char *fpath, struct lond_xattr *lond_xattr)
{
	int rc;
	struct lond_local_xattr *disk = &lond_xattr->u.lx_local;

	memset(lond_xattr, 0, sizeof(*lond_xattr));
	rc = getxattr(fpath, XATTR_NAME_LOND_LOCAL, disk, sizeof(*disk));
	if (rc == sizeof(*disk)) {
		parse_local_xattr(lond_xattr);
		return 0;
	} else if (rc < 0 && errno == ENOATTR) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "no xattr of %s", XATTR_NAME_LOND_LOCAL);
		return 0;
	} else if (rc < 0) {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "errno %d when reading xattr %s",
			 errno, XATTR_NAME_LOND_LOCAL);
		return rc;
	} else {
		snprintf(lond_xattr->lx_invalid_reason,
			 sizeof(lond_xattr->lx_invalid_reason),
			 "short read of xattr %s",
			 XATTR_NAME_LOND_LOCAL);
		return 0;
	}
	return 0;
}

/*
 * Print the EPERM reason of an inode,
 * If the inode is locked by myself, return 0. Otherwise, negative value.
 */
static int lond_lock_eperm_reason(const char *fpath, const char *key_str)
{
	int rc;
	bool immutable = false;
	struct lond_xattr lond_xattr;
	char full_fpath[PATH_MAX + 1];

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX + 1);
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

	rc = lond_read_global_xattr(fpath, &lond_xattr);
	if (rc) {
		LERROR("failed to get lond key of immutable inode [%s]: %s\n",
		       full_fpath, strerror(errno));
		return rc;
	}

	if (!lond_xattr.lx_is_valid) {
		LERROR("immutable inode [%s] doesn't have valid lond key: %s\n",
		       full_fpath, lond_xattr.lx_invalid_reason);
		LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
		       LOND_KEY_ANY, full_fpath, full_fpath);
		return -ENOATTR;
	} else {
		LERROR("inode [%s] has already been locked with key [%s]\n",
		       full_fpath, lond_xattr.lx_key_str);
		LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
		       key_str, full_fpath);
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
int lond_inode_lock(const char *fpath, struct lond_key *key, bool is_root)
{
	int rc;
	int rc2;
	char cmd[PATH_MAX + 1];
	int cmdsz = sizeof(cmd);
	struct lond_xattr set_xattr;
	struct lond_xattr get_xattr;
	char full_fpath[PATH_MAX + 1];
	char *key_str = set_xattr.lx_key_str;
	struct lond_global_xattr *disk = &set_xattr.u.lx_global;

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX + 1);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}

	set_xattr.lx_is_valid = true;
	rc = lond_key_get_string(key, key_str, sizeof(set_xattr.lx_key_str));
	if (rc) {
		LERROR("failed to get string of key\n");
		return rc;
	}
	memcpy(&disk->lgx_key, key, sizeof(*key));
	disk->lgx_is_root = is_root;
	disk->lgx_magic = LOND_MAGIC;
	disk->lgx_version = LOND_VERSION;

	rc = lsetxattr(fpath, XATTR_NAME_LOND_GLOBAL, disk, sizeof(*disk),
		       0);
	if (rc) {
		rc2 = -errno;
		if (errno == EPERM) {
			rc = lond_lock_eperm_reason(fpath, key_str);
			if (rc == 0)
				return 0;
		} else {
			LERROR("failed to set lock key of [%s] to [%s]: %s\n",
			       full_fpath, key_str, strerror(errno));
		}
		return rc2;
	}

	snprintf(cmd, cmdsz, "chattr +i '%s'", fpath);
	rc = command_run(cmd, cmdsz);
	if (rc) {
		LERROR("failed to set immutable flag of [%s], rc = %d\n",
		       full_fpath, rc);
		return rc;
	}

	rc = lond_read_global_xattr(fpath, &get_xattr);
	if (rc) {
		LERROR("failed to get lond key of immutable inode [%s]: %s\n",
		       full_fpath, strerror(errno));
		return rc;
	}

	if (!get_xattr.lx_is_valid) {
		LERROR("race of inode [%s] when locking with key [%s], got invalid key: %s\n",
		       full_fpath, key_str, get_xattr.lx_invalid_reason);
		LERROR("is it being used by other tools?\n");
		LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
		       LOND_KEY_ANY, full_fpath, full_fpath);
		return -ENOATTR;
	} else if (memcmp(&get_xattr.u.lx_global.lgx_key, key,
			  sizeof(struct lond_key)) != 0) {
		LERROR("race of inode [%s] from another lock, expected key [%s], got key [%s]\n",
		       full_fpath, key_str, get_xattr.lx_key_str);
		LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
		       key_str, full_fpath);
		return -EBUSY;
	}

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
int lond_inode_unlock(const char *fpath, bool any_key, struct lond_key *key,
		      bool ignore_used_by_other)
{
	int rc;
	char cmd[PATH_MAX + 1];
	int cmdsz = sizeof(cmd);
	bool immutable = false;
	struct lond_xattr global_xattr;
	char full_fpath[PATH_MAX + 1];
	char key_str[LOND_KEY_STRING_SIZE];

	if (any_key) {
		snprintf(key_str, sizeof(key_str), LOND_KEY_ANY);
	} else {
		rc = lond_key_get_string(key, key_str, sizeof(key_str));
		if (rc) {
			LERROR("failed to get string of key\n");
			return rc;
		}
	}

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX + 1);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}
	LDEBUG("unlocking inode [%s] with key [%s]\n", full_fpath, key_str);

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

	if (!any_key) {
		rc = lond_read_global_xattr(fpath, &global_xattr);
		if (rc) {
			LERROR("failed to get lond key of immutable inode [%s]: %s\n",
			       full_fpath, strerror(errno));
			return rc;
		}

		if (!global_xattr.lx_is_valid) {
			LERROR("immutable inode [%s] doesn't have valid lond key: %s\n",
			       full_fpath, global_xattr.lx_invalid_reason);
			LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
			       LOND_KEY_ANY, full_fpath, full_fpath);
			return -ENOATTR;
		} else if (memcmp(&global_xattr.u.lx_global.lgx_key, key,
				  sizeof(struct lond_key)) != 0) {
			if (ignore_used_by_other) {
				LDEBUG("inode [%s] is being locked with key [%s] not [%s]\n",
				      full_fpath, global_xattr.lx_key_str,
				      key_str);
				return 0;
			} else {
				LERROR("inode [%s] is being locked with key [%s] not [%s]\n",
				       full_fpath, global_xattr.lx_key_str,
				       key_str);
				LERROR("to cleanup, try [lond unlock -d -k %s %s]\n",
				       key_str, full_fpath);
				return -EBUSY;
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
	struct nftw_private_unlock *unlock = &nftw_private.u.np_unlock;
	bool any_key = unlock->npu_any_key;
	struct lond_key *key = unlock->npu_key;
	char full_fpath[PATH_MAX + 1];

	/* Only set regular files and directories to immutable */
	if (!S_ISREG(sb->st_mode) && !S_ISDIR(sb->st_mode))
		return 0;

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX + 1);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}

	/* Lock the inode first before copying to dest */
	rc = lond_inode_unlock(fpath, any_key, key, true);
	if (rc) {
		nftw_private.np_errno = rc;
		if (!nftw_private.np_ignore_error) {
			LERROR("failed to unlock file [%s], aborting\n",
			       full_fpath);
			return rc;
		} else {
			LERROR("failed to unlock file [%s], continue unlocking\n",
			       full_fpath);
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
	char cwd_buf[PATH_MAX + 1];
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

int lond_tree_unlock(const char *fpath, bool any_key, struct lond_key *key,
		     bool ignore_error)
{
	int rc;
	int flags = FTW_PHYS;
	char full_fpath[PATH_MAX + 1];
	char key_str[LOND_KEY_STRING_SIZE];

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX + 1);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}

	if (!any_key) {
		rc = lond_key_get_string(key, key_str, sizeof(key_str));
		if (rc) {
			LERROR("failed to get string of key\n");
			return rc;
		}
	} else {
		snprintf(key_str, sizeof(key_str), LOND_KEY_ANY);
	}

	/*
	 * There is no way to transfer the argument into nftw_unlock_fn,
	 * thus use global variable to do that. This means, this is not
	 * thread-safe.
	 */
	nftw_private.u.np_unlock.npu_any_key = any_key;
	nftw_private.u.np_unlock.npu_key = key;
	nftw_private.np_ignore_error = ignore_error;
	nftw_private.np_errno = 0;
	rc = nftw(fpath, nftw_unlock_fn, 32, flags);
	if (rc) {
		LERROR("failed to unlock directory tree [%s] with key [%s]\n",
		       full_fpath, key_str);
		return rc;
	}

	if (nftw_private.np_errno) {
		LERROR("got error when unlocking directory tree [%s] with key [%s]\n",
		       full_fpath, key_str);
		rc = nftw_private.np_errno;
	} else {
		LINFO("unlocked directory tree [%s] with key [%s]\n",
		      full_fpath, key_str);
	}
	return rc;
}

void lond_key_generate(struct lond_key *key)
{
	int i;
	unsigned char value;

	for (i = 0; i < LOND_KEY_ARRAY_LENGH; i++) {
		value = rand() % 256;
		key->lk_key[i] = value;
	}
}

int lond_key_get_string(struct lond_key *key, char *buffer, size_t buffer_size)
{
	char *ptr;
	int i;

	if (buffer_size < LOND_KEY_STRING_SIZE) {
		LERROR("buffer size [%d] is too short, expected [%d]\n",
		       buffer_size, LOND_KEY_STRING_SIZE);
		return -EINVAL;
	}

	ptr = buffer;
	for (i = 0; i < LOND_KEY_ARRAY_LENGH; i++) {
		sprintf(ptr, "%02x", key->lk_key[i]);
		ptr += 2;
	}
	return 0;
}

LOND_LIST_HEAD(nftw_stat_stack);

struct lond_stat_entry {
	/* Linked into stack */
	struct lond_list_head			lse_linkage;
	/* The inode full path */
	char					lse_path[PATH_MAX + 1];
	/* Whether this inode is immutable */
	bool					lse_immutable;
	/* Global xattr of this entry */
	struct lond_xattr			lse_global_xattr;
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
			     bool immutable,
			     struct lond_xattr *global_xattr,
			     struct lond_stat_entry *parent)
{
	const char *type;
	struct lond_xattr *parent_xattr = &parent->lse_global_xattr;

	if (S_ISDIR(mode))
		type = "directory";
	else
		type = "file";

	if (!immutable) {
		if (parent && parent->lse_immutable) {
			if (!parent_xattr->lx_is_valid)
				LERROR("%s [%s] is not locked by lond, but its parent is locked with invalid key (%s)\n",
				       type, full_fpath,
				       parent_xattr->lx_invalid_reason);
			else
				LERROR("%s [%s] is not locked by lond, but its parent is locked with key [%s]\n",
				       type, full_fpath,
				       parent_xattr->lx_key_str);
		} else {
			LINFO("%s [%s] is not locked by lond\n", type,
			      full_fpath);
		}
		return;
	}

	if (global_xattr->lx_is_valid) {
		if ((!parent) || (!parent->lse_immutable))
			LINFO("%s [%s] is locked with key [%s]\n",
			      type, full_fpath, global_xattr->lx_key_str);
		else if (memcmp(&global_xattr->u.lx_global.lgx_key,
				&parent->lse_global_xattr.u.lx_global.lgx_key,
				sizeof(struct lond_key)) != 0)
			LERROR("%s [%s] is locked with key [%s], but its parent is locked with key [%s]\n",
			       type, full_fpath, global_xattr->lx_key_str,
			       parent->lse_global_xattr.lx_key_str);
	} else {
		LERROR("%s [%s] is locked with invalid key (%s), please run [lond unlock -d -k %s %s] to cleanup\n",
		       type, full_fpath, global_xattr->lx_invalid_reason,
		       LOND_KEY_ANY, full_fpath);
	}
}

/* Update the stat stack during the scanning process */
static int stat_stack_update(struct lond_list_head *stack_list,
			     const char *fpath, const char *full_fpath,
			     mode_t mode, bool immutable,
			     struct lond_xattr *lond_xattr)
{
	struct lond_stat_entry *entry;
	struct lond_stat_entry *top;
	struct lond_stat_entry *parent = NULL;
	struct lond_xattr *parent_xattr;
	struct lond_key *parent_key;
	struct lond_key *key;
	char parent_path[PATH_MAX + 1];
	bool need_print;

	top = stat_stack_top(stack_list);
	/* This is the root directory to scan, just print its status */
	if (top == NULL)
		print_inode_stat(full_fpath, mode, immutable, lond_xattr,
				 NULL);

	entry = calloc(sizeof(*entry), 1);
	if (entry == NULL) {
		LERROR("failed to allocate memory\n");
		return -ENOMEM;
	}

	strncpy(entry->lse_path, fpath, sizeof(entry->lse_path));
	entry->lse_immutable = immutable;
	if (immutable)
		memcpy(&entry->lse_global_xattr, lond_xattr,
		       sizeof(*lond_xattr));

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

	parent_xattr = &parent->lse_global_xattr;
	parent_key = &lond_xattr->u.lx_global.lgx_key;
	key = &lond_xattr->u.lx_global.lgx_key;

	need_print = false;
	if (!entry->lse_immutable) {
		if (parent->lse_immutable)
			need_print = true;
	} else if (!parent->lse_immutable) {
		need_print = true;
	} else if (!lond_xattr->lx_is_valid) {
		if (parent_xattr->lx_is_valid)
			need_print = true;
	} else if (!parent_xattr->lx_is_valid) {
		need_print = true;
	} else if (memcmp(key, parent_key, sizeof(struct lond_key)) != 0) {
		need_print = true;
	}

	if (need_print)
		print_inode_stat(full_fpath, mode, immutable, lond_xattr,
				 parent);

	stat_stack_push(stack_list, entry);
	return 0;
}

int lond_inode_stat(const char *fpath, struct lond_list_head *stack_list,
		    mode_t mode)
{
	int rc;
	bool immutable = false;
	struct lond_xattr global_xattr;
	char full_fpath[PATH_MAX + 1];

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX + 1);
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

	if (immutable) {
		rc = lond_read_global_xattr(fpath, &global_xattr);
		if (rc) {
			LERROR("failed to get lond key of immutable inode [%s]: %s\n",
			       full_fpath, strerror(errno));
			return rc;
		}
	}

	if (stack_list) {
		rc = stat_stack_update(stack_list, fpath, full_fpath,
				       mode, immutable, &global_xattr);
		if (rc) {
			LERROR("failed to update the stat stack\n");
			return rc;
		}
	} else {
		print_inode_stat(full_fpath, mode, immutable, &global_xattr,
				 NULL);
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
		nftw_private.np_errno = rc;
		if (!nftw_private.np_ignore_error) {
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
	char full_fpath[PATH_MAX + 1];

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX + 1);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}

	/*
	 * There is no way to transfer the argument into nftw_x_fn,
	 * thus use global variable to do that. This means, this is not
	 * thread-safe.
	 */
	nftw_private.np_ignore_error = ignore_error;
	nftw_private.np_errno = 0;
	LOND_INIT_LIST_HEAD(&nftw_stat_stack);
	rc = nftw(fpath, nftw_stat_fn, 32, flags);
	stat_stack_free(&nftw_stat_stack);
	if (rc) {
		LERROR("failed to stat directory tree [%s]\n",
		       full_fpath);
		return rc;
	}

	if (nftw_private.np_errno)
		rc = nftw_private.np_errno;
	return rc;
}

int lustre_directory2fsname(const char *fpath, char *fsname)
{
	int rc;
	struct stat file_sb;

	rc = lstat(fpath, &file_sb);
	if (rc) {
		LERROR("failed to stat [%s] failed: %s\n",
		       fpath, strerror(errno));
		return -errno;
	}

	if (!S_ISDIR(file_sb.st_mode)) {
		LERROR("[%s] is not a directory\n",
		       fpath);
		return -EINVAL;
	}

	rc = llapi_search_fsname(fpath, fsname);
	if (rc == -ENODEV) {
		LERROR("[%s] is not a Lustre directory\n", fpath);
		return rc;
	} else if (rc) {
		LERROR("failed to find the Lustre fsname of directory [%s]: %s\n",
		       fpath, strerror(-rc));
		return rc;
	}
	return 0;
}

#define FID_SEQ_ROOT 0x200000007ULL  /* Located on MDT0 */
#define FID_OID_ROOT 1UL

static inline bool fid_is_root(const struct lu_fid *fid)
{
	return fid->f_seq == FID_SEQ_ROOT && fid->f_oid == FID_OID_ROOT;
}

/**
 * Check whether file is the root of a Lustre file system
 * \retval	0  is the root
 * \retval	1  not the root
 * \retval	<0 error code
 */
int check_lustre_root(const char *fsname, const char *fpath)
{
	int rc;
	struct lu_fid fid;

	rc = llapi_path2fid(fpath, &fid);
	if (rc) {
		LERROR("failed to get the fid of [%s]: %s\n",
		       fpath, strerror(-rc));
		return rc;
	}

	if (fid_is_root(&fid))
		return 0;
	return 1;
}
