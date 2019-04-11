/*
 * Copyright (c) 2019, DDN Storage Corporation.
 */
/*
 *
 * Head file for Lustre on Demand
 *
 * Author: Li Xi <lixi@ddn.com>
 */
#ifndef _LOND_H_
#define _LOND_H_

#define XATTR_NAME_LOND_KEY	"trusted.lond_key"
#define XATTR_NAME_LOND_HSM_FID "trusted.lond_hsm_fid"
#define LOND_KEY_LENGH 10
#define LOND_KEY_ANY "any"

int lond_inode_lock(const char *fpath, const char *key);
int lond_tree_unlock(const char *fpath, const char *key,
		     bool ignore_error);
int lond_inode_unlock(const char *fpath, const char *key,
		      bool ignore_used_by_other);

int generate_key(char *key, int buf_size);
bool is_valid_key(const char *key);
int get_full_fpath(const char *fpath, char *full_fpath, size_t buf_size);
#endif /* _LOND_H_ */
