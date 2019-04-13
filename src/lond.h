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

#include <linux/limits.h>
#include <uthash.h>
#include <linux/types.h>
#ifdef NEW_USER_HEADER
#include <linux/lustre/lustre_user.h>
#else
#include <lustre/lustre_user.h>
#endif
#include "list.h"

#define XATTR_NAME_LOND_GLOBAL	"trusted.lond_global"
#define XATTR_NAME_LOND_HSM_FID "trusted.lond_hsm_fid"
#define LOND_KEY_LENGH 10
#define LOND_KEY_ANY "any"

#define LOND_KEY_BITS 128
/* Key will be saved as char array */
#define LOND_KEY_ARRAY_LENGH (LOND_KEY_BITS / 8)
/* String will be printed in the format of hex */
#define LOND_KEY_STRING_SIZE (LOND_KEY_ARRAY_LENGH * 2 + 1)

struct lond_key {
	unsigned char lk_key[LOND_KEY_ARRAY_LENGH];
};

#define LOND_MAGIC	0x10ED10ED
#define LOND_VERSION    1

struct lond_global_xattr_disk {
	/* Magic should be euqal to LOND_MAGIC */
	__u32           lgxd_magic;
	/* version should be equal to LOND_VERSION */
	__u32           lgxd_version;
	/* The lock key */
	struct lond_key lgxd_key;
	/* Whether this global inode is the root of fetched tree */
	__u64		lgxd_is_root:1;
};

struct lond_global_xattr {
	/* xattr on disk */
	struct lond_global_xattr_disk lgx_disk;
	/* To print the key */
	char lgx_key_str[LOND_KEY_STRING_SIZE];
	/* Whether the key is valid */
	bool lgx_is_valid;
	/* Why the key is not valid */
	char lgx_invalid_reason[4096];
};

struct lond_local_xattr {
	__u32           llx_magic;
	__u32		llx_version;
	/* The lock key */
	struct lond_key llx_key;
	/* Whether this global inode is the root of fetched tree */
	struct lu_fid	llx_global_fid;
};

struct nftw_private_unlock {
	/* The key to used to unlock the global Lustre */
	struct lond_key	*npu_key;
	bool		 npu_any_key;
};

/*
 * Use ST_DEV and ST_INO as the key, FILENAME as the value.
 * These are used to associate the destination name with the source
 * device/inode pair so that if we encounter a matching dev/ino
 * pair in the source tree we can arrange to create a hard link between
 * the corresponding names in the destination tree.
 */
struct dest_entry {
	/*
	 * 2^64 = 18446744073709551616
	 *        12345678901234567890
	 */
	char	 de_key[10 * 2 + 2];
	ino_t	 de_ino;
	dev_t	 de_dev;
	/*
	 * Destination file name corresponding to the dev/ino of a copied file
	 */
	char	*de_fpath;
	/*
	 * Makes this structure hashable. Donot change this function name from
	 * @hh to something else since HASH_xxx macros reply on this name.
	 */
	UT_hash_handle hh;
};

struct nftw_private_fetch {
	/* The key to used to lock the global Lustre */
	struct lond_key *npf_key;
	/* The HSM archive ID */
	__u32 npf_archive_id;
	/* The dest directory to copy to */
	char npf_dest[PATH_MAX + 1];
	/*
	 * The source directory under the dest directory. This is genrated
	 * after copying started with source directory locked. So there is no
	 * need to init it before calling nftw.
	 */
	char npf_dest_source_dir[PATH_MAX + 1];
	/* Hash table to check whether the inode is already created before */
	struct dest_entry *npf_dest_entry_table;
};

struct nftw_private {
	/* Whether to ignore error during the iteration */
	bool	np_ignore_error;
	/* Error number during the iteration */
	bool	np_errno;
	union {
		/* Used by unlock */
		struct nftw_private_unlock	np_unlock;
		/* Used by fetch */
		struct nftw_private_fetch	np_fetch;
	} u;
};

int lond_inode_lock(const char *fpath, struct lond_key *key, bool is_root);
int lond_inode_unlock(const char *fpath, bool any_key, struct lond_key *key,
		      bool ignore_used_by_other);
int lond_inode_stat(const char *fpath, struct lond_list_head *stack_list,
		    mode_t mode);
int lond_tree_unlock(const char *fpath, bool any_key, struct lond_key *key,
		     bool ignore_error);
int lond_tree_stat(const char *fpath, bool ignore_error);
void lond_key_generate(struct lond_key *key);
int lond_key_get_string(struct lond_key *key, char *buffer,
			size_t buffer_size);
int get_full_fpath(const char *fpath, char *full_fpath, size_t buf_size);

extern struct nftw_private nftw_private;
#endif /* _LOND_H_ */
