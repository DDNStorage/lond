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
#define XATTR_NAME_LOND_LOCAL	"trusted.lond_local"
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

struct lond_global_xattr {
	/* Magic should be euqal to LOND_MAGIC */
	__u32           lgx_magic;
	/* version should be equal to LOND_VERSION */
	__u32           lgx_version;
	/* The lock key */
	struct lond_key lgx_key;
	/* Whether this global inode is the root of fetched tree */
	__u64		lgx_is_root:1;
};

struct lond_local_xattr {
	/* Magic should be euqal to LOND_MAGIC */
	__u32           llx_magic;
	/* version should be equal to LOND_VERSION */
	__u32           llx_version;
	/* The lock key */
	struct lond_key llx_key;
	/* The global fid */
	struct lu_fid	llx_global_fid;
	/* Whether this global inode is the root of fetched tree */
	__u64		llx_is_root:1;
};

struct lond_xattr {
	/* xattr on disk */
	union {
		struct lond_global_xattr lx_global;
		struct lond_local_xattr lx_local;
	} u;
	/* To print the key */
	char lx_key_str[LOND_KEY_STRING_SIZE];
	/* Whether the key is valid */
	bool lx_is_valid;
	/* Why the key is not valid */
	char lx_invalid_reason[4096];
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
	char npf_dest_source_dir[PATH_MAX + 2];
	/* Hash table to check whether the inode is already created before */
	struct dest_entry *npf_dest_entry_table;
};

struct nftw_private_sync {
	/* The dest directory to copy to */
	char	 nps_dest[PATH_MAX + 1];
	/*
	 * The source directory under the dest directory. This is genrated
	 * after copying started with source directory locked. So there is no
	 * need to init it before calling nftw.
	 */
	char	 nps_dest_source_dir[PATH_MAX + 2];
	/* Hash table to check whether the inode is already created before */
	struct	 dest_entry *nps_dest_entry_table;
	/* Mount point of dest */
	char	 nps_dest_mnt[PATH_MAX + 1];
	/* Mount point of source */
	char	 nps_source_mnt[PATH_MAX + 1];
	char	*nps_copy_buf;
	int	 nps_copy_buf_size;
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
		/* Used by sync */
		struct nftw_private_sync	np_sync;
	} u;
};

typedef int (*lond_copy_reg_file_fn)(char const *src_name,
				     char const *dst_name,
				     mode_t dst_mode,
				     mode_t omitted_permissions,
				     struct stat const *src_sb,
				     void *private);

int lond_inode_lock(const char *fpath, struct lond_key *key, bool is_root);
int lond_inode_unlock(const char *fpath, bool any_key, struct lond_key *key,
		      bool ignore_used_by_other);
int lond_inode_stat(const char *fpath, struct lond_list_head *stack_list,
		    mode_t mode);
int lond_tree_unlock(const char *fpath, bool any_key, struct lond_key *key,
		     bool ignore_error);
int lond_tree_stat(const char *fpath, bool ignore_error);
void lond_key_generate(struct lond_key *key);
bool lond_key_equal(struct lond_key *key1, struct lond_key *key2);
int lond_key_get_string(struct lond_key *key, char *buffer,
			size_t buffer_size);
int get_full_fpath(const char *fpath, char *full_fpath, size_t buf_size);
int lustre_directory2fsname(const char *fpath, char *fsname);
int check_lustre_root(const char *fsname, const char *fpath);
int lond_read_global_xattr(const char *fpath, struct lond_xattr *lond_xattr);
int lond_read_local_xattr(const char *fpath, struct lond_xattr *lond_xattr);
int lond_copy_inode(struct dest_entry **head, const char *src_name,
		    const char *dst_name, lond_copy_reg_file_fn reg_fn,
		    void *private);
void free_dest_table(struct dest_entry **head);
void remove_slash_tail(char *path);
int check_inode_is_immutable(const char *fpath, bool *immutable);
int lustre_fid_path(char *buf, int sz, const char *mnt,
		    const struct lu_fid *fid);

extern struct nftw_private nftw_private;
#endif /* _LOND_H_ */
