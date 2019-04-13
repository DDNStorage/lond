/*
 * Copyright (c) 2019, DDN Storage Corporation.
 */
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

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s <source>... <dest>\n"
		"  source: global Lustre directory tree to fetch from\n"
		"  dest: local Lustre directory to fetch to\n",
		prog);
}

static inline void fid2str(char *buf, const struct lu_fid *fid, int len)
{
	snprintf(buf, len, DFID_NOBRACE, PFID(fid));
}

static int create_stub_reg(char const *src_name, char const *dst_name,
			   mode_t dst_mode, mode_t omitted_permissions,
			   struct stat const *src_sb)
{
	int rc;
	int dest_desc;
	int open_flags = O_WRONLY | O_CREAT;
	char cmd[PATH_MAX];
	int cmdsz = sizeof(cmd);
	struct lu_fid fid;
	char fid_str[FID_NOBRACE_LEN + 1];

	rc = llapi_path2fid(src_name, &fid);
	if (rc) {
		LERROR("failed to get fid of [%s]\n", src_name);
		return rc;
	}

	fid2str(fid_str, &fid, sizeof(fid_str));

	dest_desc = open(dst_name, open_flags | O_EXCL,
			 dst_mode & ~omitted_permissions);
	if (dest_desc < 0) {
		LERROR("failed to create regular file [%s]: %s\n",
		       dst_name, strerror(errno));
		return -1;
	}

	rc = fsetxattr(dest_desc, XATTR_NAME_LOND_HSM_FID, fid_str,
		       strlen(fid_str), 0);
	if (rc) {
		LERROR("failed to set xattr [%s] of regular file [%s]: %s\n",
		       XATTR_NAME_LOND_HSM_FID, dst_name, strerror(errno));
		goto out_close;
	}

	rc = llapi_hsm_state_set_fd(dest_desc, HS_EXISTS | HS_ARCHIVED, 0,
				    nftw_private.u.np_fetch.npf_archive_id);
	if (rc) {
		LERROR("failed to set the HSM state of file [%s]: %s\n",
		       dst_name, strerror(errno));
		goto out_close;
	}

	if (close(dest_desc) < 0) {
		LERROR("failed to close regular file [%s]: %s\n",
		       dst_name, strerror(errno));
		return -1;
	}

	snprintf(cmd, cmdsz, "lfs hsm_release '%s'", dst_name);
	rc = command_run(cmd, cmdsz);
	if (rc) {
		LERROR("failed to HSM release file [%s], rc = %d\n",
		       dst_name, rc);
		goto out_close;
	}

	return rc;
out_close:
	if (close(dest_desc) < 0) {
		LERROR("failed to close regular file [%s]: %s\n",
		       dst_name, strerror(errno));
		return -1;
	}
	return rc;
}

static int create_stub_symlink(char const *src_name, char const *dst_name,
			       size_t size)
{
	int rc;
	char *src_link_val;

	src_link_val = calloc(1, size);
	if (src_link_val == NULL) {
		LERROR("failed to allocate memory\n");
		return -ENOMEM;
	}

	rc = readlink(src_name, src_link_val, size);
	if (rc < 0) {
		LERROR("failed to readlink [%s]: %s\n", src_name,
		       strerror(errno));
		rc = -errno;
		goto out;
	}

	rc = symlink(src_link_val, dst_name);
	if (rc) {
		LERROR("failed to symlink [%s] to [%s]: %s\n",
		       src_link_val, src_name, strerror(errno));
		rc = -errno;
		goto out;
	}

out:
	free(src_link_val);
	return rc;
}

static int set_owner(char const *dst_name, struct stat const *src_sb)
{
	int rc;
	uid_t uid = src_sb->st_uid;
	gid_t gid = src_sb->st_gid;

	rc = lchown(dst_name, uid, gid);
	if (rc) {
		LERROR("failed to chown file [%s]: %s\n", dst_name,
		       strerror(errno));
		return -errno;
	}
	return 0;
}

static void generate_hash_key(struct dest_entry *ent)
{
	snprintf(ent->de_key, sizeof(ent->de_key), "%"PRIuMAX"/%"PRIuMAX,
		 ent->de_dev, ent->de_ino);
}

/*
 * Add file path, copied from inode number INO and device number DEV,
 * If entry alreay exists in hash table, set $ent_in_table to it.
 */
int remember_copied(struct dest_entry **head, const char *fpath, ino_t ino,
		    dev_t dev, struct dest_entry **entry_in_table)
{
	int rc = 0;
	struct dest_entry *entry;

	entry = calloc(sizeof(*entry), 1);
	if (!entry) {
		rc = -ENOMEM;
		return rc;
	}

	entry->de_ino = ino;
	entry->de_dev = dev;
	generate_hash_key(entry);

	HASH_FIND_STR(*head, entry->de_key, *entry_in_table);
	if (*entry_in_table != NULL) {
		LDEBUG("found [%s] alrady exists as [%s]\n", fpath,
		       (*entry_in_table)->de_fpath);
		goto out_free_entry;
	}

	entry->de_fpath = strdup(fpath);
	if (entry->de_fpath == NULL) {
		rc = -ENOMEM;
		goto out_free_entry;
	}
	HASH_ADD_STR(*head, de_key, entry);
	LDEBUG("remembered [%s]\n", entry->de_fpath);

	return 0;
out_free_entry:
	free(entry);
	return rc;
}

void free_dest_table(struct dest_entry **head)
{
	struct dest_entry *entry, *tmp;

	HASH_ITER(hh, *head, entry, tmp) {
		  HASH_DEL(*head, entry);
		  free(entry->de_fpath);
		  free(entry);
	}
	*head = NULL;
}

/* 07777 */
#define CHMOD_MODE_BITS (S_ISUID|S_ISGID|S_ISVTX|S_IRWXU|S_IRWXG|S_IRWXO)

static int create_stub_inode(struct dest_entry **head, const char *src_name,
			     const char *dst_name)
{
	int rc;
	struct stat src_sb;
	struct stat dst_sb;
	mode_t src_mode;
	mode_t dst_mode = 0;
	mode_t dst_mode_bits;
	mode_t omitted_permissions;
	bool restore_dst_mode = false;
	struct dest_entry *earlier_entry = NULL;

	LDEBUG("creating [%s]\n", dst_name);

	/*
	 * Do not rust the stat of nftw, do it myself after setting the file
	 * to immutable
	 */
	rc = lstat(src_name, &src_sb);
	if (rc) {
		LERROR("failed to stat [%s]: %s\n", src_name, strerror(errno));
		return rc;
	}

	src_mode = src_sb.st_mode;
	if (!S_ISDIR(src_mode) && src_sb.st_nlink > 1) {
		rc = remember_copied(head, dst_name,
				     src_sb.st_ino, src_sb.st_dev,
				     &earlier_entry);
		if (rc) {
			LERROR("failed to remember copied\n");
			return rc;
		}

		if (earlier_entry != NULL) {
			/* Already created the inode, create hard link to it */
			rc = link(earlier_entry->de_fpath, dst_name);
			if (rc) {
				LERROR("failed to create hard link from [%s] to [%s]: %s\n",
				       earlier_entry->de_fpath, dst_name,
				       strerror(errno));
			}
			return 0;
		}
	}

	/*
	 * Omit some permissions at first, so unauthorized users cannot nip
	 * in before the file/dir is ready.
	 */
	dst_mode_bits = src_mode & CHMOD_MODE_BITS;
	omitted_permissions = dst_mode_bits & (S_IRWXG | S_IRWXO);

	if (S_ISDIR(src_mode)) {
		/* dst_name should not exist */
		rc = mkdir(dst_name, dst_mode_bits & ~omitted_permissions);
		if (rc) {
			LERROR("cannot create directory [%s]: %s\n", dst_name,
			       strerror(errno));
			return rc;
		}

		/*
		 * We need search and write permissions to the new directory
		 * for writing the directory's contents. Check if these
		 * permissions are there.
		 */
		rc = lstat(dst_name, &dst_sb);
		if (rc) {
			LERROR("failed to stat [%s]: %s\n", dst_name,
			       strerror(errno));
			return rc;
		}

		if ((dst_sb.st_mode & S_IRWXU) != S_IRWXU) {
			/* Make the new directory searchable and writable.  */
			dst_mode = dst_sb.st_mode;
			restore_dst_mode = true;

			rc = chmod(dst_name, dst_mode | S_IRWXU);
			if (rc) {
				LERROR("failed to chmod [%s]: %s\n", dst_name,
				       strerror(errno));
				return rc;
			}
		}
	} else if (S_ISREG(src_mode)) {
		rc = create_stub_reg(src_name, dst_name, dst_mode,
				     omitted_permissions, &src_sb);
		if (rc) {
			LERROR("failed to create regular stub file [%s]\n",
			       dst_name);
			return rc;
		}
	} else if (S_ISLNK(src_mode)) {
		/* Symbol link doesn't need to */
		rc = create_stub_symlink(src_name, dst_name,
					 src_sb.st_size + 1);
		if (rc) {
			LERROR("failed to create symbol link [%s]\n",
			       dst_name);
			return rc;
		}
	} else if (S_ISBLK(src_mode) || S_ISCHR(src_mode) ||
		   S_ISSOCK(src_mode)) {
		rc = mknod(dst_name, src_mode & ~omitted_permissions,
			   src_sb.st_rdev);
		if (rc) {
			LERROR("failed to create special file [%s]\n",
			       dst_name);
			return rc;
		}
	} else if (S_ISFIFO(src_mode)) {
		rc = mknod(dst_name, src_mode & ~omitted_permissions, 0);
		if (rc) {
			LERROR("failed to create fifo [%s]\n",
			       dst_name);
			return rc;
		}
	} else {
		LERROR("[%s] has unkown file type\n", src_name);
		return -1;
	}

	rc = set_owner(dst_name, &src_sb);
	if (rc) {
		LERROR("failed to set owner [%s]\n", dst_name);
		return rc;
	}

	/* TODO: timestamps, acl, copy_xattr */

	/* Cannot set permissions of symbol link */
	if (S_ISLNK(src_mode))
		return 0;

	if (omitted_permissions && !restore_dst_mode) {
		/*
		 * Permissions were deliberately omitted when the file
		 * was created due to security concerns.  See whether
		 * they need to be re-added now.  It'd be faster to omit
		 * the lstat, but deducing the current destination mode
		 * is tricky in the presence of implementation-defined
		 * rules for special mode bits.
		 */
		rc = lstat(dst_name, &dst_sb);
		if (rc) {
			LERROR("failed to stat [%s]: %s\n", dst_name,
			       strerror(errno));
			return rc;
		}

		dst_mode = dst_sb.st_mode;
		if (omitted_permissions & ~dst_mode)
			restore_dst_mode = true;
	}

	if (restore_dst_mode) {
		rc = chmod(dst_name, dst_mode | omitted_permissions);
		if (rc) {
			LERROR("failed to chmod [%s]: %s\n", dst_name,
			       strerror(errno));
			return rc;
		}
	}

	return rc;
}

/* The function of nftw() to creat stub file */
static int nftw_fetch_fn(const char *fpath, const struct stat *sb,
			 int tflag, struct FTW *ftwbuf)
{
	int rc;
	char *cwd;
	const char *base;
	char cwd_buf[PATH_MAX];
	int cwdsz = sizeof(cwd_buf);
	char dest_dir[PATH_MAX];
	int dest_dir_size = sizeof(dest_dir);
	char full_fpath[PATH_MAX];
	/* The dest directory that contains the source basename */
	char *dest_source_dir = nftw_private.u.np_fetch.npf_dest_source_dir;
	int dest_source_size;
	char *dest = nftw_private.u.np_fetch.npf_dest;
	struct dest_entry **head;
	bool is_root = (strlen(fpath) == 1 && fpath[0] == '.');

	dest_source_size = sizeof(nftw_private.u.np_fetch.npf_dest_source_dir);
	head = &nftw_private.u.np_fetch.npf_dest_entry_table;

	rc = get_full_fpath(fpath, full_fpath, PATH_MAX);
	if (rc) {
		LERROR("failed to get full path of [%s]\n", fpath);
		return rc;
	}

	LDEBUG("%-3s %2d %7lld   %-40s %d %s\n",
	       (tflag == FTW_D) ?   "d"   : (tflag == FTW_DNR) ? "dnr" :
	       (tflag == FTW_DP) ?  "dp"  : (tflag == FTW_F) ?   "f" :
	       (tflag == FTW_NS) ?  "ns"  : (tflag == FTW_SL) ?  "sl" :
	       (tflag == FTW_SLN) ? "sln" : "???",
	       ftwbuf->level, (long long int)sb->st_size,
	       full_fpath, ftwbuf->base, fpath + ftwbuf->base);

	/* Only set directory and regular file to immutable */
	if (S_ISREG(sb->st_mode) || S_ISDIR(sb->st_mode)) {
		/* Lock the inode first before copying to dest */
		rc = lond_inode_lock(fpath, nftw_private.u.np_fetch.npf_key,
				     is_root);
		if (rc) {
			LERROR("failed to lock file [%s]\n", full_fpath);
			return rc;
		}
	}

	cwd = getcwd(cwd_buf, cwdsz);
	if (cwd == NULL) {
		LERROR("failed to get cwd: %s\n", strerror(errno));
		return -errno;
	}

	if (is_root) {
		base = basename(cwd);
		if (strlen(dest) == 1) {
			if (dest[0] != '/') {
				LERROR("unexpected dest [%s], expected [/]\n",
				       dest);
				return -EINVAL;
			}
			snprintf(dest_source_dir, dest_source_size, "/%s",
				 base);
		} else {
			snprintf(dest_source_dir, dest_source_size, "%s/%s",
				 dest, base);
		}
		rc = create_stub_inode(head, fpath, dest_source_dir);
	} else {
		snprintf(dest_dir, dest_dir_size, "%s/%s", dest_source_dir,
			 fpath);
		rc = create_stub_inode(head, fpath, dest_dir);
	}

	if (rc) {
		LERROR("failed to create stub inode of [%s] in target [%s]\n",
		       full_fpath, dest_source_dir);
		return rc;
	}
	return 0;
}

static int relative_path2absolute(char *path, int buf_size)
{
	char *cwd;
	char *tmp_buf;
	char cwd_buf[PATH_MAX];
	int cwdsz = sizeof(cwd_buf);

	if (path[0] == '/')
		return 0;

	cwd = getcwd(cwd_buf, cwdsz);
	if (cwd == NULL) {
		LERROR("failed to get cwd: %s\n", strerror(errno));
		return -errno;
	}

	tmp_buf = strdup(path);
	snprintf(path, buf_size, "%s/%s", cwd_buf, tmp_buf);
	free(tmp_buf);
	return 0;
}

/*
 * Assumptions:
 * 1) Source directories and dest are all Lustre directories.
 * 2) Source directories could be in different Lustre file system.
 * 3) Dest directory should not be on the same Lustre with any source
 *    directory.
 * 4) Dest directory shouldn't contain subdir that conflict with source
 *    directory names.
 * 5) Source directories should not repeat or contain each other. Otherwise,
 *    part of the actions will fail.
 * 6) No one else except LOND is using immutable flag.
 * 7) The tree of each source directory should not mount another file system.
 */
int main(int argc, char *const argv[])
{
	int i;
	int rc;
	int rc2 = 0;
	const char *source;
	int flags = FTW_PHYS;
	char *dest = nftw_private.u.np_fetch.npf_dest;
	int dest_size = sizeof(nftw_private.u.np_fetch.npf_dest);
	struct option long_opts[] = LOND_FETCH_OPTIONS;
	char *progname;
	char short_opts[] = "h";
	char key_str[LOND_KEY_STRING_SIZE];
	struct lond_key key;
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
			exit(1);
		default:
			LERROR("failed to parse option [%c]\n", c);
			usage(progname);
			exit(1);
		}
	}

	if (argc < optind + 2)
		usage(progname);

	lond_key_generate(&key);
	rc = lond_key_get_string(&key, key_str, sizeof(key_str));
	if (rc) {
		LERROR("failed to get the string of key\n");
		return rc;
	}

	strncpy(dest, argv[argc - 1], dest_size);
	/* Remove the '/'s in the tail */
	for (i = strlen(dest) - 1; i > 0; i--) {
		if (dest[i] == '/')
			dest[i] = '\0';
		else
			break;
	}
	if (strlen(dest) <= 0)
		usage(progname);
	rc = relative_path2absolute(dest, dest_size);
	if (rc) {
		LERROR("failed to get absolute path of target [%s]\n", dest);
		return rc;
	}

	nftw_private.u.np_fetch.npf_key = &key;
	nftw_private.u.np_fetch.npf_archive_id = 1;
	for (i = optind; i < argc - 1; i++) {
		source = argv[i];
		LINFO("fetching directory [%s] to target [%s] with lock key [%s]\n",
		      source, dest, key_str);
		rc = chdir(source);
		if (rc) {
			LERROR("failed to chdir to [%s]: %s\n", source,
			       strerror(errno));
			rc2 = rc2 ? rc2 : rc;
			continue;
		}

		nftw_private.u.np_fetch.npf_dest_entry_table = NULL;
		rc = nftw(".", nftw_fetch_fn, 32, flags);
		free_dest_table(&nftw_private.u.np_fetch.npf_dest_entry_table);
		if (rc) {
			LERROR("failed to fetch directory tree [%s] to target [%s] with key [%s]\n",
			       source, dest, key_str);
			rc2 = rc2 ? rc2 : rc;
			rc = lond_tree_unlock(".", false, &key, true);
			if (rc) {
				LERROR("you might want to run [lond unlock -k %s %s] to cleanup\n",
				       key_str, source);
			}
		} else {
			LINFO("fetched directory [%s] to target [%s] with lock key [%s]\n",
			      source, dest, key_str);
		}
	}

	return rc2;
}
