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
		"Usage: %s [option]... <source>... <dest>\n"
		"  source: global Lustre directory tree to fetch from\n"
		"  dest: local Lustre directory to fetch to\n"
		"  -r|--rename: rename the source directory after finished fetching\n",
		prog);
}

static inline void fid2str(char *buf, const struct lu_fid *fid, int len)
{
	snprintf(buf, len, DFID_NOBRACE, PFID(fid));
}

static int lond_write_local_xattr(char const *src_name, char const *dst_name,
				  int dst_fd, struct lond_key *key,
				  bool is_root)
{
	int rc;
	struct lond_local_xattr disk;

	memcpy(&disk.llx_key, key, sizeof(*key));
	disk.llx_is_root = is_root;
	disk.llx_magic = LOND_MAGIC;
	disk.llx_version = LOND_VERSION;

	rc = llapi_path2fid(src_name, &disk.llx_global_fid);
	if (rc) {
		LERROR("failed to get fid of [%s]\n", src_name);
		return rc;
	}
	if (dst_fd < 0)
		rc = lsetxattr(dst_name, XATTR_NAME_LOND_LOCAL,
			       &disk, sizeof(disk), 0);
	else
		rc = fsetxattr(dst_fd, XATTR_NAME_LOND_LOCAL,
			       &disk, sizeof(disk), 0);
	if (rc) {
		LERROR("failed to set xattr [%s] of inode [%s]: %s\n",
		       XATTR_NAME_LOND_LOCAL, dst_name, strerror(errno));
		return rc;
	}
	return rc;
}

static int create_stub_reg(char const *src_name, char const *dst_name,
			   mode_t dst_mode, mode_t omitted_permissions,
			   struct stat const *src_sb, void *private)
{
	int rc;
	int dest_desc;
	int open_flags = O_WRONLY | O_CREAT;
	char cmd[PATH_MAX];
	int cmdsz = sizeof(cmd);
	struct lond_key *key = (struct lond_key *)private;

	dest_desc = open(dst_name, open_flags | O_EXCL,
			 dst_mode & ~omitted_permissions);
	if (dest_desc < 0) {
		LERROR("failed to create regular file [%s]: %s\n",
		       dst_name, strerror(errno));
		return -1;
	}

	rc = lond_write_local_xattr(src_name, dst_name, dest_desc, key, false);
	if (rc) {
		LERROR("failed to write local xattr of regular file [%s]: %s\n",
		       dst_name, strerror(errno));
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

/* The function of nftw() to fetch files */
static int nftw_fetch_fn(const char *fpath, const struct stat *sb,
			 int tflag, struct FTW *ftwbuf)
{
	int rc;
	char *cwd;
	const char *base;
	char cwd_buf[PATH_MAX];
	int cwdsz = sizeof(cwd_buf);
	char dest_dir[PATH_MAX + 3];
	int dest_dir_size = sizeof(dest_dir);
	char full_fpath[PATH_MAX];
	/* The dest directory that contains the source basename */
	char *dest_source_dir = nftw_private.u.np_fetch.npf_dest_source_dir;
	int dest_source_size;
	char *dest = nftw_private.u.np_fetch.npf_dest;
	struct dest_entry **head;
	bool is_root = (strlen(fpath) == 1 && fpath[0] == '.');
	struct lond_key *key = nftw_private.u.np_fetch.npf_key;

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
		rc = lond_inode_lock(fpath, key, is_root);
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

		rc = lond_copy_inode(head, fpath, dest_source_dir,
				     create_stub_reg, key);
		if (rc) {
			LERROR("failed to create stub inode of [%s] in target [%s]\n",
			       full_fpath, dest_source_dir);
			return rc;
		}

		rc = lond_write_local_xattr(fpath, dest_source_dir, -1, key,
					    true);
		if (rc) {
			LERROR("failed to set local xattr on [%s]\n",
			       dest_source_dir);
			return rc;
		}
	} else {
		snprintf(dest_dir, dest_dir_size, "%s/%s", dest_source_dir,
			 fpath);
		rc = lond_copy_inode(head, fpath, dest_dir, create_stub_reg,
				     key);
		if (rc) {
			LERROR("failed to create stub inode of [%s] in target [%s]\n",
			       full_fpath, dest_source_dir);
			return rc;
		}
	}

	return rc;
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
 * This function should be called with pwd under $source directory and
 * holding lock of $source
 *
 * Assum $source = $dir/$base
 * Process of snapshot:
 * 1. $source=$(cwd) to get $base
 * 2. chattr -i .
 * 3. mv ../$dname ../$base.$key.lond
 * 4. chattr +i .
 *
 * There might be some race between 2 and 4, but that should be fine
 */

static int lond_rename(struct lond_key *key, const char *key_str)
{
	int rc;
	char *cwd;
	const char *base;
	char myself[PATH_MAX + 1];
	char dest[PATH_MAX + 1];
	char cwd_buf[PATH_MAX + 1];
	int cwdsz = sizeof(cwd_buf);

	cwd = getcwd(cwd_buf, cwdsz);
	if (cwd == NULL) {
		LERROR("failed to get cwd: %s\n", strerror(errno));
		return -errno;
	}

	base = basename(cwd);
	snprintf(dest, sizeof(dest), "../%s.%s.lond", base, key_str);
	snprintf(myself, sizeof(myself), "../%s", base);

	rc = lond_inode_unlock(".", false, key, false);
	if (rc) {
		LERROR("failed to unlock directory [%s] using key [%s]\n",
		       cwd, key_str);
		return rc;
	}

	rc = rename(myself, dest);
	if (rc) {
		rc = -errno;
		LERROR("failed to move directory [%s/%s] to [%s/%s]: %s\n",
		       cwd, myself, cwd, dest, strerror(errno));
		return rc;
	}

	/* lock immediately after rename, to reduce race possibility */
	rc = lond_inode_lock(".", key, true);
	if (rc) {
		cwd = getcwd(cwd_buf, cwdsz);
		if (cwd == NULL) {
			LERROR("failed to get cwd: %s\n", strerror(errno));
			LERROR("failed to lock directory [.] using key [%s]\n",
			       key_str);
			return -errno;
		}

		LERROR("failed to lock directory [%s] using key [%s]\n",
		       cwd, key_str);
		return rc;
	} else {
		cwd = getcwd(cwd_buf, cwdsz);
		if (cwd == NULL) {
			LERROR("failed to get cwd: %s\n", strerror(errno));
			return -errno;
		}
	}
	LINFO("original dir is saved as [%s]\n", cwd);

	return 0;
}

static int lond_fetch(const char *source, const char *dest,
		      const char *dest_fsname, struct lond_key *key,
		      const char *key_str, bool need_rename)
{
	int rc;
	int rc2;
	int flags = FTW_PHYS;
	char source_fsname[MAX_OBD_NAME + 1];

	rc = lustre_directory2fsname(source, source_fsname);
	if (rc) {
		LERROR("failed to get the fsname of [%s]\n",
		       source);
		return rc;
	}

	if (strcmp(source_fsname, dest_fsname) == 0) {
		LERROR("fetching from [%s] to [%s] in the same file system [%s] dosn't make any sense\n",
		       source, dest, source_fsname);
		return -EINVAL;
	}

	rc = check_lustre_root(source_fsname, source);
	if (rc < 0) {
		LERROR("failed to check whether directory [%s] is the root of file system [%s]\n",
		       source, source_fsname);
		return rc;
	} else if (rc == 0) {
		LERROR("directory [%s] shound't be fetched to [%s] because it is the root of file system [%s]\n",
		       source, dest, source_fsname);
		return rc;
	}

	LINFO("fetching directory [%s] to target [%s] with lock key [%s]\n",
	      source, dest, key_str);
	rc = chdir(source);
	if (rc) {
		LERROR("failed to chdir to [%s]: %s\n", source,
		       strerror(errno));
		return rc;
	}

	nftw_private.u.np_fetch.npf_dest_entry_table = NULL;
	rc = nftw(".", nftw_fetch_fn, 32, flags);
	free_dest_table(&nftw_private.u.np_fetch.npf_dest_entry_table);
	if (rc) {
		LERROR("failed to fetch directory tree [%s] to target [%s] with key [%s]\n",
		       source, dest, key_str);
		goto out_unlock;
	}

	LINFO("fetched directory [%s] to target [%s] with lock key [%s]\n",
	      source, dest, key_str);
	if (!need_rename)
		return 0;

	rc = lond_rename(key, key_str);
	if (rc) {
		LERROR("failed to rename [%s]\n", source);
		return rc;
	}
	return 0;
out_unlock:
	rc2 = lond_tree_unlock(".", false, key, true);
	if (rc2) {
		LERROR("failed to unlcok, you might want to run [lond unlock -k %s %s] to cleanup\n",
		       key_str, source);
	}
	return rc;
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
	int c;
	int rc;
	int rc2 = 0;
	const char *source;
	char *dest = nftw_private.u.np_fetch.npf_dest;
	int dest_size = sizeof(nftw_private.u.np_fetch.npf_dest);
	struct option long_opts[] = LOND_FETCH_OPTIONS;
	char *progname;
	char short_opts[] = "hr";
	char key_str[LOND_KEY_STRING_SIZE];
	struct lond_key key;
	char dest_fsname[MAX_OBD_NAME + 1];
	char *cwd;
	char cwd_buf[PATH_MAX + 1];
	int cwdsz = sizeof(cwd_buf);
	bool need_rename = false;

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
		case 'r':
			need_rename = true;
			break;
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

	strncpy(dest, argv[argc - 1], dest_size - 1);
	dest[dest_size - 1] = '\0';
	remove_slash_tail(dest);
	if (strlen(dest) <= 0)
		usage(progname);
	rc = relative_path2absolute(dest, dest_size);
	if (rc) {
		LERROR("failed to get absolute path of target [%s]\n", dest);
		return rc;
	}

	rc = lustre_directory2fsname(dest, dest_fsname);
	if (rc) {
		LERROR("failed to get the fsname of [%s]\n",
		       dest);
		return rc;
	}

	cwd = getcwd(cwd_buf, cwdsz);
	if (cwd == NULL) {
		LERROR("failed to get cwd: %s\n", strerror(errno));
		return -errno;
	}

	nftw_private.u.np_fetch.npf_key = &key;
	nftw_private.u.np_fetch.npf_archive_id = 1;
	for (i = optind; i < argc - 1; i++) {
		source = argv[i];
		rc = lond_fetch(source, dest, dest_fsname, &key, key_str,
				need_rename);
		rc2 = rc2 ? rc2 : rc;

		rc = chdir(cwd);
		if (rc) {
			LERROR("failed to chdir to [%s]: %s\n",
			       cwd, strerror(errno));
			rc2 = rc2 ? rc2 : rc;
			break;
		}
	}

	return rc2;
}
