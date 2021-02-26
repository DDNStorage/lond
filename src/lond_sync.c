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
#include <stdint.h>
#include <attr/attributes.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <attr/xattr.h>
#include <unistd.h>
#include <string.h>
#include <ftw.h>
#include <lustre/lustreapi.h>
#include "definition.h"
#include "debug.h"
#include "lond.h"
#include "cmd.h"

struct dest_entry *dest_entry_table;

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s <source>... <dest>\n"
		"  source: local Lustre directory to sync from\n"
		"  dest: global Lustre directory to sync to\n",
		prog);
}

static int lond_copy(const char *source, const char *dest)
{
	int rc;
	const char *base;
	char dest_source_dir[PATH_MAX + 2];
	char cmd[PATH_MAX * 2 + 12];
	int cmdsz = sizeof(cmd);

	base = basename(source);
	snprintf(dest_source_dir, sizeof(dest_source_dir), "%s/%s",
		 dest, base);

	rc = access(dest_source_dir, F_OK);
	if (rc == 0) {
		LERROR("[%s] already exists\n", dest_source_dir);
		return -EEXIST;
	} else if (errno != ENOENT) {
		LERROR("failed to check whether [%s] already exists\n",
		       dest_source_dir);
		return -errno;
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

static int copy_data(char const *src_name, int src_desc, char const *dst_name,
		     mode_t dst_mode, mode_t omitted_permissions,
		     char *buf, int buf_size)
{
	int rc = 0;
	int dest_desc;
	ssize_t n_read;
	ssize_t n_write;

	dest_desc = open(dst_name, O_WRONLY | O_CREAT | O_EXCL,
			 dst_mode & ~omitted_permissions);
	if (dest_desc < 0) {
		LERROR("failed to create regular file [%s]: %s\n",
		       dst_name, strerror(errno));
		return -errno;
	}

	while (1) {
		n_read = read(src_desc, buf, buf_size);
		if (n_read < 0) {
			if (errno == EINTR)
				continue;
			LERROR("failed to read [%s]: %s\n", src_name,
			       strerror(errno));
			rc = -errno;
			goto out_close;
		}
		if (n_read == 0)
			break;

		n_write = write(dest_desc, buf, n_read);
		if (n_write < 0) {
			LERROR("failed to write [%s]: %s\n", dst_name,
			       strerror(errno));
			rc = -errno;
			goto out_close;
		} else if (n_write != n_read) {
			LERROR("short write of [%s], trying to write [%llu], written [%llu]\n",
			       dst_name, n_read, n_write);
			rc = -errno;
			goto out_close;
		}
	}

out_close:
	if (close(dest_desc) < 0) {
		LERROR("failed to close regular file [%s]: %s\n",
		       dst_name, strerror(errno));
		return -errno;
	}
	return rc;
}

int lond_link(const char *source, const char *dest, struct lond_key *key,
	      const char *key_str)
{
	int rc;
	struct lond_xattr lond_xattr;
	struct lond_key *global_key;
	const char *global_key_str;
	bool immutable = false;

	rc = lond_read_global_xattr(source, &lond_xattr);
	if (rc) {
		LERROR("failed to get global lond xattr of file [%s]: %s\n",
		       source, strerror(errno));
		return rc;
	}

	if (!lond_xattr.lx_is_valid) {
		LERROR("file [%s] doesn't have valid lond key: %s\n",
		       source, lond_xattr.lx_invalid_reason);
		return -ENOATTR;
	}

	global_key = &lond_xattr.u.lx_global.lgx_key;
	global_key_str = lond_xattr.lx_key_str;

	if (!lond_key_equal(global_key, key)) {
		LERROR("file [%s] doesn't have expected key, expected [%s], got [%s]\n",
		       source, key_str, global_key_str);
		return -ENOATTR;
	}

	rc = check_inode_is_immutable(source, &immutable);
	if (rc) {
		LERROR("failed to check whether file [%s] is immutable\n",
		       source);
		return rc;
	}

	if (immutable) {
		rc = lond_inode_unlock(source, false, key, false);
		if (rc) {
			LERROR("failed to unlock file [%s]\n",
			       source);
			return rc;
		}
	}

	rc = link(source, dest);
	if (rc) {
		LERROR("failed to create hard link from [%s] to [%s]: %s\n",
		       source, dest, strerror(errno));
		rc = -errno;
	}
	return rc;
}

static int sync_reg(char const *src_name, char const *dst_name,
		    mode_t dst_mode, mode_t omitted_permissions,
		    struct stat const *src_sb, void *private)
{
	int rc;
	int src_desc;
	const char *key_str;
	struct lond_key *key;
	struct hsm_user_state hus;
	struct lu_fid *global_fid;
	struct lond_xattr lond_xattr;
	char origin_source[PATH_MAX + 1];
	struct nftw_private *nprivate = (struct nftw_private *)private;
	const char *dst_mnt = nprivate->u.np_sync.nps_dest_mnt;
	char *copy_buf = nprivate->u.np_sync.nps_copy_buf;
	int buf_size = nprivate->u.np_sync.nps_copy_buf_size;

	src_desc = open(src_name, O_RDONLY | O_NONBLOCK);
	if (src_desc < 0) {
		LERROR("failed to open source file [%s]: %s\n",
		       src_name, strerror(errno));
		return -errno;
	}

	rc = lond_read_local_xattr(src_name, &lond_xattr);
	if (rc) {
		LERROR("failed to read local xattr of [%s]\n", src_name);
		goto out_close;
	}

	if (!lond_xattr.lx_is_valid) {
		LDEBUG("file [%s] was not fetched by lond, copying the data\n",
		       src_name);
		goto out_copy;
	}

	global_fid = &lond_xattr.u.lx_local.llx_global_fid;
	lustre_fid_path(origin_source, sizeof(origin_source), dst_mnt,
			global_fid);
	rc = access(origin_source, F_OK);
	if (rc < 0) {
		if (errno == ENOENT) {
			LDEBUG("original source [%s] of file [%s] doesn't exists, copying the data\n",
			       src_name, origin_source);
			goto out_copy;
		} else {
			LERROR("failed to check whether [%s] already exists\n",
			       origin_source);
			return -errno;
		}
	}

	rc = llapi_hsm_state_get_fd(src_desc, &hus);
	if (rc) {
		LERROR("failed to get HSM state of source file [%s]: %s\n",
		       src_name, strerror(-rc));
		goto out_close;
	}

	if (hus.hus_states & HS_DIRTY) {
		LDEBUG("HSM states of file [%s] is dirty, copying the data\n",
		       src_name);
		goto out_copy;
	} else if (hus.hus_states & HS_ARCHIVED) {
		/* The file is not updated, create hardlink */
		key = &lond_xattr.u.lx_local.llx_key;
		key_str = lond_xattr.lx_key_str;
		rc = lond_link(origin_source, dst_name, key, key_str);
		goto out_close;
	} else {
		LDEBUG("HSM states of file [%s] is not 'exists', copying the data\n",
		       src_name);
		goto out_copy;
	}

out_copy:
	rc = copy_data(src_name, src_desc, dst_name, dst_mode,
		       omitted_permissions, copy_buf, buf_size);
	if (rc)
		LERROR("failed to copy data from [%s] to [%s]\n",
		       src_name, dst_name);
out_close:
	close(src_desc);
	return rc;
}

/* The function of nftw() to sync files */
static int nftw_sync_fn(const char *fpath, const struct stat *sb,
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
	char *dest_source_dir = nftw_private.u.np_sync.nps_dest_source_dir;
	int dest_source_size;
	char *dest = nftw_private.u.np_sync.nps_dest;
	struct dest_entry **head;
	bool is_root = (strlen(fpath) == 1 && fpath[0] == '.');

	dest_source_size = sizeof(nftw_private.u.np_sync.nps_dest_source_dir);
	head = &nftw_private.u.np_sync.nps_dest_entry_table;

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
		rc = lond_copy_inode(head, fpath, dest_source_dir, sync_reg,
				     &nftw_private);
	} else {
		snprintf(dest_dir, dest_dir_size, "%s/%s", dest_source_dir,
			 fpath);
		rc = lond_copy_inode(head, fpath, dest_dir, sync_reg,
				     &nftw_private);
	}

	if (rc) {
		LERROR("failed to sync inode of [%s] in target [%s]\n",
		       full_fpath, dest_source_dir);
		return rc;
	}

	return rc;
}

static void lond_sync_nfwt_dir_init(struct nftw_private *nftwp)
{
	nftwp->u.np_sync.nps_dest_entry_table = NULL;
}

static void lond_sync_nfwt_dir_fini(struct nftw_private *nftwp)
{
	free_dest_table(&nftw_private.u.np_sync.nps_dest_entry_table);
}

static int lond_sync_nfwt_init(struct nftw_private *nftwp)
{
	int size = 1048576 * 16;
	char *buf;

	buf = malloc(size);
	nftwp->u.np_sync.nps_copy_buf = buf;
	nftwp->u.np_sync.nps_copy_buf_size = size;
	return -ENOMEM;
}

static void lond_sync_nfwt_fini(struct nftw_private *nftwp)
{
	free(nftwp->u.np_sync.nps_copy_buf);
}

static int lond_quick_sync(const char *source, const char *source_fsname,
			   const char *dest, const char *dest_fsname)
{
	int rc;
	const char *base;
	char dest_source_dir[PATH_MAX + 2];
	int flags = FTW_PHYS;
	char *dest_buffer = nftw_private.u.np_sync.nps_dest;
	int dest_size = sizeof(nftw_private.u.np_sync.nps_dest);
	char *dest_mnt = nftw_private.u.np_sync.nps_dest_mnt;
	char *source_mnt = nftw_private.u.np_sync.nps_source_mnt;

	rc = llapi_search_rootpath(source_mnt, source_fsname);
	if (rc) {
		LERROR("failed to get root path of Lustre file system [%s]: %s\n",
		       source_fsname, strerror(-rc));
		return rc;
	}

	rc = llapi_search_rootpath(dest_mnt, dest_fsname);
	if (rc) {
		LERROR("failed to get root path of Lustre file system [%s]: %s\n",
		       dest_fsname, strerror(-rc));
		return rc;
	}

	base = basename(source);
	snprintf(dest_source_dir, sizeof(dest_source_dir), "%s/%s",
		 dest, base);

	rc = access(dest_source_dir, F_OK);
	if (rc == 0) {
		LERROR("[%s] already exists\n", dest_source_dir);
		return -EEXIST;
	} else if (errno != ENOENT) {
		LERROR("failed to check whether [%s] already exists\n",
		       dest_source_dir);
		return -errno;
	}

	rc = chdir(source);
	if (rc) {
		LERROR("failed to chdir to [%s]: %s\n", source,
		       strerror(errno));
		return rc;
	}

	strncpy(dest_buffer, dest, dest_size);
	lond_sync_nfwt_dir_init(&nftw_private);
	rc = nftw(".", nftw_sync_fn, 32, flags);
	lond_sync_nfwt_dir_fini(&nftw_private);
	if (rc) {
		LERROR("failed to sync directory tree [%s] to target [%s]\n",
		       source, dest);
		return rc;
	}
	return 0;
}

static int lond_sync(const char *source, const char *dest, bool copy)
{
	int rc;
	struct lond_xattr lond_xattr;
	char source_fsname[MAX_OBD_NAME + 1];
	char dest_fsname[MAX_OBD_NAME + 1];

	LDEBUG("syncing from [%s] to [%s]\n", source, dest);

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

	rc = lond_read_local_xattr(source, &lond_xattr);
	if (rc) {
		LERROR("failed to read local xattr of [%s]\n", source);
		return rc;
	}

	if (!lond_xattr.lx_is_valid) {
		LERROR("directory [%s] doesn't have valid local lond xattr: %s\n",
		       source, lond_xattr.lx_invalid_reason);
		LERROR("[%s] is not fetched through lond\n",
		       source, lond_xattr.lx_invalid_reason);
		rc = -ENODATA;
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

	if (copy) {
		rc = lond_copy(source, dest);
		if (rc) {
			LERROR("failed to copy from [%s] to [%s]\n", source,
			       dest);
			return rc;
		}
	} else {
		rc = lond_quick_sync(source, source_fsname, dest, dest_fsname);
		if (rc) {
			LERROR("failed to sync quickly from [%s] to [%s]\n",
			       source, dest);
			return rc;
		}
	}

	LINFO("synced from [%s] to [%s]\n", source, dest);
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
	int c;
	int i;
	int rc;
	int rc2 = 0;
	bool copy = false;
	const char *progname;
	char dest[PATH_MAX + 1];
	char source[PATH_MAX + 1];
	char short_opts[] = "ch";
	struct option long_opts[] = LOND_SYNC_OPTIONS;

	progname = argv[0];
	while ((c = getopt_long(argc, argv, short_opts,
				long_opts, NULL)) != -1) {
		switch (c) {
		case OPT_PROGNAME:
			progname = optarg;
			break;
		case 'c':
			copy = true;
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

	if (argc <= optind + 1) {
		LERROR("please specify the local and global Lustre directories to sync between\n");
		usage(progname);
		return -EINVAL;
	}

	strncpy(dest, argv[argc - 1], sizeof(dest) - 1);
	dest[sizeof(dest) - 1] = '\0';
	remove_slash_tail(dest);

	lond_sync_nfwt_init(&nftw_private);
	for (i = optind; i < argc - 1; i++) {
		strncpy(source, argv[i], sizeof(source) - 1);
		source[sizeof(source) - 1] = '\0';
		remove_slash_tail(source);
		rc = lond_sync(source, dest, copy);
		if (rc) {
			LERROR("failed to sync from [%s] to [%s]\n", source,
			       dest);
			rc2 = rc2 ? rc2 : rc;
		}
	}
	lond_sync_nfwt_fini(&nftw_private);

	return rc2;
}
