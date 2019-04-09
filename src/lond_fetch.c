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
#include <uthash.h>
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

#define DEBUG_PERF 1

/* The dest directory to copy to */
char *dest;
/* The dest directory that contains the source basename */
char dest_source_dir[PATH_MAX];
#define LOND_IDENTITY_LENGH 10
char identity[LOND_IDENTITY_LENGH];
__u32 archive_id = 1;
#define XATTR_NAME_LOND	"trusted.lond"
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

struct dest_entry *dest_entry_table;

static void usage(const char *prog)
{
	fprintf(stderr,
		"Usage: %s SOURCE... DEST\n",
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
				    archive_id);
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

static int create_stub_inode(const char *src_name, const char *dst_name)
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
		rc = remember_copied(&dest_entry_table, dst_name,
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
static int lond_lock_eperm_reason(const char *fpath)
{
	int rc;
	bool immutable = false;
	char xattr_identity[LOND_IDENTITY_LENGH];

	rc = check_inode_is_immutable(fpath, &immutable);
	if (rc) {
		LERROR("failed to check whether file [%s] is immutable\n",
		       fpath);
		return rc;
	}

	if (!immutable) {
		LERROR("file [%s] is not immutable as expected\n", fpath);
		return -EPERM;
	}

	rc = getxattr(fpath, XATTR_NAME_LOND, xattr_identity,
		      sizeof(xattr_identity));
	if (rc < 0) {
		LERROR("failed to get LOND xattr of [%s]: %s\n",
		       fpath, strerror(errno));
		LERROR("file [%s] can't be locked because it is immutable\n",
		       fpath);
		return -errno;
	}

	if (strcmp(xattr_identity, identity) == 0)
		return 0;

	LERROR("file [%s] is being hold by LOND with ID [%s]\n", fpath,
	       xattr_identity);
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
static int lond_lock_inode(const char *fpath)
{
	int rc;
	int rc2;
	char cmd[PATH_MAX];
	int cmdsz = sizeof(cmd);
	char xattr_identity[LOND_IDENTITY_LENGH];

#ifdef DEBUG_PERF
	return 0;
#endif

	rc = setxattr(fpath, XATTR_NAME_LOND, &identity, sizeof(identity), 0);
	if (rc) {
		rc2 = -errno;
		if (errno == EPERM) {
			rc = lond_lock_eperm_reason(fpath);
			if (rc == 0)
				return 0;
		}
		LERROR("failed to set LOND xattr of [%s]: %s\n",
		       fpath, strerror(errno));
		return rc2;
	}

	snprintf(cmd, cmdsz, "chattr +i '%s'", fpath);
	rc = command_run(cmd, cmdsz);
	if (rc) {
		LERROR("failed to set immutable flag of [%s], rc = %d\n",
		       fpath, rc);
		return rc;
	}

	rc = getxattr(fpath, XATTR_NAME_LOND, xattr_identity,
		      sizeof(xattr_identity));
	if (rc < 0) {
		LERROR("failed to get LOND xattr of [%s]: %s\n",
		       fpath, strerror(errno));
		return rc;
	}

	if (strcmp(xattr_identity, identity) != 0) {
		LDEBUG("someone else locks [%s], expected identity [%s], got identity [%s]\n",
		       identity, xattr_identity);
		return -1;
	}
	LDEBUG("set immutable flag [%s]\n", fpath);
	return 0;
}

/*
 * Steps to unlock an inode:
 *
 * 1) If immutable flag is not set, the inode should not be locked or already
 *    been unlocked.
 * 2) Read the xattr.
 * 3) If the xattr matches the identity, chattr -i
 */
static int lond_unlock_inode(const char *fpath, bool may_used_by_other)
{
	int rc;
	char cmd[PATH_MAX];
	int cmdsz = sizeof(cmd);
	bool immutable = false;
	char xattr_identity[LOND_IDENTITY_LENGH];

	rc = check_inode_is_immutable(fpath, &immutable);
	if (rc) {
		LERROR("failed to check whether file [%s] is immutable\n",
		       fpath);
		return rc;
	}

	if (!immutable) {
		LDEBUG("file [%s] is not immutable, skipping unlocking\n",
		       fpath);
		return 0;
	}

	rc = getxattr(fpath, XATTR_NAME_LOND, xattr_identity,
		      sizeof(xattr_identity));
	if (rc < 0) {
		LERROR("failed to get LOND xattr of [%s]: %s\n",
		       fpath, strerror(errno));
		return rc;
	}

	if (strcmp(xattr_identity, identity) != 0) {
		if (may_used_by_other) {
			LDEBUG("someone else locks [%s], expected identity [%s], got identity [%s]\n",
			       identity, xattr_identity);
			return 0;
		} else {
			LERROR("someone else locks [%s], expected identity [%s], got identity [%s]\n",
			       identity, xattr_identity);
			return -1;
		}
	}

	snprintf(cmd, cmdsz, "chattr -i '%s'", fpath);
	rc = command_run(cmd, cmdsz);
	if (rc) {
		LERROR("failed to clear immutable flag of [%s], rc = %d\n",
		       fpath, rc);
		return rc;
	}
	LDEBUG("cleared immutable flag [%s]\n", fpath);
	return 0;
}

/* The function of nftw() to creat stub file */
static int nftw_create_stub_fn(const char *fpath, const struct stat *sb,
			       int tflag, struct FTW *ftwbuf)
{
	int rc;
	char *cwd;
	const char *base;
	char cwd_buf[PATH_MAX];
	int cwdsz = sizeof(cwd_buf);
	char dest_dir[PATH_MAX];
	int dest_dir_size = sizeof(dest_dir);

	LDEBUG("%-3s %2d %7lld   %-40s %d %s\n",
	       (tflag == FTW_D) ?   "d"   : (tflag == FTW_DNR) ? "dnr" :
	       (tflag == FTW_DP) ?  "dp"  : (tflag == FTW_F) ?   "f" :
	       (tflag == FTW_NS) ?  "ns"  : (tflag == FTW_SL) ?  "sl" :
	       (tflag == FTW_SLN) ? "sln" : "???",
	       ftwbuf->level, (long long int)sb->st_size,
	       fpath, ftwbuf->base, fpath + ftwbuf->base);

	/* Only set directory and regular file to immutable */
	if (S_ISREG(sb->st_mode) || S_ISDIR(sb->st_mode)) {
		/* Lock the inode first before copying to dest */
		rc = lond_lock_inode(fpath);
		if (rc) {
			LERROR("failed to lock file [%s]\n", fpath);
			return rc;
		}
	}

	cwd = getcwd(cwd_buf, cwdsz);
	if (cwd == NULL) {
		LERROR("failed to get cwd: %s\n", strerror(errno));
		return -errno;
	}

	if (strlen(fpath) == 1 && fpath[0] == '.') {
		base = basename(cwd);
		if (strlen(dest) == 1) {
			LASSERT(dest[0] == '/');
			snprintf(dest_source_dir, sizeof(dest_source_dir),
				 "/%s", base);
		} else {
			snprintf(dest_source_dir, sizeof(dest_source_dir),
				 "%s/%s", dest, base);
		}
		rc = create_stub_inode(fpath, dest_source_dir);
	} else {
		snprintf(dest_dir, dest_dir_size, "%s/%s", dest_source_dir,
			 fpath);
		rc = create_stub_inode(fpath, dest_dir);
	}

	if (rc) {
		LERROR("failed to create stub file\n");
		return rc;
	}
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
	rc = lond_unlock_inode(fpath, true);
	if (rc) {
		LERROR("failed to unlock file [%s]\n", fpath);
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

	if (dest[0] == '/')
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

/* Return a random int smaller than n */
static int random_int(int n)
{
	return rand() % n;
}


const char alphabet[] =
"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

/* Fill the string with random */
void randomize_string(char *string, int buf_size)
{
	int i;

	LASSERT(buf_size > 1);
	for (i = 0; i < buf_size - 1; i++)
		string[i] = alphabet[random_int(strlen(alphabet))];
	string[buf_size - 1] = '\0';
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
	char dest_buf[PATH_MAX];
	int dest_buf_size = sizeof(dest_buf);
	struct option long_opts[] = FETCH_LONG_OPTIONS;
	char *progname;
	char short_opts[] = "h";
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

	randomize_string(identity, sizeof(identity));

	dest = dest_buf;
	strncpy(dest_buf, argv[argc - 1], dest_buf_size);
	for (i = strlen(dest) - 1; i > 0; i--) {
		if (dest[i] == '/')
			dest[i] = '\0';
		else
			break;
	}
	if (strlen(dest) <= 0)
		usage(progname);
	rc = relative_path2absolute(dest, dest_buf_size);
	if (rc) {
		LERROR("failed to get absolute path of [%s]\n", dest);
		return rc;
	}

	for (i = optind; i < argc - 1; i++) {
		source = argv[i];
	    LINFO("fetching directory [%s] to dest [%s]\n", source, dest);
		rc = chdir(source);
		if (rc) {
			LERROR("failed to chdir to [%s]: %s\n", source,
			       strerror(errno));
			rc2 = rc2 ? rc2 : rc;
			continue;
		}

		rc = nftw(".", nftw_create_stub_fn, 32, flags);
		if (rc) {
			LERROR("failed to walk directory [%s] to create stub files\n",
			       source);
			rc2 = rc2 ? rc2 : rc;
			rc = nftw(".", nftw_unlock_fn, 32, flags);
			if (rc) {
				LERROR("failed to walk directory [%s] to unlock files\n",
				       source);
			}
		}
	}

	free_dest_table(&dest_entry_table);

#ifndef DEBUG_PERF
	for (i = optind; i < argc - 1; i++) {
		source = argv[i];
		rc = chdir(source);
		if (rc) {
			LERROR("failed to chdir to [%s]: %s\n", source,
			       strerror(errno));
			rc2 = rc2 ? rc2 : rc;
			continue;
		}

		rc = nftw(".", nftw_unlock_fn, 32, flags);
		if (rc) {
			LERROR("failed to walk directory [%s] to unlock files\n",
			       source);
			rc2 = rc2 ? rc2 : rc;
		}
	}
#endif
	return rc2;
}
