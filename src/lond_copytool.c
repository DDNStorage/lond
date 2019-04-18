/*
 * Copyright (c) 2019, DDN Storage Corporation.
 */
/*
 *
 * Copytool for Lustre On Demand.
 *
 * Author: Li Xi <lixi@ddn.com>
 */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <attr/xattr.h>
#include <pthread.h>
#include <signal.h>
#include <getopt.h>
#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/time.h>
#include <lustre/lustreapi.h>
#include "debug.h"
#include "lond.h"

static int err_major;
static int err_minor;
static struct hsm_copytool_private *ctdata;
static char fs_name[MAX_OBD_NAME + 1];
static bool exiting;

struct copytool_options {
	char			*o_hsm_root;
	/* Lustre mount point */
	char			*o_mnt;
	int			 o_mnt_fd;
	/* Number of archive IDs that can be saved */
	int			 o_archive_id_used;
	/* Number of valid archive IDs */
	int			 o_archive_id_cnt;
	int			*o_archive_id;
	bool			 o_all_id;
	bool			 o_abort_on_error;
	int			 o_daemonize;
	int			 o_report_int;
	int			 o_chunk_size;
	unsigned long long	 o_bandwidth;
};

/* Progress reporting period */
#define REPORT_INTERVAL_DEFAULT 30
#ifndef NSEC_PER_SEC
# define NSEC_PER_SEC 1000000000UL
#endif

struct copytool_options opt = {
	.o_report_int = REPORT_INTERVAL_DEFAULT,
	.o_chunk_size = 1048676,
};

static void handler(int signal)
{
	psignal(signal, "exiting");
	/*
	 * If we don't clean up upon interrupt, umount thinks there's a ref
	 * and doesn't remove us from mtab (EINPROGRESS). The lustre client
	 * does successfully unmount and the mount is actually gone, but the
	 * mtab entry remains. So this just makes mtab happier.
	 */
	llapi_hsm_copytool_unregister(&ctdata);
	exiting = true;
}

static int path_lustre(char *buf, int sz, const char *mnt,
			  const struct lu_fid *fid)
{
	return snprintf(buf, sz, "%s/%s/fid/"DFID_NOBRACE, mnt,
			dot_lustre_name, PFID(fid));
}

static int action_fini(struct hsm_copyaction_private **phcp,
		       const struct hsm_action_item *hai, int hp_flags,
		       int ct_rc)
{
	struct hsm_copyaction_private	*hcp;
	char				 lstr[PATH_MAX];
	int				 rc;

	LDEBUG("Action completed, notifying coordinator cookie=%#jx, FID="DFID", hp_flags=%d err=%d\n",
	       (uintmax_t)hai->hai_cookie, PFID(&hai->hai_fid),
	       hp_flags, -ct_rc);

	path_lustre(lstr, sizeof(lstr), opt.o_mnt, &hai->hai_fid);

	if (phcp == NULL || *phcp == NULL) {
		rc = llapi_hsm_action_begin(&hcp, ctdata, hai, -1, 0, true);
		if (rc < 0) {
			LERROR("llapi_hsm_action_begin() on [%s] failed\n",
			       lstr);
			return rc;
		}
		phcp = &hcp;
	}

	rc = llapi_hsm_action_end(phcp, &hai->hai_extent, hp_flags, abs(ct_rc));
	if (rc == -ECANCELED)
		LERROR("completed action on '%s' has been canceled: cookie=%#jx, FID="DFID"\n",
		       lstr,
		       (uintmax_t)hai->hai_cookie, PFID(&hai->hai_fid));
	else if (rc < 0)
		LERROR("llapi_hsm_action_end() on [%s] failed\n", lstr);
	else
		LDEBUG("llapi_hsm_action_end() on [%s] ok (rc=%d)\n",
		       lstr, rc);

	return rc;
}

static int process_archive(const struct hsm_action_item *hai,
			   const long hal_flags)
{
	return -1;
}

static int begin_restore(struct hsm_copyaction_private **phcp,
			 const struct hsm_action_item *hai,
			 int mdt_index, int open_flags)
{
	char src[PATH_MAX];
	int rc;

	rc = llapi_hsm_action_begin(phcp, ctdata, hai, mdt_index, open_flags,
				    false);
	if (rc < 0) {
		path_lustre(src, sizeof(src), opt.o_mnt, &hai->hai_fid);
		LERROR("llapi_hsm_action_begin() on '%s' failed\n", src);
	}

	return rc;
}

static inline double time_now(void)
{
	struct timeval tv;

	gettimeofday(&tv, NULL);

	return tv.tv_sec + 0.000001 * tv.tv_usec;
}

static int copy_data(struct hsm_copyaction_private *hcp, const char *src,
		     const char *dst, int src_fd, int dst_fd,
		     const struct hsm_action_item *hai, long hal_flags)
{
	struct hsm_extent	 he;
	__u64			 offset = hai->hai_extent.offset;
	struct stat		 src_st;
	struct stat		 dst_st;
	char			*buf = NULL;
	__u64			 write_total = 0;
	__u64			 length = hai->hai_extent.length;
	time_t			 last_report_time;
	int			 rc = 0;
	double			 start_ct_now = time_now();
	/* Bandwidth Control */
	time_t			start_time;
	time_t			now;
	time_t			last_bw_print;

	if (fstat(src_fd, &src_st) < 0) {
		rc = -errno;
		LERROR("cannot stat [%s]\n", src);
		return rc;
	}

	if (!S_ISREG(src_st.st_mode)) {
		rc = -EINVAL;
		LERROR("[%s] is not a regular file\n", src);
		return rc;
	}

	if (hai->hai_extent.offset > (__u64)src_st.st_size) {
		rc = -EINVAL;
		LERROR("trying to start reading past end (%ju > %jd) of [%s] source file\n",
		       (uintmax_t)hai->hai_extent.offset,
		       (intmax_t)src_st.st_size, src);
		return rc;
	}

	if (fstat(dst_fd, &dst_st) < 0) {
		rc = -errno;
		LERROR("cannot stat [%s]\n", dst);
		return rc;
	}

	if (!S_ISREG(dst_st.st_mode)) {
		rc = -EINVAL;
		LERROR("[%s] is not a regular file\n", dst);
		return rc;
	}

	/* Don't read beyond a given extent */
	if (length > src_st.st_size - hai->hai_extent.offset)
		length = src_st.st_size - hai->hai_extent.offset;

	start_time = last_bw_print = last_report_time = time(NULL);

	he.offset = offset;
	he.length = 0;
	rc = llapi_hsm_action_progress(hcp, &he, length, 0);
	if (rc < 0) {
		/* Action has been canceled or something wrong
		 * is happening. Stop copying data.
		 */
		LERROR("progress ioctl for copy [%s]->[%s] failed\n",
		       src, dst);
		goto out;
	}

	errno = 0;

	buf = malloc(opt.o_chunk_size);
	if (buf == NULL) {
		rc = -ENOMEM;
		goto out;
	}

	LDEBUG("start copy of %ju bytes from [%s] to [%s]\n",
	       (uintmax_t)length, src, dst);

	while (write_total < length) {
		ssize_t	rsize;
		ssize_t	wsize;
		int	chunk = (length - write_total > opt.o_chunk_size) ?
				 opt.o_chunk_size : length - write_total;

		rsize = pread(src_fd, buf, chunk, offset);
		if (rsize == 0)
			/* EOF */
			break;

		if (rsize < 0) {
			rc = -errno;
			LERROR("cannot read from [%s]\n", src);
			break;
		}

		wsize = pwrite(dst_fd, buf, rsize, offset);
		if (wsize < 0) {
			rc = -errno;
			LERROR("cannot write to [%s]\n", dst);
			break;
		}

		write_total += wsize;
		offset += wsize;

		now = time(NULL);
		/* sleep if needed, to honor bandwidth limits */
		if (opt.o_bandwidth != 0) {
			unsigned long long write_theory;

			write_theory = (now - start_time) * opt.o_bandwidth;

			if (write_theory < write_total) {
				unsigned long long	excess;
				struct timespec		delay;

				excess = write_total - write_theory;

				delay.tv_sec = excess / opt.o_bandwidth;
				delay.tv_nsec = (excess % opt.o_bandwidth) *
					NSEC_PER_SEC / opt.o_bandwidth;

				if (now >= last_bw_print + opt.o_report_int) {
					LDEBUG("bandwith control: %lluB/s excess=%llu sleep for %lld.%09lds\n",
					       opt.o_bandwidth, excess,
					       (long long)delay.tv_sec,
					       delay.tv_nsec);
					last_bw_print = now;
				}

				do {
					rc = nanosleep(&delay, &delay);
				} while (rc < 0 && errno == EINTR);
				if (rc < 0) {
					LERROR("delay for bandwidth control failed to sleep: residual=%lld.%09lds\n",
					       (long long)delay.tv_sec,
					       delay.tv_nsec);
					rc = 0;
				}
			}
		}

		now = time(NULL);
		if (now >= last_report_time + opt.o_report_int) {
			last_report_time = now;
			LDEBUG("%%%ju\n",
			       (uintmax_t)(100 * write_total / length));
			/* only give the length of the write since the last
			 * progress report
			 */
			he.length = offset - he.offset;
			rc = llapi_hsm_action_progress(hcp, &he, length, 0);
			if (rc < 0) {
				/* Action has been canceled or something wrong
				 * is happening. Stop copying data.
				 */
				LERROR("progress ioctl for copy [%s]->[%s] failed\n",
				       src, dst);
				goto out;
			}
			he.offset = offset;
		}
		rc = 0;
	}

out:
	/*
	 * truncate restored file
	 * size is taken from the archive this is done to support
	 * restore after a force release which leaves the file with the
	 * wrong size (can big bigger than the new size)
	 */
	if ((hai->hai_action == HSMA_RESTORE) &&
	    (src_st.st_size < dst_st.st_size)) {
		/*
		 * make sure the file is on disk before reporting success.
		 */
		rc = ftruncate(dst_fd, src_st.st_size);
		if (rc < 0) {
			rc = -errno;
			LERROR("cannot truncate [%s] to size %jd\n",
			       dst, (intmax_t)src_st.st_size);
			err_major++;
		}
	}

	if (buf != NULL)
		free(buf);

	LDEBUG("copied %ju bytes in %f seconds\n",
	       (uintmax_t)length, time_now() - start_ct_now);

	return rc;
}

static int process_restore(const struct hsm_action_item *hai,
			   const long hal_flags)
{
	int rc;
	char src[PATH_MAX]; /* HSM file */
	char dst[PATH_MAX]; /* Lustre file */
	int open_flags = 0;
	int mdt_index = -1;
	int src_fd = -1;
	int dst_fd = -1;
	int hp_flags = 0;
	struct hsm_copyaction_private *hcp = NULL;
	struct lond_xattr lond_xattr;

	rc = llapi_get_mdt_index_by_fid(opt.o_mnt_fd, &hai->hai_fid,
					&mdt_index);
	if (rc < 0) {
		LERROR("cannot get mdt index "DFID"\n", PFID(&hai->hai_fid));
		return rc;
	}

	rc = begin_restore(&hcp, hai, mdt_index, open_flags);
	if (rc < 0) {
		LERROR("failed to begin retore\n");
		goto fini;
	}

	path_lustre(dst, sizeof(dst), opt.o_mnt, &hai->hai_fid);
	rc = lond_read_local_xattr(dst, &lond_xattr);
	if (rc) {
		LERROR("failed to read local xattr of [%s]\n", dst);
		goto fini;
	}

	if (!lond_xattr.lx_is_valid) {
		LERROR("xattr of file [%s] is not valid\n", dst);
		rc = -ENODATA;
		goto fini;
	}

	path_lustre(src, sizeof(src), opt.o_hsm_root,
		    &lond_xattr.u.lx_local.llx_global_fid);

	src_fd = open(src, O_RDONLY | O_NOATIME | O_NOFOLLOW);
	if (src_fd < 0) {
		rc = -errno;
		LERROR("cannot open [%s] for read", src, strerror(errno));
		goto fini;
	}

	dst_fd = llapi_hsm_action_get_fd(hcp);
	if (dst_fd < 0) {
		rc = dst_fd;
		LERROR("cannot open [%s] for write\n", dst);
		goto fini;
	}

	rc = copy_data(hcp, src, dst, src_fd, dst_fd, hai, hal_flags);
	if (rc < 0) {
		LERROR("cannot copy data from [%s] to [%s]",
		       src, dst);
		err_major++;
		if (rc == -ETIMEDOUT)
			hp_flags |= HP_FLAG_RETRY;
		goto fini;
	}

fini:
	rc = action_fini(&hcp, hai, hp_flags, rc);

	/* object swaping is done by cdt at copy end, so close of volatile file
	 * cannot be done before
	 */
	if (!(src_fd < 0))
		close(src_fd);

	return rc;
}

static int process_remove(const struct hsm_action_item *hai,
			  const long hal_flags)
{
	return -1;
}

static int process_item(struct hsm_action_item *hai, const long hal_flags)
{
	int rc;

	switch (hai->hai_action) {
	/* set err_major, minor inside these functions */
	case HSMA_ARCHIVE:
		rc = process_archive(hai, hal_flags);
		break;
	case HSMA_RESTORE:
		rc = process_restore(hai, hal_flags);
		break;
	case HSMA_REMOVE:
		rc = process_remove(hai, hal_flags);
		break;
	case HSMA_CANCEL:
		LERROR("cancel not implemented for file system [%s]\n",
		      opt.o_mnt);
		/* Don't report progress to coordinator for this cookie:
		 * the copy function will get ECANCELED when reporting
		 * progress.
		 */
		err_minor++;
		return 0;
	default:
		rc = -EINVAL;
		LERROR("unknown action [%d] on [%s]\n", hai->hai_action,
		       opt.o_mnt);
		err_minor++;
		action_fini(NULL, hai, 0, rc);
	}

	return 0;
}

struct thread_data {
	long			 hal_flags;
	struct hsm_action_item	*hai;
};

static void *process_thread(void *data)
{
	struct thread_data *cttd = data;
	int rc;

	rc = process_item(cttd->hai, cttd->hal_flags);

	free(cttd->hai);
	free(cttd);
	pthread_exit((void *)(intptr_t)rc);
}

static int process_item_async(const struct hsm_action_item *hai,
			      long hal_flags)
{
	pthread_attr_t		 attr;
	pthread_t		 thread;
	struct thread_data	*data;
	int			 rc;

	data = malloc(sizeof(*data));
	if (data == NULL)
		return -ENOMEM;

	data->hai = malloc(hai->hai_len);
	if (data->hai == NULL) {
		free(data);
		return -ENOMEM;
	}

	memcpy(data->hai, hai, hai->hai_len);
	data->hal_flags = hal_flags;

	rc = pthread_attr_init(&attr);
	if (rc != 0) {
		LERROR("pthread_attr_init failed for [%s] service\n",
		       opt.o_mnt);
		free(data->hai);
		free(data);
		return -rc;
	}

	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

	rc = pthread_create(&thread, &attr, process_thread, data);
	if (rc != 0)
		LERROR("cannot create thread for [%s] service\n",
		       opt.o_mnt);

	pthread_attr_destroy(&attr);
	return 0;
}

static int hsm_action_handle(void)
{
	struct hsm_action_list *hal;
	struct hsm_action_item *hai;
	int msgsize;
	int i = 0;
	int rc;

	LDEBUG("waiting for message from kernel\n");
	rc = llapi_hsm_copytool_recv(ctdata, &hal, &msgsize);
	if (rc == -ESHUTDOWN) {
		return rc;
	} else if (rc < 0) {
		LERROR("cannot receive action list: %s\n", strerror(-rc));
		err_major++;
		return rc;
	}

	LDEBUG("copytool fs=%s archive#=%d item_count=%d",
	       hal->hal_fsname, hal->hal_archive_id, hal->hal_count);

	if (strcmp(hal->hal_fsname, fs_name) != 0) {
		rc = -EINVAL;
		LERROR("invalid fs name [%s], expecting [%s]\n",
		       hal->hal_fsname, fs_name);
		err_major++;
		return rc;
	}

	hai = hai_first(hal);
	while (++i <= hal->hal_count) {
		if ((char *)hai - (char *)hal > msgsize) {
			rc = -EPROTO;
			LERROR("item [%d] of file system [%s] past end of message!\n",
			       i, opt.o_mnt);
			err_major++;
			return rc;
		}
		rc = process_item_async(hai, hal->hal_flags);
		if (rc < 0)
			LERROR("failed to process item [%d] of file system [%s]\n",
			       i, opt.o_mnt);
		hai = hai_next(hai);
	}
	return 0;
}

static int setup(void)
{
	int rc;

	debug_level = DEBUG;

	rc = llapi_search_fsname(opt.o_mnt, fs_name);
	if (rc < 0) {
		LERROR("cannot find a Lustre filesystem mounted at [%s]\n",
		       opt.o_mnt);
		return rc;
	}

	opt.o_mnt_fd = open(opt.o_mnt, O_RDONLY);
	if (opt.o_mnt_fd < 0) {
		rc = -errno;
		LERROR("cannot open mount point at [%s]\n", opt.o_mnt,
		       strerror(errno));
		return rc;
	}
	return 0;
}

static int cleanup(void)
{
	int rc;

	if (opt.o_mnt_fd >= 0) {
		rc = close(opt.o_mnt_fd);
		if (rc < 0) {
			rc = -errno;
			LERROR("cannot close mount point [%s]: %s\n",
			       opt.o_mnt, strerror(errno));
			return rc;
		}
	}

	if (opt.o_archive_id_cnt > 0) {
		free(opt.o_archive_id);
		opt.o_archive_id = NULL;
		opt.o_archive_id_cnt = 0;
	}

	return 0;
}

static int start_copytool(void)
{
	struct sigaction cleanup_sigaction;
	int rc;

	if (opt.o_daemonize) {
		rc = daemon(1, 1);
		if (rc < 0) {
			rc = -errno;
			LERROR("cannot daemonize: %s\n", strerror(-rc));
			return rc;
		}
	}

	rc = llapi_hsm_copytool_register(&ctdata, opt.o_mnt,
					 opt.o_archive_id_used,
					 opt.o_archive_id, 0);
	if (rc < 0) {
		LERROR("failed to register copytool: %s\n", strerror(-rc));
		if (rc == -ENXIO) {
			/* HSM coordinator thread might not be running */
			LERROR("HSM feature might not be enabled which can be started by running following command on all MDTs of this file system:\nlctl set_param mdt.%s-MDT${INDEX}.hsm_control=enabled\n",
			       fs_name);
		}
		return rc;
	}

	memset(&cleanup_sigaction, 0, sizeof(cleanup_sigaction));
	cleanup_sigaction.sa_handler = handler;
	sigemptyset(&cleanup_sigaction.sa_mask);
	sigaction(SIGINT, &cleanup_sigaction, NULL);
	sigaction(SIGTERM, &cleanup_sigaction, NULL);

	while (1) {
		rc = hsm_action_handle();
		if (rc == -ESHUTDOWN) {
			LINFO("shutting down\n");
			break;
		} else if (rc < 0) {
			LERROR("failed to handle action: %s\n",
			       strerror(-rc));
			if (opt.o_abort_on_error && err_major)
				break;
		}

		if (exiting) {
			LINFO("exiting\n");
			return 0;
		}
	}

	rc = llapi_hsm_copytool_unregister(&ctdata);
	if (rc < 0) {
		LERROR("failed to unregister copytool\n");
		return rc;
	}
	return rc;
}

static void usage(const char *prog, int rc)
{
	fprintf(stderr,
		"Usage: %s [OPTION] <source> <dest>\n"
		"  options:\n"
		"    -h|--help  print this help\n"
		"    -i|--identity <archive_id>   set the ID(s)\n"
		"    --daemon   daemonize this copytool\n"
		"\n"
		"  source: source Lustre mount point or fsname\n"
		"  dest: target Lustre mount point or fsname\n"
		"  archive_id: integer archive ID\n",
		prog);
	exit(rc);
}


static int parse_option_archive(const char *opt_string)
{
	int i;
	char *end = NULL;
	int val = strtol(optarg, &end, 10);

	if (*end != '\0') {
		LERROR("invalid archive-id [%s]\n", optarg);
		return -EINVAL;
	}

	/* if archiveID is zero, any archiveID is accepted */
	if (opt.o_all_id)
		return 0;

	if (val == 0) {
		free(opt.o_archive_id);
		opt.o_archive_id = NULL;
		opt.o_archive_id_cnt = 0;
		opt.o_archive_id_used = 0;
		opt.o_all_id = true;
		LINFO("archive-id = 0 is found, any backend will be served\n");
		return 0;
	}

	/* skip the duplicated id */
	for (i = 0; i < opt.o_archive_id_used; i++) {
		if (opt.o_archive_id[i] == val)
			return 0;
	}

	/* extend the space */
	if (opt.o_archive_id_used >= opt.o_archive_id_cnt) {
		int *tmp;

		opt.o_archive_id_cnt *= 2;
		tmp = realloc(opt.o_archive_id, sizeof(*opt.o_archive_id) *
							opt.o_archive_id_cnt);
		if (tmp == NULL)
			return -ENOMEM;

		opt.o_archive_id = tmp;
	}

	opt.o_archive_id[opt.o_archive_id_used++] = val;
	return 0;
}

int main(int argc, char *const argv[])
{
	struct option long_opts[] = {
		{"identity",	required_argument,	NULL,	'i'},
		{"help",	no_argument,		NULL,	'h'},
		{"daemon",	no_argument, &opt.o_daemonize,	1},
		{0, 0, 0, 0}
	};
	int rc;
	int c;
	char *lustre;
	char hsm_buffer[PATH_MAX];
	char buffer[PATH_MAX];

	while ((c = getopt_long(argc, argv, "hi:",
				long_opts, NULL)) != -1) {
		switch (c) {
		case 'i':
			rc = parse_option_archive(optarg);
			if (rc) {
				LERROR("failed to parse archive option [%s]\n",
				       optarg);
				return rc;
			}
			break;
		case 'h':
			usage(argv[0], 0);
		case 0:
			break;
		default:
			LERROR("unknown option\n");
			usage(argv[0], -EINVAL);
		}
	}

	if (argc != optind + 2) {
		rc = -EINVAL;
		LERROR("must specify source and dest Lustre file systems\n");
		usage(argv[0], rc);
	}

	lustre = argv[optind];
	if (lustre[0] == '/') {
		opt.o_hsm_root = lustre;
	} else {
		opt.o_hsm_root = hsm_buffer;
		rc = llapi_search_rootpath(hsm_buffer, lustre);
		if (rc) {
			LERROR("failed to find root path of Lustre file system [%s]",
			       lustre);
			return rc;
		}
	}
	optind++;

	lustre = argv[optind];
	if (lustre[0] == '/') {
		opt.o_mnt = lustre;
	} else {
		opt.o_mnt = buffer;
		rc = llapi_search_rootpath(buffer, lustre);
		if (rc) {
			LERROR("failed to find root path of Lustre file system [%s]",
			       lustre);
			return rc;
		}
	}

	opt.o_mnt_fd = -1;

	if (opt.o_hsm_root == NULL) {
		LERROR("must specify a root directory for the backend using option [-b]\n");
		rc = -EINVAL;
		usage(argv[0], rc);
	}

	rc = setup();
	if (rc) {
		LERROR("failed to setup\n");
		goto out_cleanup;
	}

	rc = start_copytool();
	if (rc < 0) {
		LERROR("failed to start copytool\n");
		return rc;
	}
out_cleanup:
	cleanup();
	return 0;
}


