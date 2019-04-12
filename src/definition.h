/*
 * Copyright (c) 2019, DDN Storage Corporation.
 */
/*
 *
 * Definitions shared by Python and C programs
 *
 * Author: Li Xi <lixi@ddn.com>
 */

#ifndef _LOND_DEFINITION_H_
#define _LOND_DEFINITION_H_

#include <stdbool.h>
#include <sys/types.h>
#include <getopt.h>

#define ARG_TYPE_NONE		"ARG_TYPE_NONE"
#define ARG_TYPE_UNKNOWN	"ARG_TYPE_UNKNOWN"
#define ARG_TYPE_DIR_PATH	"ARG_TYPE_DIR_PATH"
#define ARG_TYPE_FILE_PATH	"ARG_TYPE_FILE_PATH"

struct command_argument {
	/* If ARG_TYPE_NONE, this arugment doesn't exist */
	const char *ca_type;
};

struct command_options {
	const char *co_name;
	struct option co_options[64];
	struct command_argument co_arguments[4];
};

enum {
	OPT_PROGNAME = 3,
};

#define LOND_OPTION_PROGNAME	"progname"

#define LOND_FETCH_OPTIONS {						\
	{ .val = OPT_PROGNAME,	.name = LOND_OPTION_PROGNAME,		\
	  .has_arg = required_argument },				\
	{ .val = 'h',	.name = "help",					\
	  .has_arg = no_argument },					\
	{ .name = NULL }						\
}

#define LOND_UNLOCK_OPTIONS {						\
	{ .val = OPT_PROGNAME,	.name = LOND_OPTION_PROGNAME,		\
	  .has_arg = required_argument },				\
	{ .val = 'h',	.name = "help",					\
	  .has_arg = no_argument },					\
	{ .val = 'd',	.name = "directory",				\
	  .has_arg = no_argument },					\
	{ .val = 'k',	.name = "key",					\
	  .has_arg = required_argument },				\
	{ .name = NULL }						\
}

#define LOND_STAT_OPTIONS {						\
	{ .val = OPT_PROGNAME,	.name = LOND_OPTION_PROGNAME,		\
	  .has_arg = required_argument },				\
	{ .val = 'h',	.name = "help",					\
	  .has_arg = no_argument },					\
	{ .val = 'd',	.name = "directory",				\
	  .has_arg = no_argument },					\
	{ .name = NULL }						\
}

#define ALL_COMMANDS {							\
	{ .co_name = "LOND_FETCH_OPTIONS",				\
	  .co_options = LOND_FETCH_OPTIONS,				\
	  .co_arguments = {						\
	     { .ca_type = ARG_TYPE_DIR_PATH },				\
	     { .ca_type = ARG_TYPE_NONE }				\
	  }								\
	},								\
	{ .co_name = "LOND_UNLOCK_OPTIONS",				\
	  .co_options = LOND_UNLOCK_OPTIONS,				\
	  .co_arguments = {						\
	     { .ca_type = ARG_TYPE_FILE_PATH },				\
	     { .ca_type = ARG_TYPE_NONE }				\
	  }								\
	},								\
	{ .co_name = "LOND_STAT_OPTIONS",				\
	  .co_options = LOND_STAT_OPTIONS,				\
	  .co_arguments = {						\
	     { .ca_type = ARG_TYPE_FILE_PATH },				\
	     { .ca_type = ARG_TYPE_NONE }				\
	  }								\
	},								\
}

#endif /* _LOND_DEFINITION_H_ */
