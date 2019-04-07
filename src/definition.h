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

enum {
	OPT_PROGNAME = 3,
};

#define LOND_OPTION_PROGNAME	"progname"

#define FETCH_LONG_OPTIONS {						\
	{ .val = OPT_PROGNAME,	.name = LOND_OPTION_PROGNAME,		\
	  .has_arg = required_argument },				\
	{ .val = 'h',	.name = "help",					\
	  .has_arg = no_argument },					\
	{ .name = NULL }						\
}

#endif /* _LOND_DEFINITION_H_ */
