/*
 * Copyright (c) 2016, DDN Storage Corporation.
 */
/*
 *
 * Tool for scanning the MDT and print list of matched files.
 *
 * Author: Li Xi <lixi@ddn.com>
 */

#include "debug.h"

int debug_level = INFO;
FILE *debug_log;
FILE *error_log;
FILE *info_log;
