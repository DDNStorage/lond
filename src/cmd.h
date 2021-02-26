/*
 *
 * Head file for running command
 *
 * Author: Li Xi <lixi@ddn.com>
 */

#ifndef _LOND_CMD_H_
#define _LOND_CMD_H_
int command_run(char *cmd, int cmdsz);
int command_read(char *cmd, char *buf, int len);
#endif /* _LOND_CMD_H_ */
