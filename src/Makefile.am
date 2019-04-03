AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}

AM_CFLAGS = -Wall -Werror -g $(json_c_CFLAGS) $(json_c_LIBS) \
	-llustreapi -lpthread

bin_PROGRAMS = lond lond_copytool

lond_SOURCES = lond.c lond.h debug.c debug.h cmd.c cmd.h
lond_copytool_SOURCES = lond_copytool.c lond.h debug.c debug.h

C_FILES = $(wildcard *.c *.h)
C_CHECKS = $(C_FILES:%=%.c_checked)
CHECKS = $(C_CHECKS)

%.c_checked: % ../c_check.pl
	../c_check.pl -f $<
	touch $@

check_clean-local:
	rm -f $(CHECKS)

check-local: $(CHECKS)

all: all-am $(CHECKS)
