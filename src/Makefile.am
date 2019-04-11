AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}

AM_CFLAGS = -Wall -Werror -g $(json_c_CFLAGS) $(json_c_LIBS) \
	-llustreapi -lpthread

sbin_PROGRAMS = lond_copytool
bin_PROGRAMS = lond_fetch lond_unlock
noinst_PROGRAMS = generate_definition

lond_fetch_SOURCES = lond.h lond_common.c lond_fetch.c debug.c debug.h cmd.c \
	cmd.h definition.h
lond_unlock_SOURCES = lond.h lond_common.c lond_unlock.c debug.c debug.h cmd.c \
	cmd.h definition.h 
lond_copytool_SOURCES = lond_copytool.c lond.h debug.c debug.h \
	definition.h
generate_definition_SOURCES = generate_definition.c debug.c debug.h \
        definition.h

../pylond/definition.py: generate_definition
	./generate_definition ../pylond/definition.py

C_FILES = $(wildcard *.c *.h)
C_CHECKS = $(C_FILES:%=%.c_checked)
CHECKS = $(C_CHECKS)

%.c_checked: % ../c_check.pl
	../c_check.pl -f $<
	touch $@

check_clean-local:
	rm -f $(CHECKS)

check-local: $(CHECKS)

all: all-am $(CHECKS) ../pylond/definition.py
