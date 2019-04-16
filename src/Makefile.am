AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}

AM_CFLAGS = -Wall -Werror -g $(json_c_CFLAGS) $(json_c_LIBS) \
	-llustreapi -lpthread

sbin_PROGRAMS = lond_copytool
bin_PROGRAMS = lond_fetch lond_stat lond_sync lond_unlock
noinst_PROGRAMS = generate_definition

GENERAL_SOURCES = cmd.c cmd.h debug.c debug.h definition.h list.h lond.h \
	lond_common.c

lond_copytool_SOURCES = lond_copytool.c $(GENERAL_SOURCES)
lond_fetch_SOURCES = lond_fetch.c $(GENERAL_SOURCES)
lond_stat_SOURCES = lond_stat.c $(GENERAL_SOURCES)
lond_sync_SOURCES = lond_sync.c $(GENERAL_SOURCES)
lond_unlock_SOURCES = lond_unlock.c $(GENERAL_SOURCES)
generate_definition_SOURCES = generate_definition.c $(GENERAL_SOURCES)

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
