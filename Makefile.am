SUBDIRS = src pylond pylcommon .

build_dir = `pwd`/build
rpmbuild_opt =

AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}

PYTHON_COMMANDS = \
	lod \
	lond \
	lond_copytoold

dist_bin_SCRIPTS = lond
dist_sbin_SCRIPTS = lond_copytoold

EXTRA_DIST = \
    $(PYTHON_COMMANDS) \
	c_check.pl \
	detect-distro.sh \
	README \
	lond.spec \
	lond.spec.in \
	systemd/* \
	lod_slurm/* \
	.pylintrc

CHECKS = $(PYTHON_COMMANDS:%=%.python_checked)

%.python_checked: % .pylintrc
	PYLINTRC=.pylintrc pylint --disable=I $<
	pep8 --ignore=E501 $<
	touch $@

check_clean-local:
	rm -f $(CHECKS)

check-local: $(CHECKS)

check:
	make check_clean
	make

all: all-am $(CHECKS)

# Clean up all the generated files that are ignored in the source repo
#
mrproper: maintainer-clean
	rm -f Makefile.in aclocal.m4 configure
	rm -f compile depcomp install-sh missing

LOND_RPM = build/RPMS/x86_64/lond-$(PACKAGE_VERSION)-$(LOND_RELEASE).el$(DISTRO_RELEASE)*.x86_64.rpm

rpms: lond.spec dist
	mkdir -p $(build_dir)/BUILD $(build_dir)/SPECS $(build_dir)/SRPMS $(build_dir)/RPMS \
		&& rpmbuild $(rpmbuild_opt) --define="_topdir $(build_dir)" \
			--define="_prefix $(prefix)" -tb $(distdir).tar.gz \
		&& echo "RPMs successfully generated in $(build_dir)/RPMS"
