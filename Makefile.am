SUBDIRS = src .

build_dir = `pwd`/build
rpmbuild_opt =

AUTOMAKE_OPTIONS = -Wall foreign
ACLOCAL_AMFLAGS = ${ALOCAL_FLAGS}

EXTRA_DIST= \
	detect-distro.sh \
	lond.spec \
	lond.spec.in \
	c_check.pl

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
