PYTHON_FILES = $(filter-out copytoold_pb2.py, $(wildcard *.py))
PYTHON_CHECKS = $(PYTHON_FILES:%=%.python_checked)
CHECKS = $(PYTHON_CHECKS)

EXTRA_DIST = \
	$(PYTHON_FILES) \
	copytoold.proto

copytoold_pb2.py: copytoold.proto
	protoc --python_out . copytoold.proto

%.python_checked: % ../.pylintrc
	PYLINTRC=../.pylintrc pylint --disable=I $<
	pep8 --ignore=E501 $<
	touch $@

check_clean-local:
	rm -f $(CHECKS)

check-local: $(CHECKS)

all: all-am copytoold_pb2.py $(CHECKS)
