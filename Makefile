MODULE_NAME=mara_pipelines


all:
	# builds virtual env. and starts install in it
	make .venv/bin/python
	make install


install:
	# install of module
	.venv/bin/pip install .


test:
	make .venv/bin/python
	# test of module
	.venv/bin/pip install .[test]
	.venv/bin/pytest


publish:
	# manually publishing the package
	.venv/bin/pip install build twine
	.venv/bin/python -m build
	.venv/bin/twine upload dist/*


clean:
	# clean up
	rm -rf .venv/ build/ dist/ ${MODULE_NAME}.egg-info/ .pytest_cache/ .eggs/


.PYTHON3:=$(shell PATH='$(subst $(CURDIR)/.venv/bin:,,$(PATH))' which python3)

.venv/bin/python:
	mkdir -p .venv
	cd .venv && $(.PYTHON3) -m venv --copies --prompt='[$(shell basename `pwd`)/.venv]' .

	.venv/bin/python -m pip install --upgrade pip