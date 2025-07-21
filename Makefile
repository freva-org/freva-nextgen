.PHONY: all release docs coverage
all: install

install: prepare
	python3 -m pip install -e ./freva-rest[dev,tests] -e ./freva-client -e ./freva-data-portal-worker[full]

prepare:
	python3 -m pip install cryptography tox
	mkdir -p dev-env/certs
	python3 dev-env/config/dev-utils.py gen-certs

coverage:
	python -m pytest -vv \
		--cov=freva-rest \
		--cov=freva-client \
		--cov=freva-data-portal-worker \
		--cov-report=html:coverage_report \
		--junitxml report.xml \
		--cov-report xml tests/
	python -m coverage report --fail-under=98.5 --precision=2

lint: types
	tox -e lint

types:
	tox -e types

docs:
	tox -e docs

release:
	tox -e release
