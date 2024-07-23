.PHONY: all release
all: install

install: prepare
	python3 -m pip install -e ./freva-rest[dev] -e ./freva-client -e ./freva-data-portal-worker[full]

prepare:
	python3 -m pip install cryptography tox
	mkdir -p dev-env/certs
	python3 run_server.py --gen-certs

lint:
	tox -e lint types

docs: tox -e docs

release: tox -e release
