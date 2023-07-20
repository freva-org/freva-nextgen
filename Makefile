.PHONY: docs

test:
	mypy --install-types --non-interactive
	black --check -l 79 -t py311 src
	python3 -m pytest -vv \
	    --cov=$(PWD)/src --cov-report=html:coverage_report \
	    --junitxml coverage.xml --cov-report xml \
		$(PWD)/src/databrowser/tests
	python3 -m coverage report

docs:
	python3 -m pip install -e .[docs]
	make -C docs clean
	make -C docs html
