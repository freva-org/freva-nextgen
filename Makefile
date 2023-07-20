.PHONY: docs

test:
	poetry run mypy --install-types --non-interactive
	poetry run black --check -l 79 -t py311 src
	poetry run python3 -m pytest -vv \
	    --cov=$(PWD)/src --cov-report=html:coverage_report \
	    --junitxml coverage.xml --cov-report xml \
		$(PWD)/src/databrowser/tests
	poetry run python3 -m coverage report

docs:
	python3 -m pip install -e .[docs]
	make -C docs clean
	make -C docs html
