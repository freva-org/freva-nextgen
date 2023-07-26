.PHONY: docs

test:
	poetry run mypy --install-types --non-interactive
	poetry run isort --check --profile black -t py311 -l 79 src
	poetry run flake8 src/databrowser --count --exit-zero --select=E9,F63,F7,F82 --show-source --statistics
	poetry run flake8 src/databrowser --count --exit-zero --max-complexity=8 --max-line-length=88 --statistics
	poetry run python3 -m pytest -vv \
	    --cov=$(PWD)/src --cov-report=html:coverage_report \
	    --junitxml report.xml --cov-report xml \
		$(PWD)/src/databrowser/tests
	poetry run python3 -m coverage report

docs:
	make -C docs clean
	make -C docs html
