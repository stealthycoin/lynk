TESTS=tests/unit tests/functional

.PHONY: htmlcov

test:
	py.test --cov lynk \
	        --cov-report term-missing \
                --cov-fail-under 95 $(TESTS)

check:
	flake8 --ignore=E731,W503 \
               --exclude src/lynk/__init__.py \
               --max-complexity 10 src/lynk/
	flake8 tests/unit/ tests/functional/ tests/integration

htmlcov:
	py.test --cov lynk --cov-report html $(TESTS)
	rm -rf /tmp/htmlcov && mv htmlcov /tmp/
	open /tmp/htmlcov/index.html
