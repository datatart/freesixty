default: tests

tests: requirements
	py.test tests

requirements:
	pip install -r requirements.txt

.PHONY: default