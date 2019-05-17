
install-python:
	apt install python3.7 python3.7-dev
	apt install python3-pip

install-dependencies:
	python3 -m pip install pipenv
	python3 -m pipenv install

install-dev-dependencies:
	python3 -m pipenv install --dev

test:
	python -m pytest

coverage:
	python -m pytest --cov=converter --cov-report term-missing --cov-fail-under=100 tests/

run:
	FLASK_APP=converter.core python -m flask run --port 3003
