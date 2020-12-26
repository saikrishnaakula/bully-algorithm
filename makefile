PYTHON = python3

clean-pyc:
	find . -name '*.pyc' -exec rm --force {} +
	find . -name '*.pyo' -exec rm --force {} +
	name '*~' -exec rm --force  {}

build:
	pip3 install -r requirements.txt

run-node:
	py testNode.py $(node) 

test:
	py deployment.py $(nodes)