TAG=bhvrops/dynamodb-bnr

.PHONY: all
all: build

.PHONY: build
build:
@docker build --tag=$(TAG) .

.PHONY: build-py3
build-py3:
@docker build --tag=$(TAG) -f Dockerfile-py3 .

.PHONY: clean
clean:
	find -name "__pycache__" | xargs rm -rf
	find -name "*.pyc" | xargs rm -rf

.PHONY: install
install:
	@python ./setup.py install

.PHONY: install-py3
install-py3:
	@python3 ./setup.py install
