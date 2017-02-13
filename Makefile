all: build

build:
	@docker build --tag=bhvrops/dynamodb-bnr .

