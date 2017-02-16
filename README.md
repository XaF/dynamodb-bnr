dynamodb-bnr
============

dynamodb backup'n'restore python script with tarfile management

## Description

`dynamodb-bnr`, or DynamoDB backup'n'restore, is a Python script that allows to backup and restore DynamoDB tables to and from directories or tar archives (with or without compression, supported formats are `tar`, `tar.gz` and `tar.bz2`). The backups can also be automatically sent to S3 buckets.

A Dockerfile is provided to run `dynamodb-bnr` from a docker container. The `dynamodb-bnr` docker container can also be obtained [from the docker hub][dockerhub] using `docker pull bhvrops/dynamodb-bnr`

## Requirements

`dynamodb-bnr` requirements are currently as follow:
  - Python 2.7 or 3.5
  - `boto3`
  - `pyopenssl`

## AWS Errors management

When making calls to AWS API, some errors can happen. Here is a list of the errors reported on the AWS API Reference for DynamoDB, and a description of how they are handled by `dynamodb-bnr`.

### [ListTables][aws_errors:listtables]
 - **InternalServerError:** script fails and exit != 0

### [CreateTable][aws_errors:createtable]
  - **InternalServerError:** script fails and exit != 0
  - **LimitExceededException:** wait for 15*(current_retry+1) secondes before retrying, max retry = 5 (default values), if not solved after max retry, script fails and exit != 0
  - **ResourceInUseException:** wait for 10*(current_retry+1) secondes before retrying, max retry = 5 (default values), if not solved after max retry, script fails and exit != 0

### [DeleteTable][aws_errors:deletetable]
  - **InternalServerError:** script fails and exit != 0
  - **LimitExceededException:** wait for 15*(current_retry+1) secondes before retrying, max retry = 5 (default values), if not solved after max retry, script fails and exit != 0
  - **ResourceInUseException:** wait for 10*(current_retry+1) secondes before retrying, max retry = 5 (default values), if not solved after max retry, script fails and exit != 0
  - **ResourceNotFoundException:** already deleted, ignore

### [DescribeTable][aws_errors:describetable]
  - **InternalServerError:** script fails and exit != 0
  - **ResourceNotFoundException**
    - _for backup:_ should not happen (describe is used to get the table structure on a table that has been found with ListTables ), script fails and exit != 0
    - _when creating table:_ should not happen (describe is used to verify that the table is not anymore in 'CREATING' mode and becomes 'ACTIVE'), script fails and exit != 0
    - _when deleting table:_ should happen (describe is used to verify that the table is not anymore in 'DELETING' mode and becomes actually deleted), ignore

### [Scan][aws_errors:scan]
  - **InternalServerError:** script fails and exit != 0
  - **ProvisionedThroughputExceededException:** wait for 10*(current_retry+1) secondes before retrying, max retry = 5 (default values), if not solved after max retry, script fails and exit != 0
  - **ResourceNotFoundException:** should not happen (scan only tables found with ListTables), script fails and exit != 0

### [BatchWriteItem][aws_errors:batchwriteitem]

  - **InternalServerError:** script fails and exit != 0
  - **ItemCollectionSizeLimitExceededException:** should not happen as the maximum batch write is set to the current AWS API maximum of 25, script fails and exit != 0
  - **ProvisionedThroughputExceededException:** wait for 10*(current_retry+1) secondes before retrying, max retry = 5 (default values)
  - **ResourceNotFoundException:** should not happen (Table is deleted and created just before running the batch write), script fails and exit != 0


[dockerhub]: https://hub.docker.com/r/bhvrops/dynamodb-bnr/
[aws_errors:listtables]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_ListTables.html#API_ListTables_Errors
[aws_errors:createtable]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_CreateTable.html#API_CreateTable_Errors
[aws_errors:deletetable]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DeleteTable.html#API_DeleteTable_Errors
[aws_errors:describetable]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeTable.html#API_DescribeTable_Errors
[aws_errors:scan]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html#API_Scan_Errors
[aws_errors:batchwriteitem]: http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html#API_BatchWriteItem_Errors
