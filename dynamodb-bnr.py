#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# dynamodb-bnr - DynamoDB backup'n'restore python script
#                with tarfile management
#
# Raphaël Beamonte <raphael.beamonte@bhvr.com>
#

import boto3
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError
import click
# import datetime
import fnmatch
import json
import logging
import multiprocessing
import OpenSSL
import os
import shutil
import StringIO
import sys
import tarfile
import time


class Namespace(object):
    def __init__(self, adict = None):
        if adict is None:
            adict = {}
        self.__dict__.update(adict)

    def update(self, adict):
        self.__dict__.update(adict)

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def __str__(self):
        return str(self.__dict__)

# class DateTimeEncoder(json.JSONEncoder):
#    def default(self, o):
#        if isinstance(o, datetime.datetime):
#            return o.isoformat()
#
#        return json.JSONEncoder.default(self, o)


const_parameters = Namespace({
    'json_indent': 2,
    'schema_file': 'schema.json',
    'data_dir': 'data',
    'throughputexceeded_sleeptime':
        os.getenv('DYNAMODB_BNR_THROUGHPUTEXCEEDED_SLEEPTIME', 10),
    'throughputexceeded_maxretry':
        os.getenv('DYNAMODB_BNR_THROUGHPUTEXCEEDED_MAXRETRY', 5),
    'resourceinuse_sleeptime':
        os.getenv('DYNAMODB_BNR_RESOURCEINUSE_SLEEPTIME', 10),
    'resourceinuse_maxretry':
        os.getenv('DYNAMODB_BNR_RESOURCEINUSE_MAXRETRY', 5),
    'limitexceeded_sleeptime':
        os.getenv('DYNAMODB_BNR_LIMITEXCEEDED_SLEEPTIME', 15),
    'limitexceeded_maxretry':
        os.getenv('DYNAMODB_BNR_LIMITEXCEEDED_MAXRETRY', 5),
    'tableoperation_sleeptime':
        os.getenv('DYNAMODB_BNR_TABLEOPERATION_SLEEPTIME', 5),
    'dynamodb_max_batch_write':
        os.getenv('DYNAMODB_BNR_MAX_BATCH_WRITE', 25),
    'opensslerror_maxretry':
        os.getenv('DYNAMODB_BNR_OPENSSLERROR_MAXRETRY', 5),
})
_global_client_dynamodb = None
_global_client_s3 = None


def get_client_dynamodb():
    global _global_client_dynamodb
    if _global_client_dynamodb is None:
        if parameters.ddb_profile:
            session = boto3.Session(profile_name=parameters.ddb_profile)
        else:
            session = boto3.Session(
                region_name=parameters.ddb_region,
                aws_access_key_id=parameters.ddb_accesskey,
                aws_secret_access_key=parameters.ddb_secretkey,
            )
        _global_client_dynamodb = session.client(
            service_name='dynamodb'
        )
    return _global_client_dynamodb


def get_client_s3():
    global _global_client_s3
    if _global_client_s3 is None:
        if parameters.s3_profile:
            session = boto3.Session(profile_name=parameters.s3_profile)
        else:
            session = boto3.Session(
                region_name=parameters.s3_region,
                aws_access_key_id=parameters.s3_accesskey,
                aws_secret_access_key=parameters.s3_secretkey,
            )
        _global_client_s3 = session.client(
            service_name='s3'
        )
    return _global_client_s3


def tar_type(path):
    tar_type = None
    if not os.path.isdir(path):
        if fnmatch.fnmatch(path, '*.tar.gz') or fnmatch.fnmatch(path, '*.tgz'):
            tar_type = 'w:gz'
        elif fnmatch.fnmatch(path, '*.tar.bz2') or fnmatch.fnmatch(path, '*.tbz2'):
            tar_type = 'w:bz2'
        elif fnmatch.fnmatch(path, '*.tar'):
            tar_type = 'w:'
    return tar_type


def is_tarfile(path):
    return os.path.isfile(path) and tarfile.is_tarfile(path)


class TarFileWriter(multiprocessing.Process):
    def __init__(self, path, tarwrite_queue):
        multiprocessing.Process.__init__(self, name='TarFileWriter')
        self.tarwrite_queue = tarwrite_queue
        self.path = path

        self.tar_type = tar_type(self.path)

    def run(self):
        if self.tar_type is None:
            return

        tar = tarfile.open(self.path, mode=self.tar_type)
        logger.info('Starting')

        next_write = self.tarwrite_queue.get()
        while next_write is not None:
            logger.debug('Treating {}'.format(next_write))
            tar.addfile(**next_write)
            self.tarwrite_queue.task_done()

            next_write = self.tarwrite_queue.get()

        logger.info('Exiting')
        self.tarwrite_queue.task_done()
        tar.close()


@click.group()
@click.option('--debug/--no-debug', '-d', default=False,
              help='Activate debug output')
@click.option('--loglevel', default='INFO',
              type=click.Choice([
                'NOTSET', 'DEBUG', 'INFO',
                'WARNING', 'ERROR', 'CRITICAL'
              ]),
              help='Define the specific log level')
@click.option('--logfile', default=None)
@click.option('--profile',
              default=os.getenv('AWS_DEFAULT_PROFILE', None),
              help='Define the AWS profile name (defaults to the environment '
                   'variable AWS_DEFAULT_PROFILE)')
@click.option('--accessKey',
              default=os.getenv('AWS_ACCESS_KEY_ID', None),
              help='Define the AWS default access key (defaults to the '
                   'environment variable AWS_ACCESS_KEY_ID)')
@click.option('--secretKey',
              default=os.getenv('AWS_SECRET_ACCESS_KEY', None),
              help='Define the AWS default secret key (defaults to the '
                   'environment variable AWS_SECRET_ACCESS_KEY)')
@click.option('--region',
              default=os.getenv('REGION', None),
              help='Define the AWS default region (defaults to the environment '
                   'variable REGION)')
@click.option('--ddb-profile',
              default=os.getenv('DDB_AWS_DEFAULT_PROFILE', None),
              help='Define the AWS DynamoDB profile name')
@click.option('--ddb-accessKey',
              default=os.getenv('DDB_AWS_ACCESS_KEY_ID', None),
              help='Define the AWS DynamoDB access key')
@click.option('--ddb-secretKey',
              default=os.getenv('DDB_AWS_SECRET_ACCESS_KEY', None),
              help='Define the AWS DynamoDB secret key')
@click.option('--ddb-region',
              default=os.getenv('DDB_REGION', None),
              help='Define the AWS DynamoDB region')
@click.option('--s3-upload/--no-s3-upload', default=False,
              help='Activate S3 upload')
@click.option('--s3-create-bucket/--no-s3-create-bucket', default=False,
              help='Create S3 bucket if it does not exist')
@click.option('--s3-profile',
              default=os.getenv('S3_AWS_DEFAULT_PROFILE', None),
              help='Define the AWS S3 profile name')
@click.option('--s3-accessKey',
              default=os.getenv('S3_AWS_ACCESS_KEY_ID', None),
              help='Define the AWS S3 access key')
@click.option('--s3-secretKey',
              default=os.getenv('S3_AWS_SECRET_ACCESS_KEY', None),
              help='Define the AWS S3 secret key')
@click.option('--s3-region',
              default=os.getenv('S3_REGION', None),
              help='Define the AWS S3 region')
@click.option('--s3-bucket',
              default=os.getenv('S3_BUCKET', None),
              help='Define the AWS S3 bucket')
@click.option('--table',
              default='*',
              help='The table to backup or restore (\'*\' means all tables, '
                   '\'t*\' means all tables starting with \'t\')')
@click.option('--dumpPath',
              default=None,
              help='The path of the dump directory')
@click.pass_context
def cli(ctx, **kwargs):
    ctx.obj.update(kwargs)

    # Set up amazon configuration
    if ctx.obj.accesskey is not None:
        if ctx.obj.ddb_accesskey is None:
            ctx.obj.ddb_accesskey = ctx.obj.accesskey
        if ctx.obj.s3_accesskey is None:
            ctx.obj.s3_accesskey = ctx.obj.accesskey
    if ctx.obj.secretkey is not None:
        if ctx.obj.ddb_secretkey is None:
            ctx.obj.ddb_secretkey = ctx.obj.secretkey
        if ctx.obj.s3_secretkey is None:
            ctx.obj.s3_secretkey = ctx.obj.secretkey
    if ctx.obj.region is not None:
        if ctx.obj.ddb_region is None:
            ctx.obj.ddb_region = ctx.obj.region
        if ctx.obj.s3_region is None:
            ctx.obj.s3_region = ctx.obj.region
    if ctx.obj.profile is not None:
        if ctx.obj.ddb_profile is None:
            ctx.obj.ddb_profile = ctx.obj.profile
        if ctx.obj.s3_profile is None:
            ctx.obj.s3_profile = ctx.obj.profile

    # Check that dynamodb configuration is available
    if ctx.obj.ddb_profile is None and \
            (ctx.obj.ddb_accesskey is None or
             ctx.obj.ddb_secretkey is None or
             ctx.obj.ddb_region is None):
        raise RuntimeError(('DynamoDB configuration is incomplete '
                            '(accessKey? {}, secretKey? {}, region? {}'
                            ') or profile? {})').format(
                                ctx.obj.ddb_accesskey is not None,
                                ctx.obj.ddb_secretkey is not None,
                                ctx.obj.ddb_region is not None,
                                ctx.obj.ddb_profile is not None))

    # Check that s3 configuration is available if needed
    if ctx.obj.s3_upload and \
            (ctx.obj.s3_profile is None and
             (ctx.obj.s3_accesskey is None or
              ctx.obj.s3_region is None or
              ctx.obj.s3_secretkey is None or
              ctx.obj.s3_bucket is None)):
        raise RuntimeError(('S3 configuration is incomplete '
                            '(accessKey? {}, secretKey? {}, region? {}, '
                            'bucket? {}) or profile {}').format(
                                ctx.obj.s3_accesskey is not None,
                                ctx.obj.s3_secretkey is not None,
                                ctx.obj.s3_region is not None,
                                ctx.obj.s3_bucket is not None,
                                ctx.obj.s3_profile is not None))

    log_level = 'DEBUG' if ctx.obj.debug else ctx.obj.loglevel
    if ctx.obj.logfile is None:
        fname = os.path.basename(__file__)
        fsplit = os.path.splitext(fname)
        if fsplit[1] == '.py':
            fname = fsplit[0]
        ctx.obj.logfile = os.path.join(os.getcwd(), '{}.log'.format(fname))

    fh = logging.FileHandler(ctx.obj.logfile)
    ch = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s::%(name)s::%(processName)s'
                                  '::%(levelname)s::%(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    log_level_value = getattr(logging, log_level)
    logger.setLevel(log_level_value)
    fh.setLevel(log_level_value)
    ch.setLevel(log_level_value)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info('Loglevel set to {}'.format(log_level))


def get_dynamo_matching_table_names(table_name_wildcard):
    client_ddb = get_client_dynamodb()

    tables = []
    more_tables = True

    table_list = client_ddb.list_tables()
    while more_tables:
        for table_name in table_list['TableNames']:
            if fnmatch.fnmatch(table_name, table_name_wildcard):
                tables.append(table_name)

        if 'LastEvaluatedTableName' in table_list:
            table_list = client_ddb.list_tables(
                ExclusiveStartTableName=table_list['LastEvaluatedTableName'])
        else:
            more_tables = False

    return tables


def manage_db_scan(client_ddb, **kwargs):
    items_list = None
    throughputexceeded_currentretry = 0
    while items_list is None:
        try:
            items_list = client_ddb.scan(**kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ProvisionedThroughputExceededException' or \
                    throughputexceeded_currentretry >= const_parameters.throughputexceeded_maxretry:
                raise e

            throughputexceeded_currentretry += 1
            sleeptime = const_parameters.throughputexceeded_sleeptime
            sleeptime *= throughputexceeded_currentretry
            logger.info(('Got \'ProvisionedThroughputExceededException\', '
                         'waiting {} seconds before retry').format(sleeptime))
            time.sleep(sleeptime)

    return items_list


def clean_table_schema(table_schema):
    # Delete noise information
    for info in ('TableArn', 'TableSizeBytes', 'TableStatus', 'ItemCount', 'CreationDateTime'):
        if info in table_schema:
            del table_schema[info]
    for info in ('NumberOfDecreasesToday', 'LastIncreaseDateTime', 'LastDecreaseDateTime'):
        if info in table_schema['ProvisionedThroughput']:
            del table_schema['ProvisionedThroughput'][info]
    if 'GlobalSecondaryIndexes' in table_schema:
        for i in xrange(len(table_schema['GlobalSecondaryIndexes'])):
            for info in ('IndexSizeBytes', 'IndexStatus', 'IndexArn', 'ItemCount'):
                if info in table_schema['GlobalSecondaryIndexes'][i]:
                    del table_schema['GlobalSecondaryIndexes'][i][info]
            for info in ('NumberOfDecreasesToday', 'LastIncreaseDateTime', 'LastDecreaseDateTime'):
                if info in table_schema['GlobalSecondaryIndexes'][i]['ProvisionedThroughput']:
                    del table_schema['GlobalSecondaryIndexes'][i]['ProvisionedThroughput'][info]

    return table_schema


def table_backup(table_name):
    client_ddb = get_client_dynamodb()

    logger.info('Starting backup of table \'{}\''.format(table_name))

    # define the table directory
    if parameters.tar_path is not None:
        table_dump_path = os.path.join(parameters.tar_path, table_name)
    else:
        table_dump_path = os.path.join(parameters.dumppath, table_name)

    # Make the data directory if it does not exist
    table_dump_path_data = os.path.join(table_dump_path, const_parameters.data_dir)
    if not parameters.tar_path:
        if parameters.only is None and os.path.isdir(table_dump_path):
            shutil.rmtree(table_dump_path)

        if parameters.only == 'data' and os.path.isdir(table_dump_path_data):
            shutil.rmtree(table_dump_path_data)

        if not os.path.isdir(table_dump_path_data) and \
                (parameters.only is None or parameters.only == 'data'):
            os.makedirs(table_dump_path_data)
        elif not os.path.isdir(table_dump_path):
            os.makedirs(table_dump_path)
    else:
        info = tarfile.TarInfo(table_dump_path)
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        parameters.tarwrite_queue.put({'tarinfo': info})

        info = tarfile.TarInfo(table_dump_path_data)
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        parameters.tarwrite_queue.put({'tarinfo': info})

    # get table schema
    if parameters.only is None or parameters.only == 'schema':
        logger.info(('Backing up table schema '
                     'for table \'{}\'').format(table_name))

        table_schema = None
        current_retry = 0
        while table_schema is None:
            try:
                table_schema = client_ddb.describe_table(TableName=table_name)
                table_schema = table_schema['Table']
            except (OpenSSL.SSL.SysCallError, OpenSSL.SSL.Error) as e:
                if current_retry >= const_parameters.opensslerror_maxretry:
                    raise e

                logger.warning("Got OpenSSL error, retrying...")
                current_retry += 1

        table_schema = clean_table_schema(table_schema)

        jdump = json.dumps(table_schema,
                           # cls=DateTimeEncoder,
                           indent=const_parameters.json_indent)
        fpath = os.path.join(table_dump_path, const_parameters.schema_file)
        if parameters.tar_path is not None:
            f = StringIO.StringIO(jdump)
            info = tarfile.TarInfo(name=fpath)
            info.size = f.len
            info.mode = 0o644
            parameters.tarwrite_queue.put({'tarinfo': info, 'fileobj': f})
        else:
            with open(fpath, 'w+') as f:
                f.write(jdump)

    # get table items
    if parameters.only is None or parameters.only == 'data':
        more_items = 1

        logger.info("Backing up items for table \'{}\'".format(table_name))

        items_list = manage_db_scan(client_ddb, TableName=table_name)
        while more_items and items_list['ScannedCount'] > 0:
            logger.info(('Backing up items for table'
                         ' \'{}\' ({})').format(table_name, more_items))
            jdump = json.dumps(items_list['Items'],
                               # cls=DateTimeEncoder,
                               indent=const_parameters.json_indent)

            fpath = os.path.join(
                table_dump_path_data,
                '{}.json'.format(str(more_items).zfill(10)))
            if parameters.tar_path is not None:
                f = StringIO.StringIO(jdump)
                info = tarfile.TarInfo(name=fpath)
                info.size = f.len
                info.mode = 0o644
                parameters.tarwrite_queue.put({'tarinfo': info, 'fileobj': f})
            else:
                with open(fpath, 'w+') as f:
                    f.write(jdump)

            if 'LastEvaluatedKey' in items_list:
                items_list = manage_db_scan(
                    client_ddb,
                    TableName=table_name,
                    ExclusiveStartKey=items_list['LastEvaluatedKey'])
                more_items += 1
            else:
                more_items = False

    logger.info('Ended backup of table \'{}\''.format(table_name))


def parallel_workers(name, target, tables):
    processes = []
    badReturn = []
    for table_name in tables:
        p = multiprocessing.Process(
            name=name.format(table_name),
            target=target,
            args=(table_name,))
        processes.append(p)
        p.start()

    try:
        for process in processes:
            process.join()
            if process.exitcode:
                badReturn.append(process)
    except KeyboardInterrupt:
        logger.info("Caught KeyboardInterrupt, terminating workers")
        for process in processes:
            process.terminate()
        for process in processes:
            process.join()
        logger.info('All workers terminated')

    return badReturn


@cli.command()
@click.option('--only', default=None, type=click.Choice(['data', 'schema']),
              help='To backup only the data or schema')
@click.pass_context
def backup(ctx, **kwargs):
    ctx.obj.update(kwargs)
    if ctx.obj.dumppath is None:
        ctx.obj.dumppath = os.path.join(
            os.getcwd(),
            'dump-{}.tgz'.format(time.strftime('%Y%m%d%H%M%S')))

    if tar_type(ctx.obj.dumppath) is not None:
        ctx.obj.tar_path = 'dump'
        ctx.obj.tarwrite_queue = multiprocessing.JoinableQueue()
        tarwrite_process = TarFileWriter(ctx.obj.dumppath, ctx.obj.tarwrite_queue)
        tarwrite_process.start()

        info = tarfile.TarInfo(ctx.obj.tar_path)
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        ctx.obj.tarwrite_queue.put({'tarinfo': info})

    tables_to_backup = get_dynamo_matching_table_names(ctx.obj.table)
    if not tables_to_backup:
        logger.error('no tables found for \'{}\''.format(ctx.obj.table))
        sys.exit(1)

    logger.info('The following tables will be backed up: {}'.format(
        ', '.join(tables_to_backup)))
    logger.info('Tables will be backed up in \'{}\''.format(ctx.obj.dumppath))

    badReturn = parallel_workers(
        name='BackupProcess({})',
        target=table_backup,
        tables=tables_to_backup)

    if ctx.obj.tar_path is not None:
        ctx.obj.tarwrite_queue.put(None)
        ctx.obj.tarwrite_queue.join()

    if badReturn:
        nErrors = len(badReturn)
        logger.info('Backup ended with {} error(s)'.format(nErrors))
        raise RuntimeError(('{} error(s) during backup '
                            'for processes: {}').format(
                           nErrors,
                           ', '.join([x.name for x in badReturn])))
    else:
        logger.info('Backup ended without error')

    # Upload to s3 if requested
    if ctx.obj.s3_upload:
        logger.info('Uploading files to S3')
        client_s3 = get_client_s3()

        # Check if bucket exists
        if ctx.obj.s3_create_bucket:
            buckets = client_s3.list_buckets()
            exists = False
            if 'Buckets' in buckets:
                for bucket in buckets['Buckets']:
                    if bucket['Name'] == ctx.obj.s3_bucket:
                        exists = True
                        break

            if not exists:
                logger.info('Creating bucket {} in S3'.format(ctx.obj.s3_bucket))
                client_s3.create_bucket(Bucket=ctx.obj.s3_bucket, ACL='private')

        # Upload content
        if ctx.obj.tar_path is not None:
            s3path = os.path.basename(ctx.obj.dumppath)
            if badReturn:
                s3path = '{}~incomplete'.format(s3path)
            try:
                client_s3.upload_file(ctx.obj.dumppath, ctx.obj.s3_bucket, s3path)
            except S3UploadFailedError as e:
                logger.exception(e)
                raise e
            s3logfname = '{}.log'.format(s3path)
        else:
            dumpdir = os.path.basename(ctx.obj.dumppath)
            dumppathlen = len(ctx.obj.dumppath) + 1
            for path, dirs, files in os.walk(ctx.obj.dumppath):
                for f in files:
                    filepath = os.path.join(path, f)
                    s3path = os.path.join(dumpdir, filepath[dumppathlen:])
                    try:
                        client_s3.upload_file(filepath, ctx.obj.s3_bucket, s3path)
                    except S3UploadFailedError as e:
                        logger.exception(e)
                        raise e
            s3logfname = '{}.log'.format(dumpdir)
        # Upload logfile
        logger.info('Uploading logfile to S3')
        try:
            client_s3.upload_file(ctx.obj.logfile, ctx.obj.s3_bucket, s3logfname)
        except S3UploadFailedError as e:
            logger.exception(e)
            raise e


def get_dump_matching_table_names(table_name_wildcard):
    tables = []
    if not is_tarfile(parameters.dumppath):
        try:
            dir_list = sorted(os.listdir(parameters.dumppath))
        except OSError:
            logger.info("Cannot find \"{}\"".format(parameters.dumppath))
            sys.exit(1)

        for dir_name in dir_list:
            if fnmatch.fnmatch(dir_name, table_name_wildcard):
                tables.append(dir_name)
    else:
        tar = tarfile.open(parameters.dumppath)
        members = sorted(tar.getmembers(), key=lambda x: x.name)
        for member in members:
            if member.isfile() and os.path.basename(member.name) == const_parameters.schema_file:
                    directory_path = os.path.dirname(member.name)
                    if parameters.tar_path is None:
                        parameters.tar_path = os.path.dirname(directory_path)
                    elif parameters.tar_path != os.path.dirname(directory_path):
                        continue
                    table_name = os.path.basename(directory_path)
                    if fnmatch.fnmatch(table_name, table_name_wildcard):
                        tables.append(table_name)

    return tables


def table_delete(client_ddb, table_name):
    logger.info('Deleting table \'{}\''.format(table_name))
    deleted = False
    managedErrors = ['ResourceInUseException', 'LimitExceededException']
    currentRetry = {
        'resourceinuse': 0,
        'limitexceeded': 0,
    }

    while not deleted:
        try:
            table_status = client_ddb.delete_table(TableName=table_name)
            table_status = table_status["TableDescription"]["TableStatus"]
            while True:
                logger.info(('Waiting {} seconds for table \'{}\' '
                             'to be deleted [current status: {}]').format(
                            const_parameters.tableoperation_sleeptime,
                            table_name,
                            table_status))
                time.sleep(const_parameters.tableoperation_sleeptime)
                table_status = client_ddb.describe_table(TableName=table_name)
                table_status = table_status["Table"]["TableStatus"]
        except ClientError as e:
            if e.response['Error']['Code'] == 'ResourceNotFoundException':
                deleted = True
            elif e.response['Error']['Code'] in managedErrors:
                errorcode = e.response['Error']['Code'][:-9].lower()
                currentRetry[errorcode] += 1
                if currentRetry[errorcode] >= const_parameters['{}_maxretry'.format(errorcode)]:
                    raise e
                sleeptime = const_parameters['{}_sleeptime'.format(errorcode)]
                sleeptime = sleeptime * currentRetry[errorcode]
                logger.info("Got \'{}\', waiting {} seconds before retry".format(
                    e.response['Error']['Code'], sleeptime))
                time.sleep(sleeptime)
            else:
                raise e


def table_create(client_ddb, **kwargs):
    table_name = kwargs['TableName']
    logger.info('Creating table \'{}\''.format(table_name))
    created = False
    managedErrors = ['ResourceInUseException', 'LimitExceededException']
    currentRetry = {
        'resourceinuse': 0,
        'limitexceeded': 0,
    }

    while not created:
        try:
            table_status = client_ddb.create_table(**kwargs)["TableDescription"]["TableStatus"]
            while table_status.upper() != 'ACTIVE':
                logger.info(('Waiting {} seconds for table \'{}\' '
                             'to be created [current status: {}]').format(
                            const_parameters.tableoperation_sleeptime,
                            table_name,
                            table_status))
                time.sleep(const_parameters.tableoperation_sleeptime)
                table_status = client_ddb.describe_table(TableName=table_name)
                table_status = table_status["Table"]["TableStatus"]
            created = True
        except ClientError as e:
            if e.response['Error']['Code'] in managedErrors:
                errorcode = e.response['Error']['Code'][:-9].lower()
                currentRetry[errorcode] += 1
                if currentRetry[errorcode] >= const_parameters['{}_maxretry'.format(errorcode)]:
                    raise e
                sleeptime = const_parameters['{}_sleeptime'.format(errorcode)]
                sleeptime = sleeptime * currentRetry[errorcode]
                logger.info("Got \'{}\', waiting {} seconds before retry".format(
                    e.response['Error']['Code'], sleeptime))
                time.sleep(sleeptime)
            else:
                raise e


def table_batch_write(client, table_name, items):
    put_requests = []
    for item in items[:const_parameters.dynamodb_max_batch_write]:
        put_requests.append({
            'PutRequest': {
                'Item': item
            }
        })

    request_items = {
        'RequestItems': {
            table_name: put_requests,
        }
    }

    response = None
    throughputexceeded_currentretry = 0
    while response is None:
        try:
            response = client.batch_write_item(**request_items)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ProvisionedThroughputExceededException' or \
                    throughputexceeded_currentretry >= const_parameters.throughputexceeded_maxretry:
                raise e

            throughputexceeded_currentretry += 1
            sleeptime = const_parameters.throughputexceeded_sleeptime
            sleeptime *= throughputexceeded_currentretry
            logger.info(('Got \'ProvisionedThroughputExceededException\', '
                         'waiting {} seconds before retry').format(sleeptime))
            time.sleep(sleeptime)

    returnedItems = []
    if 'UnprocessedItems' in response and response['UnprocessedItems']:
        for put_request in response['UnprocessedItems'][table_name]:
            item = put_request['PutRequest']['Item']
            returnedItems.append(item)
    items[:const_parameters.dynamodb_max_batch_write] = returnedItems

    return items


def table_restore(table_name):
    client_ddb = get_client_dynamodb()

    # define the table directory
    if is_tarfile(parameters.dumppath):
        tar = tarfile.open(parameters.dumppath)
        table_dump_path = os.path.join(parameters.tar_path, table_name)
        try:
            member = tar.getmember(os.path.join(table_dump_path, const_parameters.schema_file))
            f = tar.extractfile(member)

            table_schema = json.load(f)
        except KeyError as e:
            raise RuntimeError('Schema of table \'{}\' not found'.format(table_name))
    else:
        table_dump_path = os.path.join(parameters.dumppath, table_name)
        if not os.path.isdir(table_dump_path):
            raise RuntimeError('Schema of table \'{}\' not found'.format(table_name))

        with open(os.path.join(table_dump_path, const_parameters.schema_file), 'r') as f:
            table_schema = json.load(f)

    if 'Table' in table_schema:
        table_schema = clean_table_schema(table_schema['Table'])

    table_schema['TableName'] = table_name  # Use the directory name as table name
    table_delete(client_ddb, table_name)
    table_create(client_ddb, **table_schema)

    table_dump_path_data = os.path.join(table_dump_path, const_parameters.data_dir)
    if is_tarfile(parameters.dumppath):
        try:
            member = tar.getmember(table_dump_path_data)
            # Search for the restoration files in the tar
            data_files = []
            for member in tar.getmembers():
                if member.isfile() and \
                        os.sep.join(os.path.split(member.name)[:-1]) == table_dump_path_data and \
                        fnmatch.fnmatch(member.name, '*.json'):
                    data_files.append(os.path.basename(member.name))
            data_files.sort()
        except KeyError as e:
            logger.info('No data to restore for table \'{}\''.format(table_name))
            return
    else:
        if not os.path.isdir(table_dump_path_data):
            logger.info('No data to restore for table \'{}\''.format(table_name))
            return

        logger.info('Starting restoration of the data of table \'{}\''.format(table_name))

        data_files = sorted(os.listdir(table_dump_path_data))

    n_data_files = len(data_files)
    c_data_file = 0
    items = []
    for data_file in data_files:
        c_data_file += 1
        logger.info("Loading items from file {} of {}".format(c_data_file, n_data_files))

        if is_tarfile(parameters.dumppath):
            member = tar.getmember(os.path.join(table_dump_path_data, data_file))
            f = tar.extractfile(member)

            loaded_items = json.load(f)
        else:
            with open(os.path.join(table_dump_path_data, data_file), 'r') as f:
                loaded_items = json.load(f)

        if 'Items' in loaded_items:
            loaded_items = loaded_items['Items']
        items.extend(loaded_items)

        while len(items) >= const_parameters.dynamodb_max_batch_write or \
                (c_data_file == n_data_files and len(items) > 0):
            logger.debug('Current number of items: '.format(len(items)))
            items = table_batch_write(client_ddb, table_name, items)


@cli.command()
@click.pass_context
def restore(ctx, **kwargs):
    ctx.obj.update(kwargs)
    if ctx.obj.dumppath is None:
        ctx.obj.dumppath = os.path.join(os.getcwd(), 'dump')

    logger.info('Looking for tables to restore in \'{}\''.format(
        ctx.obj.dumppath))

    tables_to_restore = get_dump_matching_table_names(ctx.obj.table)
    if not tables_to_restore:
        logger.error('no tables found for \'{}\''.format(ctx.obj.table))
        sys.exit(1)

    logger.info('The following tables will be restored: {}'.format(
        ', '.join(tables_to_restore)))

    badReturn = parallel_workers(
        name='RestoreProcess({})',
        target=table_restore,
        tables=tables_to_restore)
    if badReturn:
        nErrors = len(badReturn)
        logger.info('Restoration ended with {} error(s)'.format(nErrors))
        raise RuntimeError(('{} error(s) during restoration '
                            'for processes: {}').format(
                                nErrors,
                                ', '.join([x.name for x in badReturn])))
    else:
        logger.info('Restoration ended without error')


if __name__ == '__main__':
    logger = logging.getLogger(os.path.basename(__file__))
    parameters = Namespace({'tar_path': None})

    cli(obj=parameters)
