#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# dynamodb-bnr - DynamoDB backup'n'restore python script
#                with tarfile management
#
# Raphaël Beamonte <raphael.beamonte@bhvr.com>
#

import argparse
import boto3
from botocore.exceptions import ClientError
from boto3.exceptions import S3UploadFailedError
import datetime
import errno
import fnmatch
import json
import logging
import multiprocessing
import OpenSSL
import os
import re
import shutil
from socket import error as SocketError
import sys
import tarfile
import time

try:
    from StringIO import StringIO as TarFileIO
except ImportError:
    from io import BytesIO

    def TarFileIO(s):
        return BytesIO(s.encode("utf-8"))

try:
    xrange
except NameError:
    xrange = range


class Namespace(object):
    def __init__(self, adict = None):
        if adict is not None:
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
        int(os.getenv('DYNAMODB_BNR_THROUGHPUTEXCEEDED_SLEEPTIME', 10)),
    'throughputexceeded_maxretry':
        int(os.getenv('DYNAMODB_BNR_THROUGHPUTEXCEEDED_MAXRETRY', 5)),
    'resourceinuse_sleeptime':
        int(os.getenv('DYNAMODB_BNR_RESOURCEINUSE_SLEEPTIME', 10)),
    'resourceinuse_maxretry':
        int(os.getenv('DYNAMODB_BNR_RESOURCEINUSE_MAXRETRY', 5)),
    'limitexceeded_sleeptime':
        int(os.getenv('DYNAMODB_BNR_LIMITEXCEEDED_SLEEPTIME', 15)),
    'limitexceeded_maxretry':
        int(os.getenv('DYNAMODB_BNR_LIMITEXCEEDED_MAXRETRY', 5)),
    'tableoperation_sleeptime':
        int(os.getenv('DYNAMODB_BNR_TABLEOPERATION_SLEEPTIME', 5)),
    'dynamodb_max_batch_write':
        int(os.getenv('DYNAMODB_BNR_MAX_BATCH_WRITE', 25)),
    'opensslerror_maxretry':
        int(os.getenv('DYNAMODB_BNR_OPENSSLERROR_MAXRETRY', 5)),
    's3_max_delete_objects':
        int(os.getenv('DYNAMODB_BNR_MAX_DELETE_OBJECTS', 1000)),
    'connresetbypeer_sleeptime':
        int(os.getenv('DYNAMODB_BNR_ECONNRESET_MAXRETRY', 15)),
    'connresetbypeer_maxretry':
        int(os.getenv('DYNAMODB_BNR_ECONNRESET_MAXRETRY', 5)),
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
                aws_access_key_id=parameters.ddb_access_key,
                aws_secret_access_key=parameters.ddb_secret_key,
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
                aws_access_key_id=parameters.s3_access_key,
                aws_secret_access_key=parameters.s3_secret_key,
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


def is_dumppath(path):
    return os.path.isdir(path) or tarfile.is_tarfile(path)


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


def cli():
    # Set up amazon configuration
    if parameters.access_key is not None:
        if parameters.ddb_access_key is None:
            parameters.ddb_access_key = parameters.access_key
        if parameters.s3_access_key is None:
            parameters.s3_access_key = parameters.access_key
    if parameters.secret_key is not None:
        if parameters.ddb_secret_key is None:
            parameters.ddb_secret_key = parameters.secret_key
        if parameters.s3_secret_key is None:
            parameters.s3_secret_key = parameters.secret_key
    if parameters.region is not None:
        if parameters.ddb_region is None:
            parameters.ddb_region = parameters.region
        if parameters.s3_region is None:
            parameters.s3_region = parameters.region
    if parameters.profile is not None:
        if parameters.ddb_profile is None:
            parameters.ddb_profile = parameters.profile
        if parameters.s3_profile is None:
            parameters.s3_profile = parameters.profile

    # Check that dynamodb configuration is available
    if parameters.ddb_profile is None and \
            (parameters.ddb_access_key is None or
             parameters.ddb_secret_key is None or
             parameters.ddb_region is None):
        raise RuntimeError(('DynamoDB configuration is incomplete '
                            '(access key? {}, secret key? {}, region? {}'
                            ') or profile? {})').format(
                                parameters.ddb_access_key is not None,
                                parameters.ddb_secret_key is not None,
                                parameters.ddb_region is not None,
                                parameters.ddb_profile is not None))

    # Check that s3 configuration is available if needed
    if parameters.s3 and \
        (parameters.s3_profile is None and
         (parameters.s3_access_key is None or
          parameters.s3_region is None or
          parameters.s3_secret_key is None or
          parameters.s3_bucket is None)):
        raise RuntimeError(('S3 configuration is incomplete '
                            '(access key? {}, secret key? {}, region? {}, '
                            'bucket? {}) or profile {}').format(
                                parameters.s3_access_key is not None,
                                parameters.s3_secret_key is not None,
                                parameters.s3_region is not None,
                                parameters.s3_bucket is not None,
                                parameters.s3_profile is not None))

    if parameters.logfile is None:
        fname = os.path.basename(__file__)
        fsplit = os.path.splitext(fname)
        if fsplit[1] == '.py':
            fname = fsplit[0]
        parameters.logfile = os.path.join(os.getcwd(), '{}.log'.format(fname))

    fh = logging.FileHandler(parameters.logfile)
    ch = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s::%(name)s::%(processName)s'
                                  '::%(levelname)s::%(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    log_level_value = getattr(logging, parameters.loglevel)
    logger.setLevel(log_level_value)
    fh.setLevel(log_level_value)
    ch.setLevel(log_level_value)

    logger.addHandler(fh)
    logger.addHandler(ch)

    logger.info('Loglevel set to {}'.format(parameters.loglevel))

    if parameters.s3_infrequent_access and \
        ((parameters.retention_days is None and
         (parameters.retention_weeks is not None or
          parameters.retention_months is not None)) or
         (parameters.retention_days is not None and
          parameters.retention_days < 30)):
        logger.warning('You are using S3 Infrequent Access storage '
                       'with a retention policy (days) of less than '
                       '30 days. Be aware that Infrequent Access objects '
                       'are billed for at least 30 days, even if they are '
                       'deleted.')

    for source in (locals(), globals()):
        if parameters.command in source:
            source[parameters.command]()
            return
    raise RuntimeError('Command {} not found'.format(parameters.command))


def days_difference(d1, d2):
    return int((d1 - d2).days)


def weeks_difference(d1, d2):
    d1isocal = d1.isocalendar()
    d2isocal = d2.isocalendar()

    return (d1isocal[0] - d2isocal[0]) * 52 + (d1isocal[1] - d2isocal[1])


def months_difference(d1, d2):
    return (d1.year - d2.year) * 12 + (d1.month - d2.month)


def local_listdir():
    if os.path.isdir(parameters.dump_dir):
        for fname in os.listdir(parameters.dump_dir):
            yield fname


def s3_listdir():
    client_s3 = get_client_s3()
    more_objects = True

    objects_list = client_s3.list_objects_v2(
        Bucket=parameters.s3_bucket,
        Prefix=parameters.dump_format.split('%')[0],
    )
    while more_objects and objects_list['KeyCount'] > 0:
        for object_info in objects_list['Contents']:
            object_name = object_info['Key']
            yield object_name

        if 'NextContinuationToken' in objects_list:
            objects_list = client_s3.list_objects_v2(
                Bucket=parameters.s3_bucket,
                Prefix=parameters.dump_format.split('%')[0],
                ContinuationToken=objects_list['NextContinuationToken'],
            )
        else:
            more_objects = False


def local_removefiles(files):
    for f in files:
        fpath = os.path.join(parameters.dump_dir, f)
        if os.path.isdir(f):
            logger.info('Removing directory {}'.format(fpath))
            shutil.rmtree(fpath)
        else:
            logger.info('Removing file {}'.format(fpath))
            os.remove(fpath)


def s3_delete_objects(client_s3, objects):
    delete_requests = [{'Key': obj} for obj in objects[:const_parameters.s3_max_delete_objects]]

    response = client_s3.delete_objects(
        Bucket=parameters.s3_bucket,
        Delete={'Objects': delete_requests},
    )

    returnedObjects = []
    if 'Errors' in response:
        for error in response['Errors']:
            logger.warning('Error when deleting item {}: {} - {}'.format(
                error['Key'], error['Code'], error['Message']))
            returnedObjects.append(error['Key'])

    objects[:const_parameters.s3_max_delete_objects] = returnedObjects

    return objects


def s3_removefiles(objects):
    client_s3 = get_client_s3()
    objects = list(objects)

    # Must add log files!
    objects += ['{}.log'.format(obj) for obj in objects]

    while len(objects) > 0:
        logger.debug('Current number of objects to delete: '.format(len(objects)))
        objects = s3_delete_objects(client_s3, objects)


def apply_retention_policy(days, weeks, months,
                           listdircall=local_listdir,
                           removecall=local_removefiles):
    # If no retention policy is defined
    if days is None and weeks is None and months is None:
        return

    now = datetime.datetime.now()
    keep_weeks = {}
    keep_months = {}
    found_backups = []

    matching_fname = re.sub('(%.)+', '*', parameters.dump_format.replace('*', '\\*'))
    for fname in listdircall():
        if fnmatch.fnmatch(fname, matching_fname):
            ftime = datetime.datetime.strptime(fname, parameters.dump_format)

            if days is not None and days_difference(now, ftime) < days:
                continue

            found_backups.append(fname)
            if weeks is not None and \
                    (0 if days is not None
                     else -1) < weeks_difference(now, ftime) <= weeks:
                week_token = '{}{}'.format(*['%02d' % x for x in ftime.isocalendar()[0:2]])
                if week_token not in keep_weeks or ftime > keep_weeks[week_token][1]:
                    keep_weeks[week_token] = (fname, ftime)

            if months is not None and \
                    (0 if days is not None or
                     weeks is not None
                     else -1) < months_difference(now, ftime) <= months:
                month_token = '{}{}'.format(ftime.year, '%02d' % ftime.month)
                if month_token not in keep_months or ftime > keep_months[month_token][1]:
                    keep_months[month_token] = (fname, ftime)

    removecall(set(found_backups) - set([x[0] for x in keep_weeks.values()] +
                                        [x[0] for x in keep_months.values()]))


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
    connresetbypeer_currentretry = 0
    while items_list is None:
        try:
            items_list = client_ddb.scan(**kwargs)
        except SocketError as e:
            if e.errno != errno.ECONNRESET or \
                    connresetbypeer_currentretry >= const_parameters.connresetbypeer_maxretry:
                raise e

            connresetbypeer_currentretry += 1
            sleeptime = const_parameters.connresetbypeer_sleeptime
            sleeptime *= connresetbypeer_currentretry
            logger.info(('Got \'Connection reset by peer\', '
                         'waiting {} seconds before retry').format(sleeptime))
            time.sleep(sleeptime)
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
    for index_type in ('GlobalSecondaryIndexes', 'LocalSecondaryIndexes'):
        if index_type in table_schema:
            for i in xrange(len(table_schema[index_type])):
                for info in ('IndexSizeBytes', 'IndexStatus', 'IndexArn', 'ItemCount'):
                    if info in table_schema[index_type][i]:
                        del table_schema[index_type][i][info]
                if 'ProvisionedThroughput' in table_schema[index_type][i]:
                    for info in ('NumberOfDecreasesToday', 'LastIncreaseDateTime', 'LastDecreaseDateTime'):
                        if info in table_schema[index_type][i]['ProvisionedThroughput']:
                            del table_schema[index_type][i]['ProvisionedThroughput'][info]

    return table_schema


def table_backup(table_name):
    client_ddb = get_client_dynamodb()

    logger.info('Starting backup of table \'{}\''.format(table_name))

    # define the table directory
    if parameters.tar_path is not None:
        table_dump_path = os.path.join(parameters.tar_path, table_name)
    else:
        table_dump_path = os.path.join(parameters.dump_path, table_name)

    # Make the data directory if it does not exist
    table_dump_path_data = os.path.join(table_dump_path, const_parameters.data_dir)
    if not parameters.tar_path:
        if parameters.backup_only is None and \
                os.path.isdir(table_dump_path):
            shutil.rmtree(table_dump_path)

        if parameters.backup_only == 'data' and \
                os.path.isdir(table_dump_path_data):
            shutil.rmtree(table_dump_path_data)

        if not os.path.isdir(table_dump_path_data) and \
                (parameters.backup_only is None or
                 parameters.backup_only == 'data'):
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
    if parameters.backup_only is None or \
            parameters.backup_only == 'schema':
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
            f = TarFileIO(jdump)
            info = tarfile.TarInfo(name=fpath)
            info.size = len(f.getvalue())
            info.mode = 0o644
            parameters.tarwrite_queue.put({'tarinfo': info, 'fileobj': f})
        else:
            with open(fpath, 'w+') as f:
                f.write(jdump)

    # get table items
    if parameters.backup_only is None or \
            parameters.backup_only == 'data':
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
                f = TarFileIO(jdump)
                info = tarfile.TarInfo(name=fpath)
                info.size = len(f.getvalue())
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


def upload_to_s3(incomplete=False):
    # Upload to s3 if requested
    if parameters.s3:
        logger.info('Uploading files to S3')
        client_s3 = get_client_s3()

        s3_args = {}
        if parameters.s3_server_side_encryption:
            s3_args['ServerSideEncryption'] = 'AES256'
        if parameters.s3_infrequent_access:
            s3_args['StorageClass'] = 'STANDARD_IA'

        # Check if bucket exists
        if parameters.s3_create_bucket:
            buckets = client_s3.list_buckets()
            exists = False
            if 'Buckets' in buckets:
                for bucket in buckets['Buckets']:
                    if bucket['Name'] == parameters.s3_bucket:
                        exists = True
                        break

            if not exists:
                logger.info('Creating bucket {} in S3'.format(parameters.s3_bucket))
                client_s3.create_bucket(Bucket=parameters.s3_bucket, ACL='private')

        # Upload content
        if parameters.tar_path is not None:
            s3path = os.path.basename(parameters.dump_path)
            if incomplete:
                s3path = '{}~incomplete'.format(s3path)
            try:
                client_s3.upload_file(
                    Filename=parameters.dump_path,
                    Key=s3path,
                    Bucket=parameters.s3_bucket,
                    ExtraArgs=s3_args
                )

            except S3UploadFailedError as e:
                logger.exception(e)
                raise e
            s3logfname = '{}.log'.format(s3path)
        else:
            dumpdir = os.path.basename(parameters.dump_path)
            dumppathlen = len(parameters.dump_path) + 1
            for path, dirs, files in os.walk(parameters.dump_path):
                for f in files:
                    filepath = os.path.join(path, f)
                    s3path = os.path.join(dumpdir, filepath[dumppathlen:])
                    try:
                        client_s3.upload_file(
                            Filename=filepath,
                            Key=s3path,
                            Bucket=parameters.s3_bucket,
                            ExtraArgs=s3_args
                        )
                    except S3UploadFailedError as e:
                        logger.exception(e)
                        raise e
            s3logfname = '{}.log'.format(dumpdir)
        # Upload logfile
        logger.info('Uploading logfile to S3')
        try:
            client_s3.upload_file(
                Filename=parameters.logfile,
                Key=s3logfname,
                Bucket=parameters.s3_bucket,
                ExtraArgs=s3_args
            )
        except S3UploadFailedError as e:
            logger.exception(e)
            raise e


def backup():
    if parameters.dump_path is None:
        if parameters.s3:
            listdir, removefiles = s3_listdir, s3_removefiles
        else:
            listdir, removefiles = local_listdir, local_removefiles
        apply_retention_policy(parameters.retention_days,
                               parameters.retention_weeks,
                               parameters.retention_months,
                               listdir,
                               removefiles)

        parameters.dump_path = os.path.join(
            parameters.dump_dir,
            time.strftime(parameters.dump_format))

    if tar_type(parameters.dump_path) is not None:
        parameters.tar_path = 'dump'
        parameters.tarwrite_queue = multiprocessing.JoinableQueue()
        tarwrite_process = TarFileWriter(parameters.dump_path, parameters.tarwrite_queue)
        tarwrite_process.start()

        info = tarfile.TarInfo(parameters.tar_path)
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        parameters.tarwrite_queue.put({'tarinfo': info})

    tables_to_backup = get_dynamo_matching_table_names(parameters.table)
    if not tables_to_backup:
        logger.error('no tables found for \'{}\''.format(parameters.table))
        sys.exit(1)

    logger.info('The following tables will be backed up: {}'.format(
        ', '.join(tables_to_backup)))
    logger.info('Tables will be backed up in \'{}\''.format(parameters.dump_path))

    badReturn = parallel_workers(
        name='BackupProcess({})',
        target=table_backup,
        tables=tables_to_backup)

    if parameters.tar_path is not None:
        parameters.tarwrite_queue.put(None)
        parameters.tarwrite_queue.join()

    if badReturn:
        nErrors = len(badReturn)
        logger.info('Backup ended with {} error(s)'.format(nErrors))
        upload_to_s3(incomplete=True)
        raise RuntimeError(('{} error(s) during backup '
                            'for processes: {}').format(
                           nErrors,
                           ', '.join([x.name for x in badReturn])))
    else:
        logger.info('Backup ended without error')
        upload_to_s3(incomplete=False)


def get_dump_matching_table_names(table_name_wildcard):
    tables = []
    if not is_tarfile(parameters.dump_path):
        try:
            dir_list = sorted(os.listdir(parameters.dump_path))
        except OSError:
            logger.info("Cannot find \"{}\"".format(parameters.dump_path))
            sys.exit(1)

        for dir_name in dir_list:
            if fnmatch.fnmatch(dir_name, table_name_wildcard):
                tables.append(dir_name)
    else:
        tar = tarfile.open(parameters.dump_path)
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


def load_json_from_stream(f):
    content = f.read()
    if type(content) != str:
        content = content.decode('utf-8')

    return json.loads(content)


def table_restore(table_name):
    client_ddb = get_client_dynamodb()

    # define the table directory
    if is_tarfile(parameters.dump_path):
        tar = tarfile.open(parameters.dump_path)
        table_dump_path = os.path.join(parameters.tar_path, table_name)
        try:
            member = tar.getmember(os.path.join(table_dump_path, const_parameters.schema_file))
            f = tar.extractfile(member)

            table_schema = load_json_from_stream(f)
        except KeyError as e:
            raise RuntimeError('Schema of table \'{}\' not found'.format(table_name))
    else:
        table_dump_path = os.path.join(parameters.dump_path, table_name)
        if not os.path.isdir(table_dump_path):
            raise RuntimeError('Schema of table \'{}\' not found'.format(table_name))

        with open(os.path.join(table_dump_path, const_parameters.schema_file), 'r') as f:
            table_schema = load_json_from_stream(f)

    if 'Table' in table_schema:
        table_schema = table_schema['Table']
    table_schema = clean_table_schema(table_schema)

    table_schema['TableName'] = table_name  # Use the directory name as table name
    table_delete(client_ddb, table_name)
    table_create(client_ddb, **table_schema)

    table_dump_path_data = os.path.join(table_dump_path, const_parameters.data_dir)
    if is_tarfile(parameters.dump_path):
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

        if is_tarfile(parameters.dump_path):
            member = tar.getmember(os.path.join(table_dump_path_data, data_file))
            f = tar.extractfile(member)

            loaded_items = load_json_from_stream(f)
        else:
            with open(os.path.join(table_dump_path_data, data_file), 'r') as f:
                loaded_items = load_json_from_stream(f)

        if 'Items' in loaded_items:
            loaded_items = loaded_items['Items']
        items.extend(loaded_items)

        while len(items) >= const_parameters.dynamodb_max_batch_write or \
                (c_data_file == n_data_files and len(items) > 0):
            logger.debug('Current number of items: '.format(len(items)))
            items = table_batch_write(client_ddb, table_name, items)


def restore():
    if parameters.dump_path is None and parameters.restore_last:
        matching_fname = re.sub('(%.)+', '*', parameters.dump_format.replace('*', '\\*'))
        if os.path.isdir(parameters.dump_dir):
            most_recent = None
            for f in os.listdir(parameters.dump_dir):
                fpath = os.path.join(parameters.dump_dir, f)
                if fnmatch.fnmatch(f, matching_fname) and is_dumppath(fpath):
                    t = time.strptime(f, parameters.dump_format)
                    if most_recent is None or t > most_recent[1]:
                        most_recent = (fpath, t)
            if most_recent is None:
                raise RuntimeError(('No dump found in directory \'{}\' '
                                    'for format \'{}\'').format(
                                   parameters.dump_dir,
                                   parameters.dump_format))

            parameters.dump_path = most_recent[0]

    if parameters.dump_path is None or not os.path.exists(parameters.dump_path):
        raise RuntimeError('No dump specified to restore; please use --dump-path')

    # Check dump path validity
    if not is_dumppath(parameters.dump_path):
        raise RuntimeError(('Path \'{}\' is neither a directory '
                            'nor a tar archive').format(parameters.dump_path))

    logger.info('Looking for tables to restore in \'{}\''.format(
        parameters.dump_path))

    tables_to_restore = get_dump_matching_table_names(parameters.table)
    if not tables_to_restore:
        logger.error('no tables found for \'{}\''.format(parameters.table))
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


def parse_args():
    parser = argparse.ArgumentParser(
        description='DynamoDB backup\'n\'restore python script '
                    'with tarfile management')

    # Available commands
    parser.add_argument(
        'command',
        choices=('backup', 'restore'),
        help='The command to run')

    # Logging options
    parser.add_argument(
        '-d', '--debug',
        dest='loglevel',
        default='INFO',
        action='store_const', const='DEBUG',
        help='Activate debug output')
    parser.add_argument(
        '--loglevel',
        dest='loglevel',
        default='INFO',
        choices=('NOTSET', 'DEBUG', 'INFO',
                 'WARNING', 'ERROR', 'CRITICAL'),
        help='Define the specific log level')
    parser.add_argument(
        '--logfile',
        default=None,
        help='The path to the logfile')

    # AWS general options
    parser.add_argument(
        '--profile',
        default=os.getenv('AWS_DEFAULT_PROFILE', None),
        help='Define the AWS profile name (defaults to the environment '
             'variable AWS_DEFAULT_PROFILE)')
    parser.add_argument(
        '--access-key', '--accessKey',
        default=os.getenv('AWS_ACCESS_KEY_ID', None),
        help='Define the AWS default access key (defaults to the '
             'environment variable AWS_ACCESS_KEY_ID)')
    parser.add_argument(
        '--secret-key', '--secretKey',
        default=os.getenv('AWS_SECRET_ACCESS_KEY', None),
        help='Define the AWS default secret key (defaults to the '
             'environment variable AWS_SECRET_ACCESS_KEY)')
    parser.add_argument(
        '--region',
        default=os.getenv('AWS_DEFAULT_REGION', None),
        help='Define the AWS default region (defaults to the environment '
             'variable AWS_DEFAULT_REGION)')

    # AWS DynamoDB specific options
    parser.add_argument(
        '--ddb-profile',
        default=os.getenv('DDB_AWS_DEFAULT_PROFILE', None),
        help='Define the AWS DynamoDB profile name')
    parser.add_argument(
        '--ddb-access-key', '--ddb-accessKey',
        default=os.getenv('DDB_AWS_ACCESS_KEY_ID', None),
        help='Define the AWS DynamoDB access key')
    parser.add_argument(
        '--ddb-secret-key', '--ddb-secretKey',
        default=os.getenv('DDB_AWS_SECRET_ACCESS_KEY', None),
        help='Define the AWS DynamoDB secret key')
    parser.add_argument(
        '--ddb-region',
        default=os.getenv('DDB_AWS_DEFAULT_REGION', None),
        help='Define the AWS DynamoDB region')

    # AWS S3 specific options
    parser.add_argument(
        '--s3',
        action='store_true',
        help='Activate S3 mode')
    parser.add_argument(
        '--s3-server-side-encryption', '--s3-sse',
        action='store_true',
        help='Use server side encryption (AES256)')
    parser.add_argument(
        '--s3-infrequent-access', '--s3-ia',
        action='store_true',
        help='Store the objects in S3 as Infrequent Access '
             '(WARNING: IA objects are billed for at least 30 days)')
    parser.add_argument(
        '--s3-create-bucket',
        action='store_true',
        help='Create S3 bucket if it does not exist')
    parser.add_argument(
        '--s3-profile',
        default=os.getenv('S3_AWS_DEFAULT_PROFILE', None),
        help='Define the AWS S3 profile name')
    parser.add_argument(
        '--s3-access-key', '--s3-accessKey',
        default=os.getenv('S3_AWS_ACCESS_KEY_ID', None),
        help='Define the AWS S3 access key')
    parser.add_argument(
        '--s3-secret-key', '--s3-secretKey',
        default=os.getenv('S3_AWS_SECRET_ACCESS_KEY', None),
        help='Define the AWS S3 secret key')
    parser.add_argument(
        '--s3-region',
        default=os.getenv('S3_AWS_DEFAULT_REGION', None),
        help='Define the AWS S3 region')
    parser.add_argument(
        '--s3-bucket',
        default=os.getenv('S3_BUCKET', None),
        help='Define the AWS S3 bucket')

    # Backup and restore common options
    parser.add_argument(
        '--table',
        default='*',
        help='The table to backup or restore (\'*\' means all tables, '
             '\'t*\' means all tables starting with \'t\')')

    # Dump path options
    parser.add_argument(
        '--dump-path',
        default=None,
        help='The path to the dump directory; if specified, neither '
             '--dump-dir nor --dump-format will be used')
    parser.add_argument(
        '--dump-dir',
        default=os.getcwd(),
        help='The path to the dump directory, in which the dump '
             'file/directory will be created, depending on the '
             '--dump-format')
    parser.add_argument(
        '--dump-format',
        default='dynamodb-dump-%Y%m%d%H%M%S.tgz',
        help='The format of the file (if tar extension provided) or '
             'directory used for the dump')

    # Backup specific options
    parser.add_argument(
        '--backup-only', '--only',
        default=None,
        choices=('data', 'schema'),
        help='To backup only the data or schema')
    parser.add_argument(
        '--retention-days',
        default=None,
        type=int,
        help='The retention policy for the backups, in the form of '
             '\'keep all backups for X days\'')
    parser.add_argument(
        '--retention-weeks',
        default=None,
        type=int,
        help='The retention policy for the backups, in the form of '
             '\'keep weekly backups for X weeks\'')
    parser.add_argument(
        '--retention-months',
        default=None,
        type=int,
        help='The retention policy for the backups, in the form of '
             '\'keep monthly backups for X months\'')

    # Restore specific options
    parser.add_argument(
        '--restore-last',
        action='store_true',
        help='Restore the last available backup according to '
             'the dump format')

    # Parsing
    return parser.parse_args()


if __name__ == '__main__':
    parameters = parse_args()
    parameters.tar_path = None

    logger = logging.getLogger(os.path.basename(__file__))

    cli()
