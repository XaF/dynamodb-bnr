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
import glob
import itertools
import json
import logging
import math
import multiprocessing
import OpenSSL
import os
import re
import readline
import shutil
from socket import error as SocketError
import sys
import tarfile
import tempfile
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

try:
    raw_input
except NameError:
    raw_input = input


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


class SanityCheckException(Exception):
    pass


class ReturnedItemsException(Exception):
    def __init__(self, message, returned_number, items):
        super(ReturnedItemsException, self).__init__(message)
        self.__returned_number = returned_number
        self.__items = items

    def get_returned_number(self):
        return self.__returned_number

    def get_items(self):
        return self.__items


class JobResumeRequestException(Exception):
    def __init__(self, message, delay = 0, *args, **kwargs):
        super(JobResumeRequestException, self).__init__(message)
        self.__delay = delay
        self.__args = args
        self.__kwargs = kwargs

    def get_delay(self):
        return self.__delay

    def get_args(self):
        return self.__args

    def get_kwargs(self):
        return self.__kwargs


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
        int(os.getenv('DYNAMODB_BNR_LIMITEXCEEDED_MAXRETRY', 10)),
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
parser = None


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


def parser_error(errmsg):
    parser.print_usage()
    print('{}: error: {}'.format(
        os.path.basename(__file__), errmsg))
    sys.exit(1)


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
        parser_error(('DynamoDB configuration is incomplete '
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
        parser_error(('S3 configuration is incomplete '
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
    parser_error('Command \'{}\' not found'.format(parameters.command))


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


def s3_listdir(prefix=None):
    client_s3 = get_client_s3()
    more_objects = True

    if prefix is None:
        prefix = parameters.dump_format.split('%')[0]

    objects_list = client_s3.list_objects_v2(
        Bucket=parameters.s3_bucket,
        Prefix=prefix,
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
                raise

            connresetbypeer_currentretry += 1
            sleeptime = const_parameters.connresetbypeer_sleeptime
            sleeptime *= connresetbypeer_currentretry
            logger.info(('Got \'Connection reset by peer\', '
                         'waiting {} seconds before retry').format(sleeptime))
            time.sleep(sleeptime)
        except ClientError as e:
            if e.response['Error']['Code'] != 'ProvisionedThroughputExceededException' or \
                    throughputexceeded_currentretry >= const_parameters.throughputexceeded_maxretry:
                raise

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


def sanity_check_action(threshold, table, aws_value, calc_value):
    if parameters.sanity_check_action == 'ignore':
        pass

    message = ('Sanity check went over \'{}\' '
               'threshold for table \'{}\' (AWS '
               'advertised {} items, while we scanned {})'
               ).format(threshold, table, aws_value, calc_value)

    if parameters.sanity_check_action == 'raise':
        raise SanityCheckException(message)
    elif parameters.sanity_check_action == 'warning':
        logger.warning(message)


# allow_resume and resume_args are not used in the backup context
def table_backup(table_name, allow_resume=False, resume_args=None):
    global logger

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

        table_sanity = {
            'item_count_aws': 0.0,
            'item_count_calc': 0.0,
        }

        table_schema = None
        current_retry = 0
        while table_schema is None:
            try:
                table_schema = client_ddb.describe_table(TableName=table_name)
                table_schema = table_schema['Table']
            except (OpenSSL.SSL.SysCallError, OpenSSL.SSL.Error) as e:
                if current_retry >= const_parameters.opensslerror_maxretry:
                    raise

                logger.warning("Got OpenSSL error, retrying...")
                current_retry += 1

        table_sanity['item_count_aws'] += table_schema['ItemCount']
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
            table_sanity['item_count_calc'] += items_list['Count']

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

        # Check the sanity of the backup if requested
        if parameters.sanity_check_action != 'ignore' and \
                (parameters.sanity_check_threshold_more is not None or
                 parameters.sanity_check_threshold_less is not None):
            # Compute the percent of values that we have in excess or lack
            if table_sanity['item_count_aws'] == 0:
                percent_more = float('inf') \
                    if table_sanity['item_count_calc'] > 0 \
                    else 0.0
                percent_less = 0.0
            elif table_sanity['item_count_calc'] == 0:
                percent_more = 0.0
                percent_less = float('inf') \
                    if table_sanity['item_count_aws'] > 0 \
                    else 0.0
            else:
                percent_more = (table_sanity['item_count_calc'] /
                                table_sanity['item_count_aws']) - 1
                percent_less = (1.0 / (percent_more + 1)) - 1

            # Check if a given threshold has been broken
            broke = False
            if parameters.sanity_check_threshold_more is not None and \
                    percent_more > 0 and \
                    percent_more >= (parameters.sanity_check_threshold_more * 1.0 / 100.0):
                broke = 'more'
            elif parameters.sanity_check_threshold_less is not None and \
                    percent_less > 0 and \
                    percent_less >= (parameters.sanity_check_threshold_less * 1.0 / 100.0):
                broke = 'less'

            # Act if a threshold has been broken
            if broke:
                sanity_check_action(
                    broke,
                    table_name,
                    int(table_sanity['item_count_aws']),
                    int(table_sanity['item_count_calc'])
                )
            else:
                logger.info(('Sanity check for table \'{}\' did not break any '
                             'threshold (AWS advertised {} items, and we '
                             'scanned {})').format(
                    table_name,
                    int(table_sanity['item_count_aws']),
                    int(table_sanity['item_count_calc'])))

    logger.info('Ended backup of table \'{}\''.format(table_name))


def parallel_worker(job_queue, result_dict):
    global logger

    logger.info('Worker started.')

    # Get the job
    job = job_queue.get()

    # Run until receiving killing payload
    while job is not None:
        logger.info(('Received job: {dict_key} = '
                     '{func}(*{args}, **{kwargs})').format(**{
                        'dict_key': job.get('dict_key'),
                        'func': job.get('func'),
                        'args': job.get('args', []),
                        'kwargs': job.get('kwargs', {}).keys(),
                     }))

        # Run the job
        try:
            go_to_next = False
            while not go_to_next:
                try:
                    # Waiting for the right resume time
                    if 'resume_time' in job:
                        sleeptime = int(math.ceil(job['resume_time'] - time.time()))
                        if sleeptime > 0:
                            logger.info(('Waiting {} seconds before '
                                         'resuming job'.format(sleeptime)))
                            time.sleep(sleeptime)

                    logger.info(('Running job for \'{dict_key}\''.format(**job)))
                    job.get('func')(*job.get('args', []), **job.get('kwargs', {}))
                    result_dict[job.get('dict_key')] = 0
                    go_to_next = True
                except JobResumeRequestException as e:
                    newjob = job
                    newjob['args'] = tuple(list(newjob.get('args', [])) + list(e.get_args()))
                    newjob['kwargs'] = newjob.get('kwargs', {})
                    newjob['kwargs'].update(e.get_kwargs())
                    newjob['resume_time'] = time.time() + e.get_delay()

                    if job_queue.empty():
                        # Do not wait for another process to get the task, just
                        # run it directly as nothing seems to be waiting
                        logger.info(('Job resume request received for '
                                     'job \'{dict_key}\'; will resume it '
                                     'directly').format(**job))
                        job = newjob
                    else:
                        # There is still work to do in the job queue, go to the
                        # next task while this one cools down
                        logger.info(('Job resume request received for '
                                     'job \'{dict_key}\'; sending to the '
                                     'back of the job queue').format(**job))
                        job_queue.put(newjob)
                        go_to_next = True
        except Exception as e:
            logger.exception(e)
            result_dict[job.get('dict_key')] = 1
        finally:
            job_queue.task_done()

        # Get the next job
        job = job_queue.get()

    job_queue.task_done()
    logger.info('Worker exiting.')


def parallel_workers(name, target, tables):
    workers = []
    job_queue = multiprocessing.JoinableQueue()
    manager = multiprocessing.Manager()
    result_dict = manager.dict()

    # Create the workers
    nworkers = min(parameters.max_processes, len(tables))
    logger.info(('Starting {} workers ({} max) for {} tables').format(
        nworkers, parameters.max_processes, len(tables)))
    for i in xrange(nworkers):
        w = multiprocessing.Process(
            name=name.format(i),
            target=parallel_worker,
            args=(job_queue, result_dict))
        workers.append(w)
        w.start()

    # Prepare the jobs
    for table_name in tables:
        job = {
            'func': target,
            'kwargs': {
                'table_name': table_name,
                'allow_resume': True,
                'resume_args': None,
            },
            'dict_key': table_name
        }
        job_queue.put(job)

    try:
        # Wait that the job_queue is finished
        job_queue.join()

        # Add as many killer payload as processes
        for i in xrange(nworkers):
            job_queue.put(None)

        # Wait that the job_queue to be empty of the killer payloads
        job_queue.join()

        # Wait for each process to finish
        for worker in workers:
            worker.join()
    except KeyboardInterrupt:
        logger.info("Caught KeyboardInterrupt, terminating workers")

        # Add as many killer payload as processes
        for i in xrange(nworkers):
            job_queue.put(None)

        # Terminate all the workers
        for worker in workers:
            worker.terminate()

        # Wait for them to terminate properly
        for worker in workers:
            worker.join()

        logger.info('All workers terminated')

    return [k for k, v in result_dict.items() if v != 0]


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
                raise
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
                        raise
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
            raise


def download_from_s3(s3path, path=tempfile.gettempdir()):
    logger.info('Downloading object \'{}\' from S3'.format(s3path))
    client_s3 = get_client_s3()

    # Create the directory in which we will download the file(s), if needed
    path_to_use = os.path.dirname(os.path.join(path, s3path))
    if not os.path.exists(path_to_use):
        os.makedirs(path_to_use)

    try:
        # Try downloading the file itself
        filepath = os.path.join(path, s3path)
        client_s3.download_file(
            Bucket=parameters.s3_bucket,
            Key=s3path,
            Filename=filepath
        )
        return filepath
    except ClientError as e:
        if e.response['Error']['Code'] != '404':
            raise

        # If not found, must be a directory: try to download the whole hierarchy
        for filename in s3_listdir('{}{}'.format(s3path, os.path.sep)):
            dirpath = os.path.join(path, os.path.dirname(filename))
            if not os.path.exists(dirpath):
                os.makedirs(dirpath)

            filepath = os.path.join(path, filename)
            client_s3.download_file(
                Bucket=parameters.s3_bucket,
                Key=filename,
                Filename=filepath
            )

        return os.path.join(path, s3path)


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
        if not os.path.exists(os.path.dirname(parameters.dump_path)):
            os.makedirs(os.path.dirname(parameters.dump_path))

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
                            'for tables: {}').format(
                           nErrors,
                           ', '.join(badReturn)))
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
                    raise
                sleeptime = const_parameters['{}_sleeptime'.format(errorcode)]
                sleeptime = sleeptime * currentRetry[errorcode]
                logger.info("Got \'{}\', waiting {} seconds before retry".format(
                    e.response['Error']['Code'], sleeptime))
                time.sleep(sleeptime)
            else:
                raise


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
                    raise
                sleeptime = const_parameters['{}_sleeptime'.format(errorcode)]
                sleeptime = sleeptime * currentRetry[errorcode]
                logger.info("Got \'{}\', waiting {} seconds before retry".format(
                    e.response['Error']['Code'], sleeptime))
                time.sleep(sleeptime)
            else:
                raise


def table_batch_write(client, table_name, items, action_returned='sleep'):
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
                raise

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

    if returnedItems:
        nreturnedItems = len(returnedItems)
        message = ('Table \'{0}\': {1} item(s) returned during '
                   'batch write').format(
            table_name, nreturnedItems)
        if action_returned == 'raise':
            raise ReturnedItemsException(message, nreturnedItems, items)
        else:
            logger.info('{}{}'.format(
                message,
                '; sleeping {} second(s) to avoid congestion'.format(
                    nreturnedItems)))
            time.sleep(nreturnedItems)

    return items


def load_json_from_stream(f):
    content = f.read()
    if type(content) != str:
        content = content.decode('utf-8')

    return json.loads(content)


def table_restore(table_name, allow_resume=False, resume_args=None):
    global logger

    client_ddb = get_client_dynamodb()

    # Open the tarfile
    if is_tarfile(parameters.dump_path):
        tar = tarfile.open(parameters.dump_path)
        table_dump_path = os.path.join(parameters.tar_path, table_name)
    else:
        table_dump_path = os.path.join(parameters.dump_path, table_name)

    # Insure that we're only doing that the first time and not when we
    # are resuming a restore task
    if not allow_resume or resume_args is None:
        # define the table directory
        if is_tarfile(parameters.dump_path):
            try:
                member = tar.getmember(os.path.join(table_dump_path, const_parameters.schema_file))
                f = tar.extractfile(member)

                table_schema = load_json_from_stream(f)
            except KeyError as e:
                raise RuntimeError('Schema of table \'{}\' not found'.format(table_name))
        else:
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
    if not allow_resume or resume_args is None:
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

            data_files = sorted(os.listdir(table_dump_path_data))

        c_data_file = 0
        items = []
    else:
        data_files = resume_args['data_files']
        c_data_file = resume_args['c_data_file']
        items = resume_args['items']

    n_data_files = len(data_files)

    if resume_args is not None:
        start_or_resume = 'Resuming'
        end_text = '({} items currently loaded and {} files left)'.format(
            len(items), n_data_files - c_data_file)
    else:
        start_or_resume = 'Starting'
        end_text = ''

    logger.info('{} restoration of the data for table \'{}\'{}'.format(
        start_or_resume, table_name, end_text))

    # If some items are still to be processed...
    if resume_args is not None:
        while len(items) >= const_parameters.dynamodb_max_batch_write or \
                (c_data_file == n_data_files and len(items) > 0):
            logger.debug('Current number of items: {}'.format(len(items)))
            try:
                items = table_batch_write(client_ddb, table_name, items, 'raise')
            except ReturnedItemsException as e:
                resume_args['items'] = e.get_items()
                raise JobResumeRequestException(
                    message = '{} returned items'.format(e.get_returned_number()),
                    delay = e.get_returned_number() * 2,
                    resume_args = resume_args
                )

    # Treat files containing items
    for i in xrange(c_data_file, n_data_files):
        data_file = data_files[i]

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

        action = 'raise' if allow_resume else 'sleep'

        while len(items) >= const_parameters.dynamodb_max_batch_write or \
                (c_data_file == n_data_files and len(items) > 0):
            logger.debug('Current number of items: {}'.format(len(items)))
            try:
                items = table_batch_write(client_ddb, table_name, items, action)
            except ReturnedItemsException as e:
                resume_args = {
                    'data_files': data_files,
                    'c_data_file': c_data_file,
                    'items': e.get_items(),
                }
                raise JobResumeRequestException(
                    message = '{} returned items'.format(e.get_returned_number()),
                    delay = e.get_returned_number() * 2,
                    resume_args = resume_args
                )

    logger.info('Ended restoration of table \'{}\''.format(table_name))


def get_last_backup(listdir):
    most_recent = None
    matching_fname = re.sub('(%.)+', '*', parameters.dump_format.replace('*', '\\*'))
    for filename in listdir():
        if fnmatch.fnmatch(filename, matching_fname):
            t = time.strptime(filename, parameters.dump_format)
            if most_recent is None or t > most_recent[1]:
                most_recent = (filename, t)

    if most_recent is not None:
        most_recent = most_recent[0]

    return most_recent


def local_interactive_complete(text, state):
    return (glob.glob(text + '*') + [None])[state]


def s3_interactive_complete(text, state):
    try:
        return next(itertools.islice(s3_listdir(text), state, None))
    except StopIteration as e:
        return None


def get_backup_interactive(listdir):
    most_recent = None
    matching_fname = re.sub('(%.)+', '*', parameters.dump_format.replace('*', '\\*'))
    for filename in listdir():
        if fnmatch.fnmatch(filename, matching_fname):
            t = time.strptime(filename, parameters.dump_format)
            if most_recent is None or t > most_recent[1]:
                most_recent = (filename, t)

    if most_recent is not None:
        most_recent = most_recent[0]

    return most_recent


def prepare_restore(backup_to_restore):
    if parameters.s3:
        parameters.dump_path = download_from_s3(backup_to_restore,
                                                parameters.temp_dir)
    else:
        parameters.dump_path = os.path.join(parameters.dump_dir,
                                            backup_to_restore)

    if not is_dumppath(parameters.dump_path):
        raise RuntimeError('Invalid backup path; will not be restored')


def restore():
    parameters.temp_dir = os.path.join(
        tempfile.gettempdir(),
        'tmpdir-{}~{}'.format(
            os.path.basename(__file__),
            int(time.time())
        ))

    if parameters.dump_path is None and \
            (parameters.restore_last or parameters.restore_interactive):
        if parameters.s3:
            listdir, complete = s3_listdir, s3_interactive_complete
        else:
            listdir, complete = local_listdir, local_interactive_complete

        if parameters.restore_last:
            backup_to_restore = get_last_backup(listdir)

            if backup_to_restore is None:
                if parameters.s3:
                    location = 'S3'
                else:
                    location = 'directory \'{}\''.format(parameters.dump_dir)

                raise RuntimeError(('No dump found in {} '
                                    'for format \'{}\'').format(
                                   location,
                                   parameters.dump_format))

            prepare_restore(backup_to_restore)
        else:
            readline.set_completer_delims(' \t\n;')
            readline.parse_and_bind("tab: complete")
            readline.set_completer(complete)

            try:
                found = False
                while not found:
                    print('Enter path, or Ctrl+D to abort (use tab for completion)')
                    path = raw_input('> ')

                    try:
                        prepare_restore(path)
                        found = True
                    except Exception as e:
                        logger.exception(e)
            except (KeyboardInterrupt, EOFError):
                print('Aborted.')

                # Clean up the temp directory if it was used
                if parameters.temp_dir is not None and os.path.isdir(parameters.temp_dir):
                    shutil.rmtree(parameters.temp_dir)

                sys.exit(0)

    if parameters.dump_path is None or not is_dumppath(parameters.dump_path):
        raise RuntimeError('No dump specified to restore; please use --dump-path')

    if is_tarfile(parameters.dump_path) and parameters.extract_before:
        logger.info('Extracting tar archive \'{}\' to \'{}\''.format(
            parameters.dump_path, parameters.temp_dir))
        tar = tarfile.open(parameters.dump_path)
        tar.extractall(path=parameters.temp_dir)
        parameters.dump_path = os.path.join(parameters.temp_dir, 'dump')
        logger.info('Archive extracted, proceeding')

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

    # Clean up the temp directory if it was used
    if parameters.temp_dir is not None and os.path.isdir(parameters.temp_dir):
        shutil.rmtree(parameters.temp_dir)

    if badReturn:
        nErrors = len(badReturn)
        logger.info('Restoration ended with {} error(s)'.format(nErrors))
        raise RuntimeError(('{} error(s) during restoration '
                            'for tables: {}').format(
                                nErrors,
                                ', '.join([x.name for x in badReturn])))
    else:
        logger.info('Restoration ended without error')


def parse_args():
    global parser

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
    parser.add_argument(
        '--max-processes',
        default=multiprocessing.cpu_count() * 2,
        type=int,
        help='The maximum number of processes to run concurrently '
             '(one more process will be run to write to tar files)')

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
    parser.add_argument(
        '--sanity-check-action',
        default='raise',
        choices=('ignore', 'warning', 'raise'),
        help='The action to take when a sanity check threshold is reached, '
             'ignore just ignores it, warning warn about it and raise raise '
             'an exception')
    parser.add_argument(
        '--sanity-check-threshold-more',
        default=None,
        type=int,
        help='The threshold, in percent, for the number of elements that '
             'could be in excess of what is advertised by DynamoDB (keep '
             'in mind that there is an update every six hours)')
    parser.add_argument(
        '--sanity-check-threshold-less',
        default=None,
        type=int,
        help='The threshold, in percent, for the number of elements that '
             'could be in lack of what is advertised by DynamoDB (keep '
             'in mind that there is an update every six hours)')

    # Restore specific options
    parser.add_argument(
        '--restore-last',
        action='store_true',
        help='Restore the last available backup according to '
             'the dump format')
    parser.add_argument(
        '--restore-interactive',
        action='store_true',
        help='Enter the interactive mode to select which backup to '
             'restore')
    parser.add_argument(
        '--extract-before',
        action='store_true',
        help='Extract the tar file before proceeding to fasten the '
             'restore process')

    # Parsing
    return parser.parse_args()


if __name__ == '__main__':
    parameters = parse_args()
    parameters.tar_path = None

    logger = logging.getLogger(os.path.basename(__file__))

    cli()
