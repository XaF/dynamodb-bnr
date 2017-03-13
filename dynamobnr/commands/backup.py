# -*- coding: utf-8 -*-

from boto3.exceptions import S3UploadFailedError
from botocore.exceptions import ClientError
import errno
import fnmatch
import json
import math
import multiprocessing
import OpenSSL
import os
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

from . import common
from . import parallelworkers
from . import tartools


class SanityCheckException(Exception):
    pass


_BackupInstance = None


# allow_resume and resume_args are not used in the backup context
def table_backup(table_name, allow_resume=False, resume_args=None):
    client_ddb = _BackupInstance.get_aws().get_client_dynamodb()

    _BackupInstance.get_logger().info(
        'Starting backup of table \'{}\''.format(table_name))

    backup_schema = _BackupInstance.get_parameters().backup_only is None or \
        _BackupInstance.get_parameters().backup_only == 'schema'
    backup_data = _BackupInstance.get_parameters().backup_only is None or \
        _BackupInstance.get_parameters().backup_only == 'data'
    sanity_check = \
        _BackupInstance.get_parameters().sanity_check_action != 'ignore' and \
        (_BackupInstance.get_parameters().sanity_check_threshold_more is not None or
         _BackupInstance.get_parameters().sanity_check_threshold_less is not None)
    update_read_capacity = backup_data and \
        _BackupInstance.get_parameters().tmp_read_capacity is not None
    reset_capacity = False

    table_sanity = {
        'item_count_aws': 0.0,
        'item_count_calc': 0.0,
    }

    # define the table directory
    if _BackupInstance.get_parameters().tar_path is not None:
        table_dump_path = os.path.join(
            _BackupInstance.get_parameters().tar_path,
            table_name)
    else:
        table_dump_path = os.path.join(
            _BackupInstance.get_parameters().dump_path,
            table_name)

    # Make the data directory if it does not exist
    table_dump_path_data = os.path.join(
        table_dump_path,
        _BackupInstance.get_const_parameters().data_dir)
    if not _BackupInstance.get_parameters().tar_path:
        if _BackupInstance.get_parameters().backup_only is None and \
                os.path.isdir(table_dump_path):
            shutil.rmtree(table_dump_path)

        if _BackupInstance.get_parameters().backup_only == 'data' and \
                os.path.isdir(table_dump_path_data):
            shutil.rmtree(table_dump_path_data)

        if not os.path.isdir(table_dump_path_data) and \
                (_BackupInstance.get_parameters().backup_only is None or
                 _BackupInstance.get_parameters().backup_only == 'data'):
            os.makedirs(table_dump_path_data)
        elif not os.path.isdir(table_dump_path):
            os.makedirs(table_dump_path)
    else:
        info = tarfile.TarInfo(table_dump_path)
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        _BackupInstance.tarwrite_queue.put({'tarinfo': info})

        info = tarfile.TarInfo(table_dump_path_data)
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        _BackupInstance.tarwrite_queue.put({'tarinfo': info})

    # get table schema
    if backup_schema or sanity_check or update_read_capacity:

        _BackupInstance.get_logger().info(
            ('{} table schema '
             'for table \'{}\'').format(
             'Backing up' if backup_schema else 'Reading',
             table_name))

        table_schema = _BackupInstance.table_schema(table_name)
        table_sanity['item_count_aws'] += table_schema['ItemCount']

        if backup_schema:
            table_schema = common.clean_table_schema(table_schema)

            jdump = json.dumps(
                table_schema,
                # cls=DateTimeEncoder,
                indent=_BackupInstance.get_const_parameters().json_indent)
            fpath = os.path.join(
                table_dump_path,
                _BackupInstance.get_const_parameters().schema_file)
            if _BackupInstance.get_parameters().tar_path is not None:
                f = TarFileIO(jdump)
                info = tarfile.TarInfo(name=fpath)
                info.size = len(f.getvalue())
                info.mode = 0o644
                _BackupInstance.tarwrite_queue.put({'tarinfo': info, 'fileobj': f})
            else:
                with open(fpath, 'w+') as f:
                    f.write(jdump)

        if update_read_capacity:
            tmp_read_capacity = \
                _BackupInstance.get_parameters().tmp_read_capacity(
                    table_sanity['item_count_aws'],
                    table_schema['ProvisionedThroughput']['ReadCapacityUnits'])

            if tmp_read_capacity is not None and \
                    tmp_read_capacity > \
                    table_schema['ProvisionedThroughput']['ReadCapacityUnits']:
                reset_capacity = \
                    _BackupInstance.get_parameters().reset_read_capacity
                _BackupInstance.get_logger().info(
                    ('Increasing read capacity '
                     'of table \'{}\' to {}').format(
                     table_name, tmp_read_capacity))
                _BackupInstance.table_update(
                    TableName=table_name,
                    ProvisionedThroughput={
                        'ReadCapacityUnits': tmp_read_capacity,
                        'WriteCapacityUnits':
                            table_schema['ProvisionedThroughput']['WriteCapacityUnits']
                    }
                )

    # get table items
    if backup_data:
        more_items = 1

        _BackupInstance.get_logger().info("Backing up items for table \'{}\'".format(table_name))

        items_list = _BackupInstance.manage_db_scan(client_ddb, TableName=table_name)
        while more_items and items_list['Count'] > 0:
            table_sanity['item_count_calc'] += items_list['Count']

            _BackupInstance.get_logger().info(('Backing up items for table'
                                              ' \'{}\' ({})').format(table_name, more_items))
            jdump = json.dumps(items_list['Items'],
                               # cls=DateTimeEncoder,
                               indent=_BackupInstance.get_const_parameters().json_indent)

            fpath = os.path.join(
                table_dump_path_data,
                '{}.json'.format(str(more_items).zfill(10)))
            if _BackupInstance.get_parameters().tar_path is not None:
                f = TarFileIO(jdump)
                info = tarfile.TarInfo(name=fpath)
                info.size = len(f.getvalue())
                info.mode = 0o644
                _BackupInstance.tarwrite_queue.put({'tarinfo': info, 'fileobj': f})
            else:
                with open(fpath, 'w+') as f:
                    f.write(jdump)

            if 'LastEvaluatedKey' in items_list:
                items_list = _BackupInstance.manage_db_scan(
                    client_ddb,
                    TableName=table_name,
                    ExclusiveStartKey=items_list['LastEvaluatedKey'])
                more_items += 1
            else:
                more_items = False

        _BackupInstance._logger.info(
            'Backed up {} items for table \'{}\''.format(
                int(table_sanity['item_count_calc']), table_name))

        # Check the sanity of the backup if requested
        if sanity_check:
            # Store variables in easier names to use
            c = table_sanity['item_count_aws']
            f = _BackupInstance.get_parameters().sanity_check_threshold_evolutive
            pmore = _BackupInstance.get_parameters().sanity_check_threshold_more
            pless = _BackupInstance.get_parameters().sanity_check_threshold_less

            # If we use the evolutive approach, compute the logarithm percent
            # that will be used if it is greater than the min or max percent.
            # If both pmore and pless are defined, the same difference factor
            # will be applied to the plog (i.e. if less if 2 times smaller than
            # more, plogless will receive a .5 factor)
            if f is not None:
                plog = 1. / math.log(max(2, c))
                if pless is not None and pmore is not None and pless != pmore:
                    diviser = float(max(pless, pmore)) / float(min(pless, pmore))
                    plogvalues = (f * plog, f / diviser * plog)
                    if pmore > pless:
                        plogmore, plogless = plogvalues
                    else:
                        plogless, plogmore = plogvalues
                else:
                    plogless = plogmore = f * plog
            else:
                plogless = plogmore = 0.

            # If the percentage less is defined, compute the minimum count we
            # must have received for the sanity check to pass
            if pless is not None:
                min_count = int(math.floor(c * (1. - max(pless / 100., plogless))))
            else:
                min_count = 0

            # If the percentage less is defined, compute the maximum count we
            # must have received for the sanity check to pass
            if pmore is not None:
                max_count = int(math.ceil(c * (1. + max(pmore / 100., plogmore))))
            else:
                max_count = float('inf')

            # Check if a threshold has been broken
            broke = False
            if table_sanity['item_count_calc'] > max_count:
                broke = 'more'
            elif table_sanity['item_count_calc'] < min_count:
                broke = 'less'

            # Act if a threshold has been broken
            if broke:
                _BackupInstance.sanity_check_action(
                    broke,
                    table_name,
                    int(table_sanity['item_count_aws']),
                    int(table_sanity['item_count_calc'])
                )
            else:
                _BackupInstance.get_logger().info(
                    ('Sanity check for table \'{}\' did not break any '
                     'threshold (AWS advertised {} items, and we '
                     'scanned {})').format(
                     table_name,
                     int(table_sanity['item_count_aws']),
                     int(table_sanity['item_count_calc'])))

    # Only if we wanted to reset the capacity at the end
    if reset_capacity:
        _BackupInstance.get_logger().info(('Resetting read capacity '
                                           'of table \'{}\' to {}').format(
            table_name,
            table_schema['ProvisionedThroughput']['ReadCapacityUnits']))

        # Re-read the table schema to be sure the write capacity did not change
        throughput = _BackupInstance.table_schema(table_name)['ProvisionedThroughput']
        throughput = {
            'ReadCapacityUnits':
                table_schema['ProvisionedThroughput']['ReadCapacityUnits'],
            'WriteCapacityUnits':
                throughput['WriteCapacityUnits']
        }

        # Update the table to reset the throughput read capacity
        _BackupInstance.table_update(
            TableName=table_name,
            ProvisionedThroughput=throughput
        )

    _BackupInstance.get_logger().info('Ended backup of table \'{}\''.format(table_name))


class Backup(common.Command):

    def table_schema(self, table_name):
        client_ddb = self._aws.get_client_dynamodb()

        table_schema = None
        retry = {
            'openssl': 0,
            'wrong_schema': 0
        }
        while table_schema is None:
            try:
                table_schema = client_ddb.describe_table(TableName=table_name)
                table_schema = table_schema['Table']

                if table_schema['TableName'] != table_name:
                    if retry['wrong_schema'] >= _BackupInstance.get_const_parameters().wrongschema_maxretry:
                        raise SanityCheckException(
                            'Unable to get the schema for table \'{}\''.format(
                                table_name))

                    table_schema = None
                    retry['wrong_schema'] += 1
            except (OpenSSL.SSL.SysCallError, OpenSSL.SSL.Error) as e:
                if retry['openssl'] >= _BackupInstance.get_const_parameters().opensslerror_maxretry:
                    raise

                _BackupInstance.get_logger().warning("Got OpenSSL error, retrying...")
                retry['openssl'] += 1

        return table_schema

    def upload_to_s3(self, incomplete=False):
        # Upload to s3 if requested
        if self._parameters.s3:
            self._logger.info('Uploading files to S3')
            client_s3 = self._aws.get_client_s3()

            s3_args = {}
            if self._parameters.s3_server_side_encryption:
                s3_args['ServerSideEncryption'] = 'AES256'
            if self._parameters.s3_infrequent_access:
                s3_args['StorageClass'] = 'STANDARD_IA'

            # Check if bucket exists
            if self._parameters.s3_create_bucket:
                buckets = client_s3.list_buckets()
                exists = False
                if 'Buckets' in buckets:
                    for bucket in buckets['Buckets']:
                        if bucket['Name'] == self._parameters.s3_bucket:
                            exists = True
                            break

                if not exists:
                    self._logger.info('Creating bucket {} in S3'.format(
                        self._parameters.s3_bucket))
                    client_s3.create_bucket(
                        Bucket=self._parameters.s3_bucket,
                        ACL='private')

            # Upload content
            if self._parameters.tar_path is not None:
                s3path = os.path.basename(self._parameters.dump_path)
                if incomplete:
                    s3path = '{}{}'.format(
                        s3path, self._const_parameters.incomplete_suffix)
                try:
                    client_s3.upload_file(
                        Filename=self._parameters.dump_path,
                        Key=s3path,
                        Bucket=self._parameters.s3_bucket,
                        ExtraArgs=s3_args
                    )

                except S3UploadFailedError as e:
                    self._logger.exception(e)
                    raise
                s3logfname = '{}.log'.format(s3path)
            else:
                dumpdir = os.path.basename(self._parameters.dump_path)
                dumppathlen = len(self._parameters.dump_path) + 1
                for path, dirs, files in os.walk(self._parameters.dump_path):
                    for f in files:
                        filepath = os.path.join(path, f)
                        s3path = os.path.join(dumpdir, filepath[dumppathlen:])
                        try:
                            client_s3.upload_file(
                                Filename=filepath,
                                Key=s3path,
                                Bucket=self._parameters.s3_bucket,
                                ExtraArgs=s3_args
                            )
                        except S3UploadFailedError as e:
                            self._logger.exception(e)
                            raise
                s3logfname = '{}.log'.format(dumpdir)
            # Upload logfile
            self._logger.info('Uploading logfile to S3')
            try:
                client_s3.upload_file(
                    Filename=self._parameters.logfile,
                    Key=s3logfname,
                    Bucket=self._parameters.s3_bucket,
                    ExtraArgs=s3_args
                )
            except S3UploadFailedError as e:
                self._logger.exception(e)
                raise

    def get_dynamo_matching_table_names(self, table_name_wildcard):
        client_ddb = self._aws.get_client_dynamodb()

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

    def manage_db_scan(self, client_ddb, **kwargs):
        items_list = None
        throughputexceeded_currentretry = 0
        connresetbypeer_currentretry = 0
        while items_list is None:
            try:
                items_list = client_ddb.scan(**kwargs)
            except SocketError as e:
                if e.errno != errno.ECONNRESET or \
                        connresetbypeer_currentretry >= self._const_parameters.connresetbypeer_maxretry:
                    raise

                connresetbypeer_currentretry += 1
                sleeptime = self._const_parameters.connresetbypeer_sleeptime
                sleeptime *= connresetbypeer_currentretry
                self._logger.info(('Got \'Connection reset by peer\', '
                                   'waiting {} seconds before retry').format(
                    sleeptime))
                time.sleep(sleeptime)
            except ClientError as e:
                if e.response['Error']['Code'] != 'ProvisionedThroughputExceededException' or \
                        throughputexceeded_currentretry >= self._const_parameters.throughputexceeded_maxretry:
                    raise

                throughputexceeded_currentretry += 1
                sleeptime = self._const_parameters.throughputexceeded_sleeptime
                sleeptime *= throughputexceeded_currentretry
                self._logger.info(('Got \'ProvisionedThroughputExceededException\', '
                                   'waiting {} seconds before retry').format(sleeptime))
                time.sleep(sleeptime)

        return items_list

    def sanity_check_action(self, threshold, table, aws_value, calc_value):
        if self._parameters.sanity_check_action == 'ignore':
            pass

        message = ('Sanity check went over \'{}\' '
                   'threshold for table \'{}\' (AWS '
                   'advertised {} items, while we scanned {})'
                   ).format(threshold, table, aws_value, calc_value)

        if self._parameters.sanity_check_action == 'raise':
            raise SanityCheckException(message)
        elif self._parameters.sanity_check_action == 'warning':
            self._logger.warning(message)

    def run(self):
        global _BackupInstance

        if self._parameters.dump_path is None:
            if self._parameters.s3:
                listdir, removefiles = self.s3_listdir, self.s3_removefiles
            else:
                listdir, removefiles = self.local_listdir, self.local_removefiles
            self.apply_retention_policy(
                self._parameters.retention_days,
                self._parameters.retention_weeks,
                self._parameters.retention_months,
                listdir,
                removefiles)

            self._parameters.dump_path = os.path.join(
                self._parameters.dump_dir,
                time.strftime(self._parameters.dump_format))

        if tartools.tar_type(self._parameters.dump_path) is not None:
            self._parameters.tar_path = 'dump'
            self.tarwrite_queue = multiprocessing.JoinableQueue()
            if not os.path.exists(os.path.dirname(self._parameters.dump_path)):
                os.makedirs(os.path.dirname(self._parameters.dump_path))

            tarwrite_process = tartools.TarFileWriter(
                self._parameters.dump_path,
                self.tarwrite_queue,
                self._logger)
            tarwrite_process.start()

            info = tarfile.TarInfo(self._parameters.tar_path)
            info.type = tarfile.DIRTYPE
            info.mode = 0o755
            self.tarwrite_queue.put({'tarinfo': info})

        tables_to_backup = self.get_dynamo_matching_table_names(self._parameters.table)
        if not tables_to_backup:
            self._logger.error('no tables found for \'{}\''.format(self._parameters.table))
            sys.exit(1)

        self._logger.info('The following tables will be backed up: {}'.format(
            ', '.join(tables_to_backup)))
        self._logger.info('Tables will be backed up in \'{}\''.format(
            self._parameters.dump_path))

        _BackupInstance = self
        badReturn = parallelworkers.ParallelWorkers(
            max_processes=self._parameters.max_processes,
            logger=self._logger,
        ).launch(
            name='BackupProcess({})',
            target=table_backup,
            tables=tables_to_backup
        )

        if self._parameters.tar_path is not None:
            self.tarwrite_queue.put(None)
            self.tarwrite_queue.join()

        if badReturn:
            nErrors = len(badReturn)
            err = RuntimeError(('Backup ended with {} error(s) '
                                'for tables: {}').format(
                                    nErrors,
                                    ', '.join(badReturn)))
            self._logger.exception(err)
            self.upload_to_s3(incomplete=True)
            raise err
        else:
            self._logger.info('Backup ended without error')
            self.upload_to_s3(incomplete=False)
