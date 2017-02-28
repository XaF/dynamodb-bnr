# -*- coding: utf-8 -*-

from botocore.exceptions import ClientError
import itertools
import json
import fnmatch
import glob
import os
import re
import readline
import shutil
import sys
import tarfile
import tempfile
import time

from . import common
from . import parallelworkers
from . import tartools
from .backup import SanityCheckException

try:
    xrange
except NameError:
    xrange = range

try:
    raw_input
except NameError:
    raw_input = input


def load_json_from_stream(f):
    content = f.read()
    if type(content) != str:
        content = content.decode('utf-8')

    return json.loads(content)


class ReturnedItemsException(Exception):
    def __init__(self, message, returned_number, items):
        super(ReturnedItemsException, self).__init__(message)
        self._returned_number = returned_number
        self._items = items

    def get_returned_number(self):
        return self._returned_number

    def get_items(self):
        return self._items


_RestoreInstance = None


def table_restore(table_name, allow_resume=False, resume_args=None):
    # If resetting the write capacity is needed
    reset_capacity = {}

    # Open the tarfile
    if tartools.is_tarfile(_RestoreInstance.get_parameters().dump_path):
        tar = tarfile.open(_RestoreInstance.get_parameters().dump_path)
        table_dump_path = os.path.join(_RestoreInstance.get_parameters().tar_path, table_name)
    else:
        table_dump_path = os.path.join(_RestoreInstance.get_parameters().dump_path, table_name)

    # Insure that we're only doing that the first time and not when we
    # are resuming a restore task
    if not allow_resume or resume_args is None:
        # define the table directory
        if tartools.is_tarfile(_RestoreInstance.get_parameters().dump_path):
            try:
                member = tar.getmember(os.path.join(
                    table_dump_path,
                    _RestoreInstance.get_const_parameters().schema_file))
                f = tar.extractfile(member)

                table_schema = load_json_from_stream(f)
            except KeyError as e:
                raise RuntimeError('Schema of table \'{}\' not found'.format(table_name))
        else:
            if not os.path.isdir(table_dump_path):
                raise RuntimeError('Schema of table \'{}\' not found'.format(table_name))

            with open(os.path.join(
                    table_dump_path,
                    _RestoreInstance.get_const_parameters().schema_file
            ), 'r') as f:
                table_schema = load_json_from_stream(f)

        if 'Table' in table_schema:
            table_schema = table_schema['Table']
        table_schema = common.clean_table_schema(table_schema)

        # Warning if the directory does not have the same name as in the schema
        if table_schema['TableName'] != table_name:
            if _RestoreInstance.get_parameters().ensure_matching_names != 'ignore':
                message = (
                    'Directory table name \'{}\' does not match schema '
                    'table name \'{}\''.format(
                        table_name,
                        table_schema['TableName']
                    ))
                if _RestoreInstance.get_parameters().ensure_matching_names == 'raise':
                    raise SanityCheckException(message)

                _RestoreInstance.get_logger().warning(
                    '{}; using directory table name.'.format(message))

            table_schema['TableName'] = table_name

        # Change the write capacity if requested and needed
        if _RestoreInstance.get_parameters().tmp_write_capacity is not None:
            # Check global throughput
            table_schema['ProvisionedThroughput'] = \
                table_schema.get('ProvisionedThroughput', {
                    'WriteCapacityUnits': 1,
                    'ReadCapacityUnits': 1
                })
            table_schema['ProvisionedThroughput']['WriteCapacityUnits'] = \
                table_schema['ProvisionedThroughput'].get('WriteCapacityUnits', 1)

            if table_schema['ProvisionedThroughput']['WriteCapacityUnits'] < \
                    _RestoreInstance.get_parameters().tmp_write_capacity:
                reset_capacity['ProvisionedThroughput'] = \
                    dict(table_schema['ProvisionedThroughput'])
                table_schema['ProvisionedThroughput']['WriteCapacityUnits'] = \
                    _RestoreInstance.get_parameters().tmp_write_capacity

                _RestoreInstance.get_logger().info((
                    'Write capacity of table \'{}\' will temporarily '
                    'be set to {} instead of {}').format(
                        table_name,
                        _RestoreInstance.get_parameters().tmp_write_capacity,
                        reset_capacity['ProvisionedThroughput']['WriteCapacityUnits']))

            # Check global secondary indexes
            if 'GlobalSecondaryIndexes' in table_schema:
                for i in xrange(len(table_schema['GlobalSecondaryIndexes'])):
                    table_schema['GlobalSecondaryIndexes'][i]['ProvisionedThroughput'] = \
                        table_schema['GlobalSecondaryIndexes'][i].get('ProvisionedThroughput', {
                            'WriteCapacityUnits': 1,
                            'ReadCapacityUnits': 1
                        })
                    table_schema['GlobalSecondaryIndexes'][i]['ProvisionedThroughput']['WriteCapacityUnits'] = \
                        table_schema['GlobalSecondaryIndexes'][i]['ProvisionedThroughput'].get('WriteCapacityUnits', 1)

                    if table_schema['GlobalSecondaryIndexes'][i]['ProvisionedThroughput']['WriteCapacityUnits'] < \
                            _RestoreInstance.get_parameters().tmp_write_capacity:

                        if 'GlobalSecondaryIndexUpdates' not in reset_capacity:
                            reset_capacity['GlobalSecondaryIndexUpdates'] = []

                        gsiu = dict(table_schema['GlobalSecondaryIndexes'][i]['ProvisionedThroughput'])
                        reset_capacity['GlobalSecondaryIndexUpdates'].append({
                            'Update': {
                                'IndexName': table_schema['GlobalSecondaryIndexes'][i]['IndexName'],
                                'ProvisionedThroughput': gsiu,
                            },
                        })
                        table_schema['GlobalSecondaryIndexes'][i]['ProvisionedThroughput']['WriteCapacityUnits'] = \
                            _RestoreInstance.get_parameters().tmp_write_capacity

                        _RestoreInstance.get_logger().info((
                            'Write capacity of global secondary index \'{}\' '
                            'will temporarily be set to {} instead '
                            'of {}').format(
                                table_schema['GlobalSecondaryIndexes'][i]['IndexName'],
                                _RestoreInstance.get_parameters().tmp_write_capacity,
                                gsiu['WriteCapacityUnits']))

        # Delete the table
        _RestoreInstance.table_delete(table_name)

        # Create the table
        _RestoreInstance.table_create(**table_schema)

    table_dump_path_data = os.path.join(
        table_dump_path,
        _RestoreInstance.get_const_parameters().data_dir)
    if not allow_resume or resume_args is None:
        if tartools.is_tarfile(_RestoreInstance.get_parameters().dump_path):
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
                _RestoreInstance.get_logger().info(
                    'No data to restore for table \'{}\''.format(table_name))
                return
        else:
            if not os.path.isdir(table_dump_path_data):
                _RestoreInstance.get_logger().info(
                    'No data to restore for table \'{}\''.format(table_name))
                return

            data_files = sorted(os.listdir(table_dump_path_data))

        c_data_file = 0
        items = []
        n_items = 0
    else:
        reset_capacity = resume_args['reset_capacity']
        data_files = resume_args['data_files']
        c_data_file = resume_args['c_data_file']
        items = resume_args['items']
        n_items = resume_args['n_items']

    n_data_files = len(data_files)

    if resume_args is not None:
        start_or_resume = 'Resuming'
        end_text = '({} items currently loaded and {} files left)'.format(
            len(items), n_data_files - c_data_file)
    else:
        start_or_resume = 'Starting'
        end_text = ''

    _RestoreInstance.get_logger().info('{} restoration of the data for table \'{}\'{}'.format(
        start_or_resume, table_name, end_text))

    # If some items are still to be processed...
    if resume_args is not None:
        while len(items) >= _RestoreInstance.get_const_parameters().dynamodb_max_batch_write or \
                (c_data_file == n_data_files and len(items) > 0):
            _RestoreInstance.get_logger().debug('Current number of items: {}'.format(len(items)))
            try:
                items = _RestoreInstance.table_batch_write(
                    table_name, items, 'raise')
            except ReturnedItemsException as e:
                resume_args['items'] = e.get_items()
                raise parallelworkers.JobResumeRequestException(
                    message = '{} returned items'.format(e.get_returned_number()),
                    delay = e.get_returned_number() * 2,
                    resume_args = resume_args
                )

    # Treat files containing items
    for i in xrange(c_data_file, n_data_files):
        data_file = data_files[i]

        c_data_file += 1
        _RestoreInstance.get_logger().info(
            'Loading items from file {} of {}'.format(c_data_file, n_data_files))

        if tartools.is_tarfile(_RestoreInstance.get_parameters().dump_path):
            member = tar.getmember(os.path.join(table_dump_path_data, data_file))
            f = tar.extractfile(member)

            loaded_items = load_json_from_stream(f)
        else:
            with open(os.path.join(table_dump_path_data, data_file), 'r') as f:
                loaded_items = load_json_from_stream(f)

        if 'Items' in loaded_items:
            loaded_items = loaded_items['Items']
        n_items += len(loaded_items)
        items.extend(loaded_items)

        action = 'raise' if allow_resume else 'sleep'

        while len(items) >= _RestoreInstance.get_const_parameters().dynamodb_max_batch_write or \
                (c_data_file == n_data_files and len(items) > 0):
            _RestoreInstance.get_logger().debug(
                'Current number of items: {}'.format(len(items)))
            try:
                items = _RestoreInstance.table_batch_write(
                    table_name, items, action)
            except ReturnedItemsException as e:
                resume_args = {
                    'reset_capacity': reset_capacity,
                    'data_files': data_files,
                    'c_data_file': c_data_file,
                    'items': e.get_items(),
                    'n_items': n_items,
                }
                raise parallelworkers.JobResumeRequestException(
                    message = '{} returned items'.format(e.get_returned_number()),
                    delay = e.get_returned_number() * 2,
                    resume_args = resume_args
                )

    _RestoreInstance._logger.info(
        'Restored {} items for table \'{}\''.format(
            n_items, table_name))

    # Resetting the write capacity if needed
    if reset_capacity:
        reset_msg = []
        if 'ProvisionedThroughput' in reset_capacity:
            reset_msg.append('table \'{}\' to {}'.format(
                table_name,
                reset_capacity['ProvisionedThroughput']['WriteCapacityUnits']))
        if 'GlobalSecondaryIndexUpdates' in reset_capacity:
            for x in reset_capacity['GlobalSecondaryIndexUpdates']:
                x = x['Update']
                reset_msg.append('global secondary index \'{}\' to {}'.format(
                    x['IndexName'],
                    x['ProvisionedThroughput']['WriteCapacityUnits']))
        _RestoreInstance.get_logger().info(
            'Resetting write capacity of {}'.format(', '.join(reset_msg)))

        _RestoreInstance.table_update(
            TableName=table_name,
            **reset_capacity
        )

    _RestoreInstance.get_logger().info(
        'Ended restoration of table \'{}\''.format(table_name))


class Restore(common.Command):

    def download_from_s3(self, s3path, path=tempfile.gettempdir()):
        self._logger.info('Downloading object \'{}\' from S3'.format(s3path))
        client_s3 = self._aws.get_client_s3()

        # Create the directory in which we will download the file(s), if needed
        path_to_use = os.path.dirname(os.path.join(path, s3path))
        if not os.path.exists(path_to_use):
            os.makedirs(path_to_use)

        try:
            # Try downloading the file itself
            filepath = os.path.join(path, s3path)
            client_s3.download_file(
                Bucket=self._parameters.s3_bucket,
                Key=s3path,
                Filename=filepath
            )
            return filepath
        except ClientError as e:
            if e.response['Error']['Code'] != '404':
                raise

            # If not found, must be a directory: try to download the whole hierarchy
            for filename in self.s3_listdir('{}{}'.format(s3path, os.path.sep)):
                dirpath = os.path.join(path, os.path.dirname(filename))
                if not os.path.exists(dirpath):
                    os.makedirs(dirpath)

                filepath = os.path.join(path, filename)
                client_s3.download_file(
                    Bucket=self._parameters.s3_bucket,
                    Key=filename,
                    Filename=filepath
                )

            return os.path.join(path, s3path)

    def get_dump_matching_table_names(self, table_name_wildcard):
        tables = []
        if not tartools.is_tarfile(self._parameters.dump_path):
            try:
                dir_list = sorted(os.listdir(self._parameters.dump_path))
            except OSError:
                self._logger.info("Cannot find \"{}\"".format(self._parameters.dump_path))
                sys.exit(1)

            for dir_name in dir_list:
                if fnmatch.fnmatch(dir_name, table_name_wildcard):
                    tables.append(dir_name)
        else:
            tar = tarfile.open(self._parameters.dump_path)
            members = sorted(tar.getmembers(), key=lambda x: x.name)
            for member in members:
                if member.isfile() and \
                        os.path.basename(member.name) == self._const_parameters.schema_file:
                    directory_path = os.path.dirname(member.name)
                    if self._parameters.tar_path is None:
                        self._parameters.tar_path = os.path.dirname(directory_path)
                    elif self._parameters.tar_path != os.path.dirname(directory_path):
                        continue
                    table_name = os.path.basename(directory_path)
                    if fnmatch.fnmatch(table_name, table_name_wildcard):
                        tables.append(table_name)

        return tables

    def table_delete(self, table_name):
        client_ddb = self._aws.get_client_dynamodb()

        self._logger.info('Deleting table \'{}\''.format(table_name))
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
                    self._logger.info((
                        'Waiting {} seconds for table \'{}\' '
                        'to be deleted [current status: {}]').format(
                            self._const_parameters.tableoperation_sleeptime,
                            table_name,
                            table_status))
                    time.sleep(self._const_parameters.tableoperation_sleeptime)
                    table_status = client_ddb.describe_table(TableName=table_name)
                    table_status = table_status["Table"]["TableStatus"]
            except ClientError as e:
                if e.response['Error']['Code'] == 'ResourceNotFoundException':
                    deleted = True
                elif e.response['Error']['Code'] in managedErrors:
                    errorcode = e.response['Error']['Code'][:-9].lower()
                    currentRetry[errorcode] += 1
                    if currentRetry[errorcode] >= self._const_parameters['{}_maxretry'.format(errorcode)]:
                        raise
                    sleeptime = self._const_parameters['{}_sleeptime'.format(errorcode)]
                    sleeptime = sleeptime * currentRetry[errorcode]
                    self._logger.info(
                        'Got \'{}\', waiting {} seconds before retry'.format(
                            e.response['Error']['Code'], sleeptime))
                    time.sleep(sleeptime)
                else:
                    raise

    def table_create(self, **kwargs):
        client_ddb = self._aws.get_client_dynamodb()

        table_name = kwargs['TableName']
        self._logger.info('Creating table \'{}\''.format(table_name))
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
                    self._logger.info((
                        'Waiting {} seconds for table \'{}\' '
                        'to be created [current status: {}]').format(
                            self._const_parameters.tableoperation_sleeptime,
                            table_name,
                            table_status))
                    time.sleep(self._const_parameters.tableoperation_sleeptime)
                    table_status = client_ddb.describe_table(TableName=table_name)
                    table_status = table_status["Table"]["TableStatus"]
                created = True
            except ClientError as e:
                if e.response['Error']['Code'] in managedErrors:
                    errorcode = e.response['Error']['Code'][:-9].lower()
                    currentRetry[errorcode] += 1
                    if currentRetry[errorcode] >= self._const_parameters['{}_maxretry'.format(errorcode)]:
                        raise
                    sleeptime = self._const_parameters['{}_sleeptime'.format(errorcode)]
                    sleeptime = sleeptime * currentRetry[errorcode]
                    self._logger.info(
                        'Got \'{}\', waiting {} seconds before retry'.format(
                            e.response['Error']['Code'], sleeptime))
                    time.sleep(sleeptime)
                else:
                    raise

    def table_update(self, **kwargs):
        client_ddb = self._aws.get_client_dynamodb()

        updated = False
        managedErrors = ['ResourceInUseException', 'LimitExceededException']
        currentRetry = {
            'resourceinuse': 0,
            'limitexceeded': 0,
        }

        while not updated:
            try:
                client_ddb.update_table(**kwargs)
                updated = True
            except ClientError as e:
                if e.response['Error']['Code'] in managedErrors:
                    errorcode = e.response['Error']['Code'][:-9].lower()
                    currentRetry[errorcode] += 1
                    if currentRetry[errorcode] >= self._const_parameters['{}_maxretry'.format(errorcode)]:
                        raise
                    sleeptime = self._const_parameters['{}_sleeptime'.format(errorcode)]
                    sleeptime = sleeptime * currentRetry[errorcode]
                    self._logger.info(
                        'Got \'{}\', waiting {} seconds before retry'.format(
                            e.response['Error']['Code'], sleeptime))
                    time.sleep(sleeptime)
                else:
                    raise

    def table_batch_write(self, table_name, items, action_returned='sleep'):
        client_ddb = self._aws.get_client_dynamodb()

        put_requests = []
        for item in items[:self._const_parameters.dynamodb_max_batch_write]:
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
                response = client_ddb.batch_write_item(**request_items)
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

        returnedItems = []
        if 'UnprocessedItems' in response and response['UnprocessedItems']:
            for put_request in response['UnprocessedItems'][table_name]:
                item = put_request['PutRequest']['Item']
                returnedItems.append(item)

        items[:self._const_parameters.dynamodb_max_batch_write] = returnedItems

        if returnedItems:
            nreturnedItems = len(returnedItems)
            message = ('Table \'{0}\': {1} item(s) returned during '
                       'batch write').format(
                table_name, nreturnedItems)
            if action_returned == 'raise':
                raise ReturnedItemsException(message, nreturnedItems, items)
            else:
                self._logger.info('{}{}'.format(
                    message,
                    '; sleeping {} second(s) to avoid congestion'.format(
                        nreturnedItems)))
                time.sleep(nreturnedItems)

        return items

    def get_last_backup(self, listdir):
        most_recent = None
        matching_fname = re.sub('(%.)+', '*', self._parameters.dump_format.replace('*', '\\*'))
        for filename in listdir():
            if fnmatch.fnmatch(filename, matching_fname):
                t = time.strptime(filename, self._parameters.dump_format)
                if most_recent is None or t > most_recent[1]:
                    most_recent = (filename, t)

        if most_recent is not None:
            most_recent = most_recent[0]

        return most_recent

    def local_interactive_complete(self, text, state):
        return (glob.glob(text + '*') + [None])[state]

    def s3_interactive_complete(self, text, state):
        try:
            return next(itertools.islice(self.s3_listdir(text), state, None))
        except StopIteration as e:
            return None

    def get_backup_interactive(self, listdir):
        most_recent = None
        matching_fname = re.sub('(%.)+', '*', self._parameters.dump_format.replace('*', '\\*'))
        for filename in listdir():
            if fnmatch.fnmatch(filename, matching_fname):
                t = time.strptime(filename, self._parameters.dump_format)
                if most_recent is None or t > most_recent[1]:
                    most_recent = (filename, t)

        if most_recent is not None:
            most_recent = most_recent[0]

        return most_recent

    def prepare_restore_path(self, backup_to_restore):
        if self._parameters.s3:
            self._parameters.dump_path = self.download_from_s3(backup_to_restore,
                                                               self._parameters.temp_dir)
        else:
            self._parameters.dump_path = os.path.join(self._parameters.dump_dir,
                                                      backup_to_restore)

        if not tartools.is_dumppath(self._parameters.dump_path):
            raise RuntimeError('Invalid backup path; will not be restored')

    def init_restore(self):
        self._parameters.temp_dir = os.path.join(
            tempfile.gettempdir(),
            'tmpdir-{}~{}'.format(
                os.path.basename(__file__),
                time.strftime('%Y%m%d%H%M%S')
            ))

        if self._parameters.dump_path is None and \
                (self._parameters.restore_last or self._parameters.restore_interactive):
            if self._parameters.s3:
                listdir, complete = self.s3_listdir, self.s3_interactive_complete
            else:
                listdir, complete = self.local_listdir, self.local_interactive_complete

            if self._parameters.restore_last:
                if self._parameters.s3:
                    location = 'S3'
                else:
                    location = 'directory \'{}\''.format(self._parameters.dump_dir)

                self._logger.info('Searching for last backup in {}'.format(location))
                backup_to_restore = self.get_last_backup(listdir)

                if backup_to_restore is None:
                    raise RuntimeError(('No dump found in {} '
                                        'for format \'{}\'').format(
                                       location,
                                       self._parameters.dump_format))

                self.prepare_restore_path(backup_to_restore)
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
                            self.prepare_restore_path(path)
                            found = True
                        except Exception as e:
                            self._logger.exception(e)
                except (KeyboardInterrupt, EOFError):
                    print('Aborted.')

                    # Clean up the temp directory if it was used
                    if self._parameters.temp_dir is not None and \
                            os.path.isdir(self._parameters.temp_dir):
                        shutil.rmtree(self._parameters.temp_dir)

                    sys.exit(0)

        if self._parameters.dump_path is None or \
                not tartools.is_dumppath(self._parameters.dump_path):
            raise RuntimeError('No dump specified to restore; please use --dump-path')

        if tartools.is_tarfile(self._parameters.dump_path) and \
                self._parameters.extract_before:
            self._logger.info('Extracting tar archive \'{}\' to \'{}\''.format(
                self._parameters.dump_path, self._parameters.temp_dir))
            tar = tarfile.open(self._parameters.dump_path)
            tar.extractall(path=self._parameters.temp_dir)
            self._parameters.dump_path = os.path.join(
                self._parameters.temp_dir, 'dump')
            self._logger.info('Archive extracted, proceeding')

        # Check dump path validity
        if not tartools.is_dumppath(self._parameters.dump_path):
            raise RuntimeError(('Path \'{}\' is neither a directory '
                                'nor a tar archive').format(
                self._parameters.dump_path))

        self._logger.info('Looking for tables to restore in \'{}\''.format(
            self._parameters.dump_path))

        tables_to_restore = self.get_dump_matching_table_names(
            self._parameters.table)
        if not tables_to_restore:
            self._logger.error('no tables found for \'{}\''.format(
                self._parameters.table))
            sys.exit(1)

        return tables_to_restore

    def clean_tmp_dir(self):
        # Clean up the temp directory if it was used
        if self._parameters.temp_dir is not None and \
                os.path.isdir(self._parameters.temp_dir):
            shutil.rmtree(self._parameters.temp_dir)

    def run(self):
        global _RestoreInstance

        try:
            tables_to_restore = self.init_restore()

            self._logger.info('The following tables will be restored: {}'.format(
                ', '.join(tables_to_restore)))

            _RestoreInstance = self
            badReturn = parallelworkers.ParallelWorkers(
                max_processes=self._parameters.max_processes,
                logger=self._logger,
            ).launch(
                name='RestoreProcess({})',
                target=table_restore,
                tables=tables_to_restore
            )

            if badReturn:
                nErrors = len(badReturn)
                err = RuntimeError(('Restoration ended with {} error(s) '
                                    'for tables: {}').format(
                                        nErrors,
                                        ', '.join(badReturn)))
                self._logger.exception(err)
                raise err
            else:
                self._logger.info('Restoration ended without error')
        finally:
            self.clean_tmp_dir()
