# -*- coding: utf-8 -*-

from botocore.exceptions import ClientError
import datetime
import fnmatch
import re
import os
import shutil
import time

from . import awstools

try:
    xrange
except NameError:
    xrange = range

#
# def _pickle_method(m):
#     if m.im_self is None:
#         return getattr, (m.im_class, m.im_func.func_name)
#     else:
#         return getattr, (m.im_self, m.im_func.func_name)
#
#
# copy_reg.pickle(types.MethodType, _pickle_method)


def days_difference(d1, d2):
    return int((d1 - d2).days)


def weeks_difference(d1, d2):
    d1isocal = d1.isocalendar()
    d2isocal = d2.isocalendar()

    return (d1isocal[0] - d2isocal[0]) * 52 + (d1isocal[1] - d2isocal[1])


def months_difference(d1, d2):
    return (d1.year - d2.year) * 12 + (d1.month - d2.month)


def clean_table_schema(table_schema):
    cleaninfo = ('TableArn', 'TableSizeBytes', 'TableStatus', 'ItemCount', 'CreationDateTime')
    cleanprovisioned = ('NumberOfDecreasesToday', 'LastIncreaseDateTime', 'LastDecreaseDateTime')
    index_types = ('GlobalSecondaryIndexes', 'LocalSecondaryIndexes')
    cleaninfoindex = ('IndexSizeBytes', 'IndexStatus', 'IndexArn', 'ItemCount')

    # Delete noise information
    for info in cleaninfo:
        if info in table_schema:
            del table_schema[info]
    for info in cleanprovisioned:
        if info in table_schema['ProvisionedThroughput']:
            del table_schema['ProvisionedThroughput'][info]
    for index_type in index_types:
        if index_type in table_schema:
            for i in xrange(len(table_schema[index_type])):
                for info in cleaninfoindex:
                    if info in table_schema[index_type][i]:
                        del table_schema[index_type][i][info]
                if 'ProvisionedThroughput' in table_schema[index_type][i]:
                    for info in cleanprovisioned:
                        if info in table_schema[index_type][i]['ProvisionedThroughput']:
                            del table_schema[index_type][i]['ProvisionedThroughput'][info]

    return table_schema


class Command:
    def __init__(self, parameters, const_parameters, logger):
        self._parameters = parameters
        self._const_parameters = const_parameters
        self._aws = awstools.AWSTools(parameters)
        self._logger = logger

    def get_parameters(self):
        return self._parameters

    def get_const_parameters(self):
        return self._const_parameters

    def get_aws(self):
        return self._aws

    def get_logger(self):
        return self._logger

    def local_listdir(self):
        if os.path.isdir(self._parameters.dump_dir):
            for fname in os.listdir(self._parameters.dump_dir):
                yield fname

    def s3_listdir(self, prefix=None):
        client_s3 = self._aws.get_client_s3()
        more_objects = True

        if prefix is None:
            prefix = self._parameters.dump_format.split('%')[0]

        objects_list = client_s3.list_objects_v2(
            Bucket=self._parameters.s3_bucket,
            Prefix=prefix,
        )
        while more_objects and objects_list['KeyCount'] > 0:
            for object_info in objects_list['Contents']:
                object_name = object_info['Key']
                yield object_name

            if 'NextContinuationToken' in objects_list:
                objects_list = client_s3.list_objects_v2(
                    Bucket=self._parameters.s3_bucket,
                    Prefix=self._parameters.dump_format.split('%')[0],
                    ContinuationToken=objects_list['NextContinuationToken'],
                )
            else:
                more_objects = False

    def local_removefiles(self, files):
        for f in files:
            fpath = os.path.join(self._parameters.dump_dir, f)
            if os.path.isdir(f):
                self._logger.info('Removing directory {}'.format(fpath))
                shutil.rmtree(fpath)
            else:
                self._logger.info('Removing file {}'.format(fpath))
                os.remove(fpath)

    def s3_delete_objects(self, objects):
        client_s3 = self._aws.get_client_s3()

        delete_requests = [
            {'Key': obj}
            for obj in objects[:self._const_parameters.s3_max_delete_objects]]

        response = client_s3.delete_objects(
            Bucket=self._parameters.s3_bucket,
            Delete={'Objects': delete_requests},
        )

        returnedObjects = []
        if 'Errors' in response:
            for error in response['Errors']:
                self._logger.warning('Error when deleting item {}: {} - {}'.format(
                    error['Key'], error['Code'], error['Message']))
                returnedObjects.append(error['Key'])

        objects[:self._const_parameters.s3_max_delete_objects] = returnedObjects

        return objects

    def s3_removefiles(self, objects):
        objects = list(objects)

        # Must add log files!
        objects += ['{}.log'.format(obj) for obj in objects]

        while len(objects) > 0:
            self._logger.debug('Current number of objects to delete: '.format(len(objects)))
            objects = self.s3_delete_objects(objects)

    def apply_retention_policy(self, days, weeks, months,
                               listdircall=local_listdir,
                               removecall=local_removefiles):
        # If no retention policy is defined
        if days is None and weeks is None and months is None:
            return

        now = datetime.datetime.now()
        keep_weeks = {}
        keep_months = {}
        found_backups = []

        matching_fname = re.sub(
            '(%.)+', '*', self._parameters.dump_format.replace('*', '\\*'))
        matching_fname_incomplete = '{}{}'.format(
            matching_fname, self._const_parameters.incomplete_suffix)

        for fname in listdircall():
            # Check if the filename matches a backup name, either full or
            # incomplete, and if so apply the policy for that file
            match = fnmatch.fnmatch(fname, matching_fname)
            incomplete = fnmatch.fnmatch(fname, matching_fname_incomplete)

            if match or incomplete:
                # Get the date and time of the backup from the file name
                dump_format = self._parameters.dump_format
                if incomplete:
                    dump_format = '{}{}'.format(
                        dump_format, self._const_parameters.incomplete_suffix)

                ftime = datetime.datetime.strptime(fname, dump_format)

                # If the backup is more recent than the limit of days to keep,
                # just keep it by not adding it to the found backups list
                if days is not None and days_difference(now, ftime) < days:
                    continue

                # Add the backup to the found backup list
                found_backups.append(fname)

                # Check if that backup should be kept as a weekly backup
                if weeks is not None and \
                        (0 if days is not None
                         else -1) < weeks_difference(now, ftime) <= weeks:
                    token = '{}{}'.format(*['%02d' % x for x in ftime.isocalendar()[0:2]])
                    if token not in keep_weeks or \
                            (not incomplete and
                             keep_weeks[token]['incomplete']) or \
                            (incomplete == keep_weeks[token]['incomplete'] and
                             ftime > keep_weeks[token]['time']):
                        keep_weeks[token] = {
                            'name': fname,
                            'time': ftime,
                            'incomplete': incomplete,
                        }

                # Check if that backup should be kept as a monthly backup
                if months is not None and \
                        (0 if days is not None or
                         weeks is not None
                         else -1) < months_difference(now, ftime) <= months:
                    token = '{}{}'.format(ftime.year, '%02d' % ftime.month)
                    if token not in keep_months or \
                            (not incomplete and
                             keep_months[token]['incomplete']) or \
                            (incomplete == keep_months[token]['incomplete'] and
                             ftime > keep_months[token]['time']):
                        keep_months[token] = {
                            'name': fname,
                            'time': ftime,
                            'incomplete': incomplete,
                        }

        # Remove the files that did not make the cut, if there is any
        removecall(set(found_backups) - set([x['name'] for x in keep_weeks.values()] +
                                            [x['name'] for x in keep_months.values()]))

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
                    maxretryid = '{}_maxretry'.format(errorcode)
                    if currentRetry[errorcode] >= self._const_parameters[maxretryid]:
                        raise
                    sleeptime = self._const_parameters['{}_sleeptime'.format(errorcode)]
                    sleeptime = sleeptime * currentRetry[errorcode]
                    self._logger.info(
                        'Got \'{}\', waiting {} seconds before retry'.format(
                            e.response['Error']['Code'], sleeptime))
                    time.sleep(sleeptime)
                else:
                    raise
