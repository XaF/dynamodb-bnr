# -*- coding: utf-8 -*-

import datetime
import fnmatch
import re
import os
import shutil

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
            objects = self._s3_delete_objects(objects)

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
        for fname in listdircall():
            if fnmatch.fnmatch(fname, matching_fname):
                ftime = datetime.datetime.strptime(
                    fname, self._parameters.dump_format)

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
