# -*- coding: utf-8 -*-

import os
import tarfile

from . import parallelworkers
from . import tartools
from .backup import SanityCheckException
from . import restore

try:
    xrange
except NameError:
    xrange = range

try:
    raw_input
except NameError:
    raw_input = input


_CheckInstance = None


# allow_resume and resume_args are not used in the check context
def table_check(table_name, allow_resume=False, resume_args=None):
    _CheckInstance.get_logger().info(
        'Checking the backup of table \'{}\''.format(table_name))

    is_tarfile = tartools.is_tarfile(_CheckInstance.get_parameters().dump_path)

    # Open the tarfile
    if is_tarfile:
        tar = tarfile.open(_CheckInstance.get_parameters().dump_path)
        table_dump_path = os.path.join(
            _CheckInstance.get_parameters().tar_path,
            table_name)
    else:
        table_dump_path = os.path.join(
            _CheckInstance.get_parameters().dump_path,
            table_name)

    # define the table directory
    if is_tarfile:
        try:
            member = tar.getmember(os.path.join(
                table_dump_path,
                _CheckInstance.get_const_parameters().schema_file))
            f = tar.extractfile(member)

            table_schema = restore.load_json_from_stream(f)
        except KeyError as e:
            raise RuntimeError('Schema of table \'{}\' not found'.format(table_name))
    else:
        if not os.path.isdir(table_dump_path):
            raise RuntimeError('Schema of table \'{}\' not found'.format(table_name))

        with open(os.path.join(
                table_dump_path,
                _CheckInstance.get_const_parameters().schema_file
        ), 'r') as f:
            table_schema = restore.load_json_from_stream(f)

    if 'Table' in table_schema:
        table_schema = table_schema['Table']

    # Warning if the directory does not have the same name as in the schema
    if table_schema['TableName'] != table_name:
        message = (
            'Directory table name \'{}\' does not match schema '
            'table name \'{}\''.format(
                table_name,
                table_schema['TableName']
            ))
        raise SanityCheckException(message)

    _CheckInstance.get_logger().info(
        'Ended check of the backup of table \'{}\''.format(table_name))


class Check(restore.Restore):

    def run(self):
        global _CheckInstance

        try:
            tables_to_check = self.init_restore()

            self._logger.info('The following tables will be checked: {}'.format(
                ', '.join(tables_to_check)))

            _CheckInstance = self
            badReturn = parallelworkers.ParallelWorkers(
                max_processes=self._parameters.max_processes,
                logger=self._logger,
            ).launch(
                name='CheckProcess({})',
                target=table_check,
                tables=tables_to_check
            )

            if badReturn:
                nErrors = len(badReturn)
                err = RuntimeError(('Check ended with {} error(s) '
                                    'for tables: {}').format(
                                        nErrors,
                                        ', '.join(badReturn)))
                self._logger.exception(err)
                raise err
            else:
                self._logger.info('Check ended without error')
        finally:
            self.clean_tmp_dir()
