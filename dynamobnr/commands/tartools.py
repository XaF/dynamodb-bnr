# -*- coding: utf-8 -*-

import fnmatch
import logging
import multiprocessing
import os
import tarfile


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
    def __init__(self, path, tarwrite_queue, logger=logging):
        multiprocessing.Process.__init__(self, name='TarFileWriter')
        self._logger = logger
        self.tarwrite_queue = tarwrite_queue
        self.path = path

        self.tar_type = tar_type(self.path)

    def run(self):
        if self.tar_type is None:
            return

        tar = tarfile.open(self.path, mode=self.tar_type)
        self._logger.info('Starting')

        next_write = self.tarwrite_queue.get()
        while next_write is not None:
            self._logger.debug('Treating {}'.format(next_write))
            tar.addfile(**next_write)
            self.tarwrite_queue.task_done()

            next_write = self.tarwrite_queue.get()

        self._logger.info('Exiting')
        self.tarwrite_queue.task_done()
        tar.close()
