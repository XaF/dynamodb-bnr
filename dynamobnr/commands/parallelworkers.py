# -*- coding: utf-8 -*-

import logging
import math
import multiprocessing
import time

try:
    xrange
except NameError:
    xrange = range


class JobResumeRequestException(Exception):
    def __init__(self, message, delay = 0, *args, **kwargs):
        super(JobResumeRequestException, self).__init__(message)
        self._delay = delay
        self._args = args
        self._kwargs = kwargs

    def get_delay(self):
        return self._delay

    def get_args(self):
        return self._args

    def get_kwargs(self):
        return self._kwargs


class ParallelWorkers:
    def __init__(self, max_processes, logger=logging):
        self._max_processes = max_processes
        self._logger = logger

    def _parallel_worker(self, job_queue, result_dict):
        self._logger.info('Worker started.')

        # Get the job
        job = job_queue.get()

        # Run until receiving killing payload
        while job is not None:
            self._logger.info(('Received job: {dict_key} = '
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
                                self._logger.info(('Waiting {} seconds before '
                                                   'resuming job'.format(sleeptime)))
                                time.sleep(sleeptime)

                        self._logger.info(('Running job for \'{dict_key}\''.format(**job)))
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
                            self._logger.info(('Job resume request received for '
                                               'job \'{dict_key}\'; will resume it '
                                               'directly').format(**job))
                            job = newjob
                        else:
                            # There is still work to do in the job queue, go to the
                            # next task while this one cools down
                            self._logger.info(('Job resume request received for '
                                               'job \'{dict_key}\'; sending to the '
                                               'back of the job queue').format(**job))
                            job_queue.put(newjob)
                            go_to_next = True
            except Exception as e:
                self._logger.exception(e)
                result_dict[job.get('dict_key')] = 1
            finally:
                job_queue.task_done()

            # Get the next job
            job = job_queue.get()

        job_queue.task_done()
        self._logger.info('Worker exiting.')

    def launch(self, name, target, tables):
        workers = []
        job_queue = multiprocessing.JoinableQueue()
        manager = multiprocessing.Manager()
        result_dict = manager.dict()

        # Create the workers
        nworkers = min(self._max_processes, len(tables))
        self._logger.info(('Starting {} workers ({} max) for {} tables').format(
            nworkers, self._max_processes, len(tables)))
        for i in xrange(nworkers):
            w = multiprocessing.Process(
                name=name.format(i),
                target=self._parallel_worker,
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
            self._logger.info("Caught KeyboardInterrupt, terminating workers")

            # Add as many killer payload as processes
            for i in xrange(nworkers):
                job_queue.put(None)

            # Terminate all the workers
            for worker in workers:
                worker.terminate()

            # Wait for them to terminate properly
            for worker in workers:
                worker.join()

            self._logger.info('All workers terminated')

        return [k for k, v in result_dict.items() if v != 0]
