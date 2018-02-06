#!/usr/bin/env python
# encoding: utf-8
"""
This module summarises the tasks that are to be run daily.
"""

import luigi
from tasks.hdfs.listings import GenerateHDFSSummaries


class DailyIngestTasks(luigi.WrapperTask):
    """
    Daily ingest tasks, should generally be a few hours ahead of the access-side tasks (below):
    """
    def requires(self):
        return [GenerateHDFSSummaries()]


if __name__ == '__main__':
    # Running from Python, but using the Luigi scheduler:
    luigi.run(['DailyIngestTasks'])
