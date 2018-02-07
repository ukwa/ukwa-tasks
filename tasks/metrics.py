import luigi
from prometheus_client import CollectorRegistry, Gauge
import tasks.hdfs.listings

# --------------------------------------------------------------------------
# Metrics definitions:
# --------------------------------------------------------------------------


def record_task_outcome(registry, task, value):
    # type: (CollectorRegistry, luigi.Task, int) -> None

    g = Gauge('ukwa_task_event_timestamp',
              'Timestamp of this task event.',
              labelnames=['task_namespace'], registry=registry)
    g.labels(task_namespace=task.task_namespace).set_to_current_time()

    g = Gauge('ukwa_task_status',
              'Record a 1 if a task ran, 0 if a task failed.',
               labelnames=['task_namespace'], registry=registry)
    g.labels(task_namespace=task.task_namespace).set(value)

    # Task-specific metrics:

    if isinstance(task, tasks.hdfs.listings.ListAllFilesOnHDFSToLocalFile):
        record_hdfs_stats(registry,task)

    if isinstance(task, tasks.hdfs.listings.ListUKWAWebArchiveFilesByCollection):
        record_hdfs_collection_stats(registry, task)


def record_hdfs_collection_stats(registry, task):
    # type: (CollectorRegistry, tasks.hdfs.listings.ListUKWAWebArchiveFilesByCollection) -> None
    col = task.subset

    g = Gauge('ukwa_warc_files_total_bytes',
              'Total size of WARC files on HDFS in bytes.',
              labelnames=['collection'], registry=registry)
    g.labels(collection=col).set(task.total_bytes)

    g = Gauge('ukwa_warc_files_total_count',
              'Total number of WARC files on HDFS.',
              labelnames=['collection'], registry=registry)
    g.labels(collection=col).set(task.total_files)


def record_hdfs_stats(registry, task):
    # type: (CollectorRegistry, tasks.hdfs.listings.ListAllFilesOnHDFSToLocalFile) -> None
    hdfs_service = 'hdfs-0.20'

    g = Gauge('hdfs_files_total_bytes',
              'Total size of files on HDFS in bytes.',
              labelnames=['service'], registry=registry)
    g.labels(service=hdfs_service).set(task.total_bytes)

    g = Gauge('hdfs_files_total_count',
              'Total number of files on HDFS.',
              labelnames=['service'], registry=registry)
    g.labels(service=hdfs_service).set(task.total_files)

    g = Gauge('hdfs_dirs_total_count',
              'Total number of directories on HDFS.',
              labelnames=['service'], registry=registry)
    g.labels(service=hdfs_service).set(task.total_directories)

    g = Gauge('hdfs_under_replicated_files_total_count',
              'Total number of files on HDFS with less than three copies.',
              labelnames=['service'], registry=registry)
    g.labels(service=hdfs_service).set(task.total_under_replicated)

