import os
import posixpath
import luigi.contrib.hdfs
import luigi.contrib.webhdfs
from tasks.hdfs.webhdfs import WebHdfsPlainFormat
from metrics import *
from prometheus_client import CollectorRegistry, push_to_gateway

LOCAL_STATE_FOLDER = os.environ.get('LOCAL_STATE_FOLDER', '/var/task-state')
HDFS_STATE_FOLDER = os.environ.get('HDFS_STATE_FOLDER','/9_processing/task-state/')


def state_file(date, tag, suffix, on_hdfs=False, use_gzip=False, use_webhdfs=True):
    # Set up the state folder:
    state_folder = LOCAL_STATE_FOLDER
    pather = os.path
    if on_hdfs:
        pather = posixpath
        state_folder = HDFS_STATE_FOLDER

    # build the full path:
    if date:
        full_path = pather.join( str(state_folder),
                         date.strftime("%Y-%m"),
                         tag,
                         '%s-%s' % (date.strftime("%Y-%m-%d"), suffix))
    else:
        full_path = pather.join( str(state_folder), tag, suffix)

    if on_hdfs:
        if use_webhdfs:
            return luigi.contrib.hdfs.HdfsTarget(path=full_path, format=WebHdfsPlainFormat(use_gzip=use_gzip))
        else:
            return luigi.contrib.hdfs.HdfsTarget(path=full_path, format=luigi.contrib.hdfs.PlainFormat())
    else:
        return luigi.LocalTarget(path=full_path)


# --------------------------------------------------------------------------
# This general handler reports task failure and success, for each task
# family (class name) and namespace.
#
# For some specific classes, additional metrics are computed.
# --------------------------------------------------------------------------


@luigi.Task.event_handler(luigi.Event.FAILURE)
def notify_any_failure(task, exception):
    # type: (luigi.Task) -> None
    """
       Will be called directly after a successful execution
       and is used to update any relevant metrics
    """
    registry = CollectorRegistry()
    record_task_outcome(registry, task, 0)
    push_to_gateway(os.environ.get("PUSH_GATEWAY"), job=task.get_task_family(), registry=registry)


@luigi.Task.event_handler(luigi.Event.SUCCESS)
def celebrate_any_success(task):
    """Will be called directly after a successful execution
       of `run` on any Task subclass (i.e. all luigi Tasks)
    """

    # Where to store the metrics:
    registry = CollectorRegistry()

    # Generic metrics:
    record_task_outcome(registry, task, 1)

    # POST to prometheus:
    push_to_gateway(os.environ.get("PUSH_GATEWAY"), job=task.get_task_family(), registry=registry)

