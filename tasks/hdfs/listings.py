import os
import csv
import json
import logging
import datetime
import subprocess
import luigi
import luigi.contrib.hdfs
import luigi.contrib.webhdfs
from tasks.common import state_file

logger = logging.getLogger(__name__)

DEFAULT_BUFFER_SIZE = 1024*1000

csv_fieldnames = ['permissions', 'number_of_replicas', 'userid', 'groupid', 'filesize', 'modified_at', 'filename']


class ListAllFilesOnHDFSToLocalFile(luigi.Task):
    """
    This task lists all files on HDFS (skipping directories).

    As this can be a very large list, it avoids reading it all into memory. It
    parses each line, and creates a JSON item for each, outputting the result in
    [JSON Lines format](http://jsonlines.org/).

    It set up to run once a day, as input to downstream reporting or analysis processes.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    total_directories = -1
    total_files = -1
    total_bytes = -1
    total_under_replicated = -1

    def output(self):
        return state_file(self.date,'hdfs','all-files-list.jsonl', on_hdfs=False)

    def get_stats(self):
        return {'dirs' : self.total_directories, 'files': self.total_files, 'bytes' : self.total_bytes,
                'under-replicated' : self.total_under_replicated}

    def run(self):
        command = luigi.contrib.hdfs.load_hadoop_cmd()
        command += ['fs', '-lsr', '/']
        self.total_directories = 0
        self.total_files = 0
        self.total_bytes = 0
        self.total_under_replicated = 0
        with self.output().open('w') as fout:
            process = subprocess.Popen(command, stdout=subprocess.PIPE)
            for line in iter(process.stdout.readline, ''):
                if "lsr: DEPRECATED: Please use 'ls -R' instead." in line:
                    logger.warning(line)
                else:
                    permissions, number_of_replicas, userid, groupid, filesize, modification_date, modification_time, filename = line.split(None, 7)
                    filename = filename.strip()
                    timestamp = datetime.datetime.strptime('%s %s' % (modification_date, modification_time), '%Y-%m-%d %H:%M')
                    info = {
                        'permissions' : permissions,
                        'number_of_replicas': number_of_replicas,
                        'userid': userid,
                        'groupid': groupid,
                        'filesize': filesize,
                        'modified_at': timestamp.isoformat(),
                        'filename': filename
                    }
                    # Skip directories:
                    if permissions[0] != 'd':
                        self.total_files += 1
                        self.total_bytes += filesize
                        if number_of_replicas < 3:
                            self.total_under_replicated += 1
                        fout.write(json.dumps(info)+'\n')
                    else:
                        self.total_directories += 1


class ListAllFilesPutOnHDFS(luigi.Task):
    """
    This task lists all files on HDFS (skipping directories).

    As this can be a very large list, it avoids reading it all into memory. It
    parses each line, and creates a JSON item for each, outputting the result in
    [JSON Lines format](http://jsonlines.org/).

    It set up to run once a day, as input to downstream reporting or analysis processes.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date,'hdfs','all-files-list.jsonl.gz', on_hdfs=True, use_gzip=True)

    def run(self):
        # Read the file in and write it to HDFS
        with self.input().open('r') as reader:
            with self.output().open('w') as writer:
                for line in reader:
                    writer.write(line)


class ListEmptyFiles(luigi.Task):
    """
    Takes the full file list and extracts the empty files, as these should be checked.
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'empty-files-list.jsonl')

    def run(self):
        with self.output().open('w') as f:
            for line in self.input().open('r'):
                item = json.loads(line.strip())
                # Archive file names:
                if not item['permissions'].startswith('d') and item['filesize'] == "0":
                    f.write(json.dumps(item) + '\n')


class ListWebArchiveFiles(luigi.Task):
    """
    Takes the full file list and strips it down to just the WARCs and ARCs
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'warc-files-list.csv')

    def run(self):
        with self.output().open('w') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
            writer.writeheader()
            for line in self.input().open('r'):
                item = json.loads(line.strip())
                item['filename'] = item['filename'].strip()
                # Archive file names:
                if item['filename'].endswith('.warc.gz') or item['filename'].endswith('.arc.gz') \
                        or item['filename'].endswith('.warc') or item['filename'].endswith('.arc'):
                    writer.writerow(item)


class ListUKWAWebArchiveFiles(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by folder:
    """
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def requires(self):
        return ListWebArchiveFiles(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'warc-ukwa-files-list.csv')

    def run(self):
        with self.output().open('w') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
            writer.writeheader()
            with self.input().open('r') as fin:
                reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
                for item in reader:
                    # Archive file names:
                    if item['filename'].startswith('/data/') or item['filename'].startswith('/heritrix/'):
                        writer.writerow(item)


class ListUKWAWebArchiveFilesByCollection(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by collection 'npld' or 'selective':
    """
    date = luigi.DateParameter(default=datetime.date.today())
    subset = luigi.Parameter(default='npld')
    task_namespace = "hdfs"

    total_files = 0
    total_bytes = 0

    def requires(self):
        return ListWebArchiveFiles(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'warc-ukwa-%s-files-list.csv' % self.subset)

    def run(self):
        with self.output().open('w') as f:
            writer = csv.DictWriter(f, fieldnames=csv_fieldnames)
            writer.writeheader()
            with self.input().open('r') as fin:
                reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
                for item in reader:
                    # Archive file names:
                    if (self.subset == 'selective' and item['filename'].startswith('/data/')) \
                            or (self.subset == 'npld' and item['filename'].startswith('/heritrix/')):
                        self.total_files += 1
                        self.total_bytes += int(item['filesize'])
                        writer.writerow(item)


class ListDuplicateWebArchiveFiles(luigi.Task):
    """
    Takes the full WARC list and filters UKWA content by folder:
    """
    date = luigi.DateParameter(default=datetime.date.today())
    collection = luigi.Parameter(default='ukwa')
    task_namespace = "hdfs"

    def requires(self):
        if self.collection == 'ukwa':
            return ListUKWAWebArchiveFiles(self.date)
        elif self.collection == 'all':
            return ListWebArchiveFiles(self.date)
        else:
            raise Exception("Unrecognised collection parameter! %s non known!" % self.collection)

    def output(self):
        return state_file(self.date, 'hdfs', 'warc-%s-duplicate-files-list.tsv' % self.collection)

    def run(self):
        filenames = {}
        with self.input().open('r') as fin:
            reader = csv.DictReader(fin, fieldnames=csv_fieldnames)
            for item in reader:
                # Archive file names:
                basename = os.path.basename(item['filename'])
                if basename not in filenames:
                    filenames[basename] = [item['filename']]
                else:
                    filenames[basename].append(item['filename'])

        # And emit duplicates:
        unduplicated = 0
        with self.output().open('w') as f:
            for basename in filenames:
                if len(filenames[basename]) > 1:
                    f.write("%s\t%i\t%s\n" % (basename, len(filenames[basename]), json.dumps(filenames[basename])))
                else:
                    unduplicated += 1
        logger.info("Of %i WARC filenames, %i are stored in a single HDFS location." % (len(filenames), unduplicated))


class GenerateHDFSSummaries(luigi.WrapperTask):
    task_namespace = "hdfs"

    def requires(self):
        return [ ListAllFilesPutOnHDFS(), ListUKWAWebArchiveFiles(), ListDuplicateWebArchiveFiles(), ListEmptyFiles(),
                 ListUKWAWebArchiveFilesByCollection() ]


class PrintSomeLines(luigi.Task):
    """
    An example to try to get things working:
    """
    date = luigi.DateParameter(default=datetime.date(2017,11,22))

    def requires(self):
        return ListAllFilesOnHDFSToLocalFile(self.date)

    def output(self):
        return state_file(self.date, 'hdfs', 'empty-files-list.jsonl')

    def run(self):
        for line in self.input().open('r'):
            item = json.loads(line.strip())
            print(item)
            break



if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    #luigi.run(['ListUKWAWebArchiveFilesOnHDFS', '--local-scheduler'])
    luigi.run(['PrintSomeLines', '--local-scheduler'])
    #luigi.run(['ListEmptyFilesOnHDFS', '--local-scheduler'])
