import os
import logging
import datetime
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar
from tasks.hdfs.listings import ListWarcsByDate
from tasks.hadoop.cdx_verify import CheckCdxIndex
from tasks.common import state_file, report_file

logger = logging.getLogger('luigi-interface')


class CopyToHDFS(luigi.Task):
    """
    This task lists all files on HDFS (skipping directories).

    As this can be a very large list, it avoids reading it all into memory. It
    parses each line, and creates a JSON item for each, outputting the result in
    [JSON Lines format](http://jsonlines.org/).

    It set up to run once a day, as input to downstream reporting or analysis processes.
    """
    input_file = luigi.Parameter()
    tag = luigi.Parameter()
    date = luigi.DateParameter(default=datetime.date.today())
    task_namespace = "hdfs"

    def output(self):
        full_path = os.path.join(self.tag, os.path.basename(self.input_file))
        return luigi.contrib.hdfs.HdfsTarget(full_path, format=luigi.contrib.hdfs.PlainFormat())

    def run(self):
        # Read the file in and write it to HDFS
        input = luigi.LocalTarget(path=self.input_file)
        with input.open('r') as reader:
            with self.output().open('w') as writer:
                for line in reader:
                    writer.write(line)


class CdxIndexer(luigi.contrib.hadoop_jar.HadoopJarJobTask):
    input_file = luigi.Parameter()
    num_reducers = 50
    cdx_server = "http://bigcdx:8080/data-heritrix"
    meta_flag = ""
    task_namespace = "index"

    def output(self):
        out_name = os.path.join("warcs2cdx", "%s-submitted.txt" % os.path.splitext(os.path.basename(self.input_file))[0])
        return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.Plain)

    def requires(self):
        return CopyToHDFS(input_file = self.input_file, tag="warcs2cdx")

    def jar(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        return os.path.join(dir_path, "jars/warc-hadoop-recordreaders-3.0.0-SNAPSHOT-job.jar")

    def main(self):
        return "uk.bl.wa.hadoop.mapreduce.cdx.ArchiveCDXGenerator"

    def args(self):
        return [
            "-Dmapred.compress.map.output=true",
            "-Dmapred.output.compress=true",
            "-Dmapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec",
            "-i", self.input(),
            "-o", self.output(),
            "-r", self.num_reducers,
            "-w",
            "-h",
            "-m", self.meta_flag,
            "-t", self.cdx_server,
            "-c", "CDX N b a m s k r M S V g"
        ]


class CdxIndexAndVerify(luigi.Task):
    target_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))
    stream = luigi.Parameter(default='npld')

    def requires(self):
        return ListWarcsByDate(target_date=self.target_date, stream=self.stream, file_list_date=datetime.date.today())

    def output(self):
        target_date_string = self.target_date.strftime("%Y-%m-%d")
        return state_file(self.target_date, 'cdx', 'indexed-warc-files-for-%s.txt' % target_date_string)

    def run(self):
        # First, yield a Hadoop job to run the indexer:
        #index_task = CdxIndexer(self.input().path)
        #yield index_task
        # Then yield another job to check it worked:
        verify_task = CheckCdxIndex(input_file=self.input().path)
        yield verify_task
        # If it worked, record it here.
        pass


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)
    luigi.interface.setup_interface_logging()

    luigi.run(['CdxIndexAndVerify', '--local-scheduler', '--target-date', '2018-02-10'])
    #task = CdxIndexAndVerify(target_date=datetime.datetime.strptime("2018-02-10","%Y-%m-%d"))
    #task.run()

    #input = os.path.join(os.getcwd(),'test/input-list.txt')
    #luigi.run(['CheckCdxIndex', '--input-file', input, '--from-local', '--local-scheduler'])
