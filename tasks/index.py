import os
import re
import logging
import datetime
import xml.dom.minidom
from xml.parsers.expat import ExpatError
import random
import warcio
import urllib
from urllib import quote_plus  # python 2
# from urllib.parse import quote_plus # python 3
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar
from tasks.hdfs.listings import ListWarcsByDate
from tasks.hadoop.warc.warctasks import TellingReader
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
    cdx_server = luigi.Parameter(default='http://bigcdx:8080/data-heritrix')

    meta_flag = ''
    task_namespace = 'index'

    num_reducers = 50

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


class CheckCdxIndex(luigi.Task):
    input_file = luigi.Parameter()
    sampling_rate = luigi.IntParameter(default=100)
    cdx_server = luigi.Parameter(default='http://bigcdx:8080/data-heritrix')
    task_namespace = "index"

    count = 0
    tries = 0
    hits = 0

    def output(self):
        return state_file(None, 'cdx', 'checked-warc-files.txt')

    def run(self):
        # For each input file, open it up and get some URLs and timestamps.
        with open(str(self.input_file)) as flist:
            for line in flist:
                hdfs_file = luigi.contrib.hdfs.HdfsTarget(path=line.strip())
                logger.warning("Opening " + hdfs_file.path)
                with hdfs_file.open('r') as fin:
                    reader = warcio.ArchiveIterator(TellingReader(fin))
                    logger.info("Reader decompression types: %s" % reader.reader.decomp_type)
                    for record in reader:
                        record_url = record.rec_headers.get_header('WARC-Target-URI')
                        timestamp = record.rec_headers.get_header('WARC-Date')
                        logger.warning("Found a record: %s @ %s" % (record_url, timestamp))

                        # Only look at valid response records:
                        if record.rec_type == 'response' and record.content_type.startswith(b'application/http'):
                            # Strip down to Wayback form:
                            timestamp = re.sub('[^0-9]', '', timestamp)
                            # Check a random subset of the records, always emitting the first record:
                            if self.count == 0 or random.randint(1, self.sampling_rate) == 1:
                                logger.info("Checking a record: %s" % record_url)
                                capture_dates = self.get_capture_dates(record_url)
                                if timestamp in capture_dates:
                                    self.hits += 1
                                # Keep track of checked records:
                                self.tries += 1
                            # Keep track of total records:
                            self.count += 1
                    print("HELLO")

    def get_capture_dates(self, url):
        # Get the hits for this URL:
        q = "type:urlquery url:" + quote_plus(url)
        cdx_query_url = "%s?q=%s" % (self.cdx_server, quote_plus(q))
        capture_dates = []
        try:
            proxies = { 'http': 'http://explorer:3127'}
            f = urllib.urlopen(cdx_query_url, proxies=proxies)
            dom = xml.dom.minidom.parseString(f.read())
            for de in dom.getElementsByTagName('capturedate'):
                capture_dates.append(de.firstChild.nodeValue)
            f.close()
        except ExpatError, e:
            logger.warning("Exception on lookup: "  + str(e))

        return capture_dates


class CdxIndexAndVerify(luigi.Task):
    target_date = luigi.DateParameter(default=datetime.date.today() - datetime.timedelta(1))
    stream = luigi.Parameter(default='npld')

    def requires(self):
        return ListWarcsByDate(target_date=self.target_date, stream=self.stream)

    def output(self):
        return state_file(self.target_date, 'cdx', 'indexed-warc-files.txt')

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

    luigi.run(['CdxIndexAndVerify', '--local-scheduler'])

#    very = CdxIndexAndVerify(
#        date=datetime.datetime.strptime("2018-02-16","%Y-%m-%d"),
#        target_date = datetime.datetime.strptime("2018-02-10", "%Y-%m-%d")
#    )
#    cdx = CheckCdxIndex(input_file=very.input().path)
#    cdx.run()

    #input = os.path.join(os.getcwd(),'test/input-list.txt')
    #luigi.run(['CheckCdxIndex', '--input-file', input, '--from-local', '--local-scheduler'])
