import os
import re
import random
import logging
import datetime
import xml.dom.minidom
import urllib
from urllib import quote_plus  # python 2
# from urllib.parse import quote_plus # python 3
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop_jar
from tasks.hdfs.listings import ListWarcsByDate
from tasks.hadoop.warc.warctasks import HadoopWarcReaderJob
from tasks.common import state_file, report_file
from warcio.recordloader import ArcWarcRecord

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
        return os.path.join(dir_path, "jars/warc-hadoop-recordreaders-2.2.0-BETA-7-SNAPSHOT-job.jar")

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


class CheckCdxIndex(HadoopWarcReaderJob):
    """
    Picks a sample of URLs from some WARCs and checks they are in the CDX index.

    Parameters:
        input_file: The path for the file that contains the list of WARC files to process
        from_local: Whether the paths refer to files on HDFS or local files
        read_for_offset: Whether the WARC parser should read the whole record so it can populate the
                         record.raw_offset and record.raw_length fields (good for CDX indexing). Enabling this will
                         mean the reader has consumed the content body so your job will not have access to it.

    """

    sampling_rate = luigi.IntParameter(default=1)
    cdx_server = luigi.Parameter(default="http://bigcdx:8080/data-heritrix")

    n_reduce_tasks = 10

    def __init__(self, **kwargs):
        """Ensure arguments are set up correctly."""
        super(CheckCdxIndex, self).__init__(**kwargs)

    def output(self):
        """ Specify the output file name, which is based on the input file name."""
        out_name = "%s-cdx-verification-sampling-rate-%i.txt" % \
                   (os.path.splitext(self.input_file)[0], self.sampling_rate)
        if self.from_local:
            return luigi.LocalTarget(out_name)
        else:
            return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.PlainFormat)

    def mapper(self, record):
        # type: (ArcWarcRecord) -> [(str, str)]
        """ Takes the parsed WARC record and extracts some basic stats."""

        # Only look at valid response records:
        if record.rec_type == 'response' and record.content_type.startswith(b'application/http'):

            # Extract the URI and status code:
            record_url = record.rec_headers.get_header('WARC-Target-URI')
            timestamp = record.rec_headers.get_header('WARC-Date')
            # Strip down to Wayback form:
            timestamp = re.sub('[^0-9]','', timestamp)
            # Yield a random subset of the records:
            if random.randint(1, self.sampling_rate) == 1:
                logger.info("Emitting a record: %s" % record_url)
                yield record_url, timestamp

    def reducer(self, url, timestamps):
        """
        This takes the URL and Status Code and verifies that it's in the CDX Server

        :param key:
        :param values:
        :return:
        """

        # Get the hits for this URL:
        q = "type:urlquery url:" + quote_plus(url)
        cdx_query_url = "%s?q=%s" % (self.cdx_server, quote_plus(q))
        capture_dates = []
        try:
            f = urllib.urlopen(cdx_query_url)
            dom = xml.dom.minidom.parseString(f.read())
            for de in dom.getElementsByTagName('capturedate'):
                capture_dates.append(de.firstChild.nodeValue)
            f.close()
        except Exception, e:
            print(e)

        print(capture_dates, timestamps)

        misses = 0
        hits = 0
        for timestamp in timestamps:
            if timestamp in capture_dates:
                print("OK!?")
                hits += 1
            else:
                print("MISS!!")
                misses += 1

        yield url, "%i\t%i" %( hits, misses )


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
        verify_task = CheckCdxIndex(input_file=self.input().path, from_local=True)
        yield verify_task
        # If it worked, record it here.
        pass


if __name__ == '__main__':
    import logging

    logging.getLogger().setLevel(logging.INFO)

    #luigi.run(['CdxIndexAndVerify', '--local-scheduler', '--target-date', '2018-02-10'])
    input = os.path.join(os.getcwd(),'test/input-list.txt')
    luigi.run(['CheckCdxIndex', '--input-file', input, '--from-local', '--local-scheduler'])
