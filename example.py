import os
import luigi
from luigi.hadoop.warc.warc_tasks import HadoopWarcReaderJob
from six.moves.urllib.parse import urlparse


class GenerateWarcStats(HadoopWarcReaderJob):
    """
    Generates some WARC stats from a stream of ARC/WARC Records. The  :py:class:`HadoopWarcReaderJob` superclass
    handles the

    See :py:class:`HadoopWarcReaderJob` for details of the job parameters.
    """

    def output(self):
        out_name = "%s-stats.tsv" % os.path.splitext(self.input_file)[0]
        if self.from_local:
            return luigi.LocalTarget(out_name)
        else:
            return luigi.contrib.hdfs.HdfsTarget(out_name, format=luigi.contrib.hdfs.PlainFormat)

    def mapper(self, record):
        # type: (ArcWarcRecord) -> [(str, str)]
        """ Takes the parsed WARC record and extracts some basic stats."""

        if record.rec_type == 'response' and record.content_type.startswith(b'application/http'):

            # Extract
            record_url = record.rec_headers.get_header('WARC-Target-URI')
            status_code = record.http_headers.get_statuscode()

            if self.read_for_offset:
                print(record.raw_offset, record.raw_length, record.length, record.content_stream().read())
            else:
                print(record.length, record.content_stream().read())

            hostname = urlparse(record_url).hostname
            yield "%s\t%s" % (hostname, status_code), 1

    def reducer(self, key, values):
        """

        :param key:
        :param values:
        :return:
        """
        # for value in values:
        yield key, sum(values)


if __name__ == '__main__':
    # Just run it directly, rather than via the Luigi Scheduler:
    job = GenerateWarcStats('/Users/andy/Documents/workspace/python-shepherd/input-files.txt')
    job.run()
    out = job.output()

    # Example via Luigi:
    #luigi.run(['GenerateWarcStats', '--input-file', '/Users/andy/Documents/workspace/python-shepherd/input-files.txt', '--local-scheduler'])
