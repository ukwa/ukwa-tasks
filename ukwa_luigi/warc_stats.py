import luigi
from ukwa_luigi.warc_tasks import HadoopWarcReaderJob
try:
    from urllib.parse import urlparse
except ImportError:
    from urlparse import urlparse


class GenerateWarcStats(HadoopWarcReaderJob):
    """
    Generates the Warc stats by reading in each file and splitting the stream into entries.

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
    luigi.run(['GenerateWarcStats', '--input-file', '/Users/andy/Documents/workspace/python-shepherd/input-files.txt', '--from-local', '--local-scheduler'])
