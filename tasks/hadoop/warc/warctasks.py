import os
import io
import sys
import binascii
import logging
import luigi
import luigi.contrib.hdfs
import luigi.contrib.hadoop
import warcio, six
from warcio.recordloader import ArcWarcRecord
import requests, urllib3, chardet, certifi, idna # Needed for HTTP actions

logger = logging.getLogger('luigi-interface')

#
# This is a Python-based streaming Hadoop job for performing basic processing of warcs
# and e.g. generating stats. However all implementations (`warctools`, `warc` and `pywb`) require
# behaviour that is 'difficult' to support. For the first two, both use Python's gzip support,
# which requires seekable streams (e.g. `seek(offset,whence)` support). Python Wayback (`pywb`)
# does not appear to depend on that module, but required `ffi` support for native calls, which
# makes deployment more difficult. Therefore, we use Java map-reduce jobs for WARC parsing, but
# we can generate simple line-oriented text files from the WARCs, after which streaming works
# just fine.
#
# Further experimentation with warcio shows this seems to be working better as it has very few dependencies.
#
# Also attempted to use Hadoop's built-in auto-gunzipping support, which is built into streaming mode.
# After some difficulties, this could be made to work, but was unreliable as different nodes would behave differently
# with respect to keeping-going when gunzipping concateneted gz records.
#


class ExternalListFile(luigi.ExternalTask):
    """
    This ExternalTask defines the Target at the top of the task chain. i.e. resources that are overall inputs rather
    than generated by the tasks themselves.
    """
    input_file = luigi.Parameter()

    def complete(self):
        """
        Assume the files are present, as checking for them slows things down a lot,
        and if they are not there we'll soon find out.
        """
        return True

    def output(self):
        """
        Returns the target output for this task.
        In this case, it expects a file to be present in HDFS.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.contrib.hdfs.HdfsTarget(self.input_file)


class ExternalFilesFromList(luigi.ExternalTask):
    """
    This ExternalTask defines the Target at the top of the task chain. i.e. resources that are overall inputs rather
    than generated by the tasks themselves.
    """
    input_file = luigi.Parameter()
    from_local = luigi.BoolParameter(default=False)

    def output(self):
        """
        Returns the target output for this task.
        In this case, it expects a file to be present in HDFS.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        for line in open(self.input_file, 'r').readlines():
            line = line.strip()
            if line:
                if self.from_local:
                    #logger.debug("Yielding local target: %s" % line)
                    yield luigi.LocalTarget(path=line)
                else:
                    #logger.debug("Yielding HDFS target: %s" % line)
                    yield luigi.contrib.hdfs.HdfsTarget(line, format=luigi.contrib.hdfs.format.PlainFormat)


# Special reader to read the input stream and yield WARC records:
class TellingReader():
    def __init__(self, stream):
        # Ensure open in binary mode:
        self.stream = io.open(stream.fileno(), 'rb')
        #self.stream = stream
        self.pos = 0

    def read(self, size=None):
        #logger.warning("read()ing from current position: %i, size=%s" % (self.pos, size))
        chunk = self.stream.read(size)
        #if len(bytes(chunk)) == 0:
        #    logger.warning("read() 0 bytes, current position: %i" % self.pos)
        #else:
        #    logger.warning("read() %s" % binascii.hexlify(chunk[:64]))
        self.pos += len(bytes(chunk))
        #logger.warning("read()ing current position now: %i" % self.pos)
        return chunk

    def readline(self, size=None):
        #logger.warning("readline()ing from current position: %i" % self.pos)
        line = self.stream.readline(size)
        #logger.warning("readline() %s" % line)
        self.pos += len(bytes(line))
        #logger.warning("readline()ing current position now: %i" % self.pos)
        return line

    def tell(self):
        #logger.debug("tell()ing current position: %i" % self.pos)
        return self.pos


class BinaryInputHadoopJobRunner(luigi.contrib.hadoop.HadoopJobRunner):
    """
    A job runner to use the UnsplittableInputFileFormat (based on DefaultHadoopJobRunner):
    """

    def __init__(self):
        config = luigi.configuration.get_config()
        streaming_jar = config.get('hadoop', 'streaming-jar')
        # Find our JAR:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        jar_path = os.path.join(dir_path, "../../jars/warc-hadoop-recordreaders-3.0.0-SNAPSHOT-job.jar")
        # Setup:
        super(BinaryInputHadoopJobRunner, self).__init__(
            streaming_jar=streaming_jar,
            input_format="uk.bl.wa.hadoop.mapred.UnsplittableInputFileFormat",
            libjars=[jar_path])


class HadoopWarcReaderJob(luigi.contrib.hadoop.JobTask):
    """
    Specialisation of the usual Hadoop JobTask that is configured to parse warc files.

    Should be sub-classed to make tasks that work with WARCs

    As this uses the stream directly and so data-locality is preserved (at least for the first chunk).

    Parameters:
        input_file: The path for the file that contains the list of WARC files to process
        from_local: Whether the paths refer to files on HDFS or local files
        read_for_offset: Whether the WARC parser should read the whole record so it can populate the
                         record.raw_offset and record.raw_length fields (good for CDX indexing). Enabling this will
                         mean the reader has consumed the content body so your job will not have access to it.
    """
    input_file = luigi.Parameter()
    from_local = luigi.BoolParameter(default=False)
    read_for_offset = luigi.BoolParameter(default=False)

    kv_separator = '\t'

    def __init__(self, **kwargs):
        super(HadoopWarcReaderJob, self).__init__(**kwargs)

    def requires(self):
        return ExternalFilesFromList(self.input_file, from_local=self.from_local)

    def extra_files(self):
        return []

    def extra_modules(self):
        # Always needs to include the root packages of everything that's imported above except luigi (because luigi handles that)
        return [warcio,six,requests,urllib3,chardet,certifi,idna]

    def jobconfs(self):
        jcs = super(HadoopWarcReaderJob, self).jobconfs()
        jcs.append('stream.map.input.field.separator=%s' % self.kv_separator)
        jcs.append('stream.map.input.ignoreKey=true')
        return jcs

    def job_runner(self):
        outputs = luigi.task.flatten(self.output())
        for output in outputs:
            if not isinstance(output, luigi.contrib.hdfs.HdfsTarget):
                logger.warn("Job is using one or more non-HdfsTarget outputs" +
                              " so it will be run in local mode")
                return luigi.contrib.hadoop.LocalJobRunner()
        else:
            return BinaryInputHadoopJobRunner()

    def run_mapper(self, stdin=sys.stdin, stdout=sys.stdout):
        """
        Run the mapper on the hadoop node.

        ANJ: Creating modified version to pass through the raw stdin
        """
        self.init_hadoop()
        self.init_mapper()
        outputs = self._map_input(stdin)
        if self.reducer == NotImplemented:
            self.writer(outputs, stdout)
        else:
            self.internal_writer(outputs, stdout)

    def read_key_from_stream(self, stream):
        c = ''
        name = []
        while c != self.kv_separator:
            name.append(c)
            c = stream.read(1)
        name = ''.join(name)
        #logger.warning("Got file name '%s'..." % name)
        return name

    def _map_input(self, input_stream):
        """
        Iterate over input and call the mapper for each item.
        If the job has a parser defined, the return values from the parser will
        be passed as arguments to the mapper.

        If the input is coded output from a previous run,
        the arguments will be split in key and value.

        ANJ: Modified to use the warcio parser instead of splitting lines.
        """
        # Wrap the stream in a handy Reader:
        wrapped_stream = TellingReader(input_stream)
        # Parse the start of the input stream, which is <filename>\t<filedata>
        #name = self.read_key_from_stream(wrapped_stream)
        ## Having consumed the 'key', read the payload:
        #wrapped_stream.pos = 0
        reader = warcio.ArchiveIterator(wrapped_stream)
        #logger.warning("Reader types: %s %s" % (reader.reader.decompressor, reader.reader.decomp_type))
        for record in reader:
            #logger.warning("Got record type: %s %s %i" % (record.rec_type, record.content_type, record.length ))
            #logger.warning("Got record format and headers: %s %s %s" % (record.format, record.rec_headers, record.http_headers ))
            #content = record.content_stream().read()
            #logger.warning("Record content: %s" % content[:128])
            #logger.warning("Record content as hex: %s" % binascii.hexlify(content[:128]))
            #logger.warning("Got record offset + length: %i %i" % (reader.get_record_offset(), reader.get_record_length() ))
            if self.read_for_offset:
                record.raw_offset = reader.get_record_offset()
                record.raw_length = reader.get_record_length()
            for output in self.mapper(record):
                yield output
        if self.final_mapper != NotImplemented:
            for output in self.final_mapper():
                yield output
        self._flush_batch_incr_counter()

    def mapper(self, record):
        # type: (ArcWarcRecord) -> [(str, str)]
        """ Override this call to implement your own ArcWarcRecord-reading mapper. """
        yield None, record

