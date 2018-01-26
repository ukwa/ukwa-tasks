# ukwa-hadoop-tasks
Luigi tasks for running Hadoop jobs and managing material held on HDFS

## Getting started

n.b. we currently run Python 2.7, although code should be compatible.

### Set up the environment

    sudo pip install virtualenv
    virtualenv -p python2.7 venv
    source venv/bin/activate
    pip install -r requirements.txt


### Running locally

The `example.py` file shows how to run a local test. Running

    python example.py

will parse the WARC file listed in `test/input-list.txt` and create a `test/input-list-stats.tsv` file that summarises
the contents of the WARC file in terms of hosts and status codes.

