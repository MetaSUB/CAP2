Quickstart
==========


Installing the CAP
^^^^^^^^^^^^^^^^^^

To install download the repo and run the setup command

.. code-block:: bash

    python setup.py install
    cap2 --help


Running the CAP
^^^^^^^^^^^^^^^

Set Up
------

After the CAP is intalled it can be run on local data. There is no need to install additional programs or databases as these will be automatically installed as needed.

To run the CAP you will need to make two files. A manifest file telling the CAP what data to analyze and a config file telling the CAP where to store its outputs.

This manifest file should have three columns separated by tabs.
 - The first column is the name of the sample
 - the second column is the path to the first read
 - the third column is the path to the second read

 An example manifest could look like this

.. code-block:: bash

    sample_1    /path/to/sample_1.R1.fq.gz  /path/to/sample_1.R2.fq.gz
    sample_2    /path/to/sample_2.R1.fq.gz  /path/to/sample_2.R2.fq.gz
    sample_3    /path/to/sample_3.R1.fq.gz  /path/to/sample_3.R2.fq.gz

The config file should be a yaml file with two keys `out_dir` and `db_dir`

 An example manifest could look like this

.. code-block:: bash

    out_dir: /path/to/output/dir
    db_dir: /path/to/dir/where/databases/should/be/stored

Once you have the manifest and config file you can run the pipeline.

Running the Pipeline
-------------------

To run the qpipeline use the following command

.. code-block:: bash

    cap2 run pipeline -c /path/to/config.yaml /path/to/manifest.txt

The CAP uses subpipelines to group modules. There are five such subpipelines: quality control, preprocessing, short read analysis, assembly, and databases. To run a specific sub-pipelines add `--stage <stage name>` to the above command. The different stage names are listed when you run `cap2 run pipeline --help`

You can also run more than one task at the same time using `--workers <number of simultaneous tasks>` or increase the number of threads per worker with `--threads <num threads>`

An example of running the quality control pipeline with two workers that have four threads each would be:

.. code-block:: bash

    cap2 run pipeline --stage qc --workers 2 --threads 4 -c /path/to/config.yaml /path/to/manifest.txt


See the `Demo <https://github.com/MetaSUB/CAP2/tree/master/demo>`_ for more details.


Advanced Topics
^^^^^^^^^^^^^^^

Installing Subprograms
----------------------

By default the CAP installs all necessary subprograms itself. If a particular version of a subprogram is required it is usually possible to override the defaults and supply the program as a parameter.


Databases
---------

The CAP uses a number of large reference databases. Building and indexing these databases is considered a first-class part of the pipeline and :ref:`modules are included<Databases>` to do so. However, in most cases users will simply want to download prebuilt version of the databases rather than build them from scratch. This is the pipeline default and will happen automatically.

To preload databases on a machine run this from the command line:

.. code-block:: bash

    cap2 run db -c /path/to/config/file

or from python

.. code-block:: python

    from .api import run_db_stage

    config = /path/to/config/file
    run_db_stage(config_path=config, cores=1, workers=1)



Running Tests
-------------

Unit tests can be run with pytest from the repo

.. code-block:: bash

    python -m pytest tests


Configuration
-------------

By default CAP2 downloads all necessary programs and databases when it is run. For users running CAP2 multiple times on the same system it will be beneficial to set up configuration so that downloads only occur once.

Configuration consists of setting three environmental variables. These should go in your `.bashrc` or equivalent.

.. code-block:: bash

    CAP2_DB_DIR=<some local path...>
    CAP2_CONDA_SPEC_DIR=<some local path...>
    CAP2_CONDA_BASE_PATH=<some local path...>


You can also use a yaml configuration file. See :ref:`the API <capconfig>` for details and all options.


Running in the cloud
--------------------

Running the CAP2 in the cloud often requires some additional setup. This is what we needed to do to get the CAP2 running on DigitalOcean Ubuntu Servers:

.. code-block:: bash

    sudo apt update
    sudo apt install build-essential python-dev libfreetype6-dev config
    wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
    bash Miniconda3-latest-Linux-x86_64.sh
    source ~/.bashrc
    cd CAP2/
    git checkout feat/single-ended-reads
    python setup.py develop
    cd
    mkdir workdir
    cd workdir
    cap2 --help


