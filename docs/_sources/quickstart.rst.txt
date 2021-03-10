Quickstart
==========


Installing the CAP
^^^^^^^^^^^^^^^^^^

To install download the repo and run the setup command

.. code-block:: bash

    python setup.py install
    cap2 --help


Installing Subprograms
----------------------

By default the CAP installs all necessary subprograms itself. If a particular version of a subprogram is required it is usually possible to override the defaults and supply the program as a parameter.


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


Databases
^^^^^^^^^

The CAP uses a number of large reference databases. Building and indexing these databases is considered a first-class part of the pipeline and :ref:`modules are included<Databases>` to do so. However, in most cases users will simply want to download prebuilt version of the databases rather than build them from scratch. This is the pipeline default and will happen automatically.

To preload databases on a machine run this from the command line:

.. code-block:: bash

    cap2 run db -c /path/to/config/file

or from python

.. code-block:: python

    from .api import run_db_stage

    config = /path/to/config/file
    run_db_stage(config_path=config, cores=1, workers=1)
