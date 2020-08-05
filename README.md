# CAP2

[![CircleCI](https://circleci.com/gh/MetaSUB/CAP2.svg?style=svg)](https://circleci.com/gh/MetaSUB/CAP2)

This repository contains code for the second version of the MetaSUB Core Analysis Pipeline. This pipeline is in Beta and under development.

## Major Changes and Goals

There are a number of important changes from CAP1 to CAP2
 - Replaced Snakemake with Luigi as a pipeline framework
 - Aiming to keep maximum RAM usage under 128GB
 - More aggressive quality control pipeline
 - Better incorporation of dynamic data (e.g. metagenome assembled genomes)
 - Unit testing for modules
 - More in depth assembly pipeline
 - Better strain calling

## Installation, Testing, and Running

To install and run tests download the repo and run the setup command
```
python setup.py develop
python -m pytest tests
```

To run CAP2 use the command `cap2 --help` which will show you all available options. To process a group of samples you will need to give the CAP2 a file with three columns: the sample name, a path to the read 1 fastq file, a path to the read 2 fastq file.

### Configuration

By default CAP2 downloads all necessary programs and databases when it is run. For users running CAP2 multiple times on the same system it will be beneficial to set up configuration so that downloads only occur once.

Configuration consists of setting three environmental variables. These shoudl go in your `.bashrc` or equivalent.

```
CAP2_DB_DIR=<some local path...>
CAP2_CONDA_SPEC_DIR=<some local path...>
CAP2_CONDA_BASE_PATH=<some local path...>
```

You can also use a yaml configuration file. See `cap2/pipeline/config.py` for details and all options.


## Tools and Status

| Stage      | Step             | Command Written | Tests Written | Tests Passing |
| ---------- | ---------------- | --------------- | ------------- | ------------- |
| QC         | count reads      | x               | x             | x             |
| QC         | host removal     | x               | x             | x             |
| QC         | fastqc           | x               | x             | x             |
| QC         | error correction | x               | x             | x             |
| QC         | contam removal   |                 |               |               |
| QC         | id contams       |                 |               |               |
| QC         | id controls      |                 |               |               |
| QC         | score controls   |                 |               |               |
| QC         | remove adapters  |                 |               |               |
| QC         | remove dupes     |                 |               |               |
|            |                  |                 |               |               |
| Short Read | MASH Sketch      | x               | x             | x             |
| Short Read | HMP Similarity   | x               | x             | x             |
| Short Read | Uniref90 Align   | x               | x             | x             |
| Short Read | read stats       | x               | x             | x             |
| Short Read | Microbe Census   | x               | x             |               |
| Short Read | HUMAnN2          | x               | x             | x             |
| Short Read | Kraken2          | x               | x             | x             |
| Short Read | Strain Calling   |                 |               |               |
| Short Read | GRiD             |                 |               |               |
| Short Read | EMP Similarity   |                 |               |               |
| Short Read | MetaSUB Sim.     |                 |               |               |
| Short Read | Groot AMR        | x               | x             |               |
|            |                  |                 |               |               |
| DB         | Uniref90         | x               | x             | x             |
| DB         | host removal     | x               | x             | x             |
| DB         | HMP Similarity   | x               | x             | x             |
| DB         | Kraken2          | x               | x             | x             |
|            |                  |                 |               |               |
| Contig     | MetaSPAdes       | x               | x             | x             |
| Contig     | Progdigal        | x               |               |               |
| Contig     | Deep BGC         |                 |               |               |
| Contig     | Taxonomy         |                 |               |               |
| Contig     | PlasFlow         |                 |               |               |
| Contig     | AMR Id           | x               |               |               |
| Contig     | MetaBAT2         |                 |               |               |
| Contig     | Genome Binning   |                 |               |               |

## Notes

 - `samtools` needed to be manually installed on my mac
 - several tests still failing on CircleCI but passing on my laptop, not clear why. Maybe something to do with perl

## License

MIT License
