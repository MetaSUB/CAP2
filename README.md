# CAP2

[![CircleCI](https://circleci.com/gh/MetaSUB/CAP2.svg?style=svg)](https://circleci.com/gh/MetaSUB/CAP2)

This repository contains code for the second version of the MetaSUB Core Analysis Pipeline. This pipeline is not complete.

## Major Changes and Goals

There are a number of important changes from CAP1 to CAP2
 - Replaced Snakemake with Luigi as a pipeline framework
 - Aiming to keep maximum RAM usage under 128GB
 - More aggressive quality control pipeline
 - Better incorporation of dynamic data (e.g. metagenome assembled genomes)
 - Unit testing for modules
 - More in depth assembly pipeline
 - Better strain calling

## Installation and Testing

```
python setup.py develop
python -m pytest tests
```

## Tools and Status

| Stage      | Step             | Command Written | Tests Written | Tests Passing |
| ---------- | ---------------- | --------------- | ------------- | ------------- |
| QC         | count reads      | x               | x             | x             |
| QC         | host removal     | x               | x             |               |
| QC         | fastqc           | x               | x             |               |
| QC         | error correction |                 |               |               |
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
| Short Read | Microbe Census   | x               | x             |               |
| Short Read | HUMAnN2          | x               |               |               |
| Short Read | KrakenUniq       | x               |               |               |
| Short Read | Strain Calling   |                 |               |               |
| Short Read | GRiD             |                 |               |               |
| Short Read | EMP Similarity   |                 |               |               |
| Short Read | MetaSUB Sim.     |                 |               |               |
| Short Read | MegaRes Align    |                 |               |               |
| Short Read | CARD Align       |                 |               |               |
| Short Read | SAKEIMA Sketch   |                 |               |               |
| Short Read | read stats       |                 |               |               |
|            |                  |                 |               |               |
| DB         | Uniref90         | x               | x             | x             |
| DB         | host removal     | x               | x             | x             |
| DB         | HMP Similarity   | x               | x             | x             |
| DB         | KrakenUniq       | x               |               |               |
|            |                  |                 |               |               |


## License

MIT License
