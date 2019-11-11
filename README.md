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

## License

MIT License
