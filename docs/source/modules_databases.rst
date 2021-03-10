Databases
=======
.. _Databases:
.. figure:: _static/subpipelines/dbs.jpeg
   :width: 600


Modules
^^^^^^^

Mouse Removal Database
----------------------

The CAP uses `mm39 <ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCA/000/001/635/GCA_000001635.9_GRCm39/GCA_000001635.9_GRCm39_genomic.fna.gz>`_ for mouse removal. The raw (fasta) genome is indexed using Bowtie2.

.. autoclass:: cap2.pipeline.databases.MouseRemovalDB

Human Removal Database
----------------------


The CAP uses `Hg38 with alternate contigs <https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/>`_. for human removal. The raw (fasta) genome is indexed using Bowtie2. Th rationale for picking this genome is based on `this <https://lh3.github.io/2017/11/13/which-human-reference-genome-to-use>`_.

.. autoclass:: cap2.pipeline.databases.HumanRemovalDB

Kraken2 Taxonomic Databases
---------------------------

The CAP uses built in tools from Kraken2 to build a databse. The CAP2 taxonomic databases includes archaea, bacteria, plasmids, viruses, fungi, protozoa, and human.

.. autoclass:: cap2.pipeline.databases.Kraken2DB