Quality Control
=======

.. _Quality Control:
.. figure:: _static/subpipelines/qc.jpeg
   :width: 600


The Quality Control subpipeline checks incoming reads to ensure they are high quality. It runs directly on the raw reads.


Modules
^^^^^^^

FastQC
------

`FastQC <https://www.bioinformatics.babraham.ac.uk/projects/fastqc/>`_ provides simple quality control checks on raw sequence data. It provides a set of analyses that help researchers diagnose any downstream issues or outliers. 

FastQC produces two files. An standalone HTML file with diagnostic graphs and a zip file with the intermeidate data used to build the HTML report. An example of the output files from this module may be found on `Pangea <https://pangeabio.io/samples/bb81df30-50ab-442b-b799-3322e48bf740/analysis-results/e49c8df7-6c52-4386-a87e-7709d9c22411>`_.


.. autoclass:: cap2.pipeline.preprocessing.FastQC