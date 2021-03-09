CAPalyzer
=========

The CAPalyzer is a package to parse and process the results produced by the main CAP pipeline. It performs two key tasks:

- builds data tables that summarize the results of modules for multiple samples
- parses those data tables and provides processing tools

API
^^^

Table Builder
-------------

.. autoclass:: cap2.capalyzer.table_builder.cap_table_builder.CAPFileSource
    :members:
    :undoc-members:


.. autoclass:: cap2.capalyzer.table_builder.CAPTableBuilder
    :members:
    :undoc-members:


.. automodule:: cap2.capalyzer.table_builder.parsers
    :members:
    :undoc-members: