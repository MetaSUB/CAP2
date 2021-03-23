Architecture
============

Like most pipelines the CAP is a collection of subprograms strung together. These subprograms are organized into modules and modules are organized into subpipelines.

Luigi
^^^^^

The MetaSUB CAP is written using `Luigi <https://github.com/spotify/luigi>`_. Luigi is a generic pipelining system that handles scheduling tasks, running jobs, and associated tasks. Unlike some other pipelineing systems Luigi does not use a domain specific language and instead accepts pipelines written in pure python. The MetaSUB CAP uses the base version of Luigi without modifications.

Luigi provides a centralized scheduler server that can be used to coordinate jobs across multiple worker machines. This scheduler ensures jobs will not be run multiple times and provides a GUI view of pipeline progress. The CAP can use this scheduler but it is not required.

Modules
^^^^^^^

Modules typically encapsulate a single subprogram. Conceptually modules are the basic building block of the pipelines that provide a single coherent scientific analysis. Modules can depend on other modules as well as tasks like software installation.

Programatically modules are written as a python class, typically with one module class per file. Modules classes are subclasses of a `BaseCapTask` class which is itself a subclass of `luigi.Task`. Each module class provides a set of methods that provide important information about the module. An 'instance' of a module is a specific case where a module is instantiated on some real data while we use the term 'module' to refer to a generic step in the pipeline that could be instantiated.

An API reference for Modules can be found :ref:`here<captaskcode>`


Sub-Pipelines
^^^^^^^^^^^^^

Sub-pipelines are an organizational tool to group subsections of the pipeline. There are five subpipelines in the CAP currently.

- :ref:`Quality Control<Quality Control>`
- :ref:`Preprocessing<Preprocessing>`
- :ref:`Short Read<Short Read>`
- :ref:`Assembly<Assembly>`
- :ref:`Databases<Databases>`

There are also several experimental subpipelines. These are tools that are being actively developed and might become part of the CAP eventually. These pipelines provide additional functionality and a place to test new ideas.

