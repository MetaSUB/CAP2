# Containerization


## Running the MCAP with Docker


To run the MCAP with docker you will need to create two directories: 1) one where databases should be stored on your machine's filesystem and 2) one where the output from the mcap should go. For simplicity we will call these 1) `/my/databases` and `/my/outputs`.

To run the MCAP use this command:

```
docker run \
	--mount source=/my/databases,target=/mcap/dbs,type=bind,readonly \
	--mount source=/my/outputs,target=/mcap/out,type=bind \
	-it mcap \
	cap2 --help
```

This command will print a help message and then exit. To run the mcap pipeline replace `cap2 --help` with a more complete command.


### Using external databases

Typically you won't want to download new databases every time you use the MCAP. Unfortunately the databases used by the MCAP are too large to fit comfortably into a docker image. As a work around the MCAP docker image can be set to look for already existing databases in your machine's filesystem. To do this you need to 1) load databases onto your local filesystem, 2) instruct docker to connect to these databases when it runs.

#### Loading databases to your local filesystem


#### Letting docker connect to your databases

Suppose you have the MCAP databases downloaded into a directory on your local filesystem called `/path/to/mcap/databases`. You can instruct docker to connect to this folder at runtime using the following command:

```
docker run \
	--mount source=/path/to/mcap/databases,target=/mcap/dbs,type=bind,readonly \
	-it mcap \
	/bin/bash

```

This command will make it possible for commands run in this docker image to read data from `/path/to/mcap/databases`. This command is read-only meaning the docker image will not be able to edit files.