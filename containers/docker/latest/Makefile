
build:
	docker buildx build --platform linux/amd64  -t mcap .

shell: build
	docker run \
		--mount source=`pwd`/dbs,target=/mcap/dbs,type=bind,readonly \
		--mount source=`pwd`/out,target=/mcap/out,type=bind \
		-it mcap \
		/bin/bash

mcap: build
	docker run \
		--mount source=`pwd`/dbs,target=/mcap/dbs,type=bind,readonly \
		--mount source=`pwd`/out,target=/mcap/out,type=bind \
		-it mcap \
		cap2 --help