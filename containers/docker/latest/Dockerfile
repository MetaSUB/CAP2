FROM --platform=linux/amd64 ubuntu:20.04

WORKDIR /mcap
RUN mkdir -p /mcap/dbs /mcap/demo

# Necessary for R
ENV TZ=America/New_York
ENV DEBIAN_FRONTEND=noninteractive
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone


RUN apt-get update -y && apt-get install -y \
	bzip2 \
	build-essential \
	zlib1g-dev \
	libc6 \
	libncurses5-dev \
	libncursesw5-dev \
	libnss-sss \
	libbz2-dev \
	liblzma-dev \
	less \
	libcurl4-openssl-dev \
	wget \
	unzip \
	zip \
	r-base \
	r-base-core \
	r-recommended \
	default-jre \
	default-jdk \
	python \
	python3 \
	python3-pip \
	mafft \
	curl \
	rsync \
	git && \
	rm -rf /var/lib/apt/lists/*

# Install Miniconda
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"
RUN wget \
    https://repo.anaconda.com/miniconda/Miniconda3-py38_4.10.3-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-py38_4.10.3-Linux-x86_64.sh -b \
    && rm -f Miniconda3-py38_4.10.3-Linux-x86_64.sh
RUN conda --version

# Install software with conda
# Based on https://jcristharif.com/conda-docker-tips.html
RUN conda install --yes -c bioconda \
	numpy \
	pandas \
	scipy

RUN pip install \
	pangea_api \
	Jinja2==3.0.0a1 \
	biopython==1.76 \
	click==6.7 \
	pysam \
	python-louvain

RUN pip install \
	gimmebio.seqs \
	bloom_filter \
    luigi==3.0.0b2 \
    PyYaml==5.3.1

RUN conda install --yes -c conda-forge matplotlib

RUN conda clean -afy

RUN pip install cap2==0.4.1

RUN cap2 --help

ADD demo /mcap/demo/
WORKDIR /mcap/demo
RUN cap2 run pipeline --stage pre manifest.txt


WORKDIR /mcap
COPY mcap_config.yaml /mcap/config.yaml
ENV CAP2_CONFIG=/mcap/config.yaml
RUN mkdir -p /mcap/out


CMD ["bash"]