![Build Status](https://travis-ci.org/rabix/rabix.svg?branch=devel)

## Reproducible Analyses for Bioinformatics 

Rabix is an open source implementation of the specification being developed on the
[Common Workflow Language mailing list](https://groups.google.com/forum/#!forum/common-workflow-language).

CWL is an informal task force consisting of people from various organizations
that have an interest in portability of bioinformatics workflows.
The goal is to specify a way to describe bioinformatics tools and workflowsthat is powerful,
easy to use and allows for portability of tools/workflows and reproducibility of runs.

Version 0.5 should be compatible with
[draft1](https://github.com/common-workflow-language/common-workflow-language/blob/draft-1/specification/tool-description.md)
specification.
To play with describing tools and making workflows visit [rabix.org](http://rabix.org).

This repo includes a local python executor and some utilities for building docker images.


### Install

Rabix requires Python 2.7 or 3.x to run.

There are several external dependencies for rabix.
The first one is [Docker](https://docs.docker.com/installation/#installation)
for running command line bioinformatics tools.
Second one is a JavaScript interpreter: 
you can look for available options on
[PyExecJS](https://github.com/doloopwhile/PyExecJS),
but the easiest way is probably to install something like PhantomJS or NodeJS
from your distro's repo.
Finally you should install `libyaml` development package.

If you are running recent Ubuntu (14.04 or newer),
the following should setup your system:

```
$ sudo apt-get install python-dev python-pip docker.io phantomjs libyaml-dev
```

although you'll probably want to install newer version of Docker from the above link.
 
Now we can install rabix via `pip`:

```
$ pip install git+https://github.com/rabix/rabix
```

If you are using Anaconda there might be an issue with a version of `requests`,
so you should create separate environment with requests 2.2.1,
prior to running `pip`:
 
```
$ conda create -n rabix pip requests=2.2.1
$ source activate rabix
```

Check if everything works by installing an example tool:

```
$ rabix --install https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json#tool
```

The "install" command simply pre-fetches referenced docker image.
If you don't want to download hundreds of megabytes, you can just attempt to
run the tool, which should fetch only the JSON files and present you with
input options:
 
```
$ rabix https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json#tool 
```

Optionally, run the tool with some example data. It won't take long:

```
$ rabix https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json#tool -- \
 --reference https://s3.amazonaws.com/rabix/rabix-test/chr20.fa \
 --reads https://s3.amazonaws.com/rabix/rabix-test/example_human_Illumina.pe_1.fastq \
 --reads https://s3.amazonaws.com/rabix/rabix-test/example_human_Illumina.pe_2.fastq
```
