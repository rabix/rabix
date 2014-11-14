![Build Status](https://travis-ci.org/rabix/rabix.svg?branch=devel)

## Reproducible Analyses for Bioinformatics 

Rabix is an open source implementation of the specification being developed on the
 [Common Workflow Language mailing list](https://groups.google.com/forum/#!forum/common-workflow-language).

CWL is an informal task force consisting of people from various organizations that have an interest in portability
 of bioinformatics workflows.
The goal is to specify a way to describe bioinformatics tools and workflows that is powerful,
 easy to use and allows for portability of tools/workflows and reproducibility of runs.

To play with describing tools and making workflows visit [rabix.org](http://rabix.org).

This repo includes a local python executor (currently only running tools) and some utilities for building docker images.


### Install

First, [install docker](https://docs.docker.com/installation/#installation) and [Node.js](http://nodejs.org/) on
a linux machine.
 
Second, install rabix via pip:

```
$ pip install git+https://github.com/rabix/rabix
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
