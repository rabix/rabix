![Build Status](https://travis-ci.org/rabix/rabix.svg?branch=devel)

## Reproducible Analyses for Bioinformatics 

The goal of this project is to provide an easy way to package, distribute and run tools and pipelines
using [docker](http://docker.com) images.

There are three main components:

* Format and protocol specification.
* Tools to run apps and pipelines.
* An SDK to easily package tools and pipelines into reusable components.

Note that this project is still under development. You can run pipelines, but can't (easily) make them yet.

### Install

First, [install docker](https://docs.docker.com/installation/#installation) on a linux machine.
 
Second, install rabix via pip:

```
$ pip install git+https://github.com/rabix/rabix
```

Check if everything works by installing an example pipeline:

```
$ rabix install https://s3.amazonaws.com/boysha/pipeline_test_bwa_freebayes.json
```

The "install" command simply pre-fetches referenced docker images.
If you don't want to download hundreds of megabytes, you can just attempt to run the pipeline,
which should fetch only the JSON files and present you with input options:
 
```
$ rabix run https://s3.amazonaws.com/boysha/pipeline_test_bwa_freebayes.json 
```

Optionally, run the pipeline with some example data. It won't take long:

```
$ rabix run https://s3.amazonaws.com/boysha/pipeline_test_bwa_freebayes.json \
  --reference https://s3.amazonaws.com/boysha/testfiles/example_human_reference.fasta \
  --read https://s3.amazonaws.com/boysha/testfiles/example_human_Illumina.pe_1.fastq \
  --read https://s3.amazonaws.com/boysha/testfiles/example_human_Illumina.pe_2.fastq
```
