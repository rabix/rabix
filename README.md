![Build Status](https://travis-ci.org/rabix/rabix.svg?branch=devel)

## Reproducible Analyses for Bioinformatics 

The goal of the rabix toolkit is easily capture and disseminate computational analyses. Our approach is to use [docker](http://docker.com) images to package the tools and describe tools, pipelines and analyses using easily shareable JSON files.


Eventually, we plan to have the following:

* Schema and protocol specification.
* Tools to run apps and pipelines.
* An SDK to easily package tools and pipelines into reusable components.

Note that this project is still in early development. You can run some pipelines, but can't (easily) make them yet and the document schemas keep changing as we explore different options.

### Install

First, [install docker](https://docs.docker.com/installation/#installation) on a linux machine.
 
Second, install rabix via pip:

```
$ pip install git+https://github.com/rabix/rabix
```

Use the run command to fetch an example pipeline and see input options:

```
$ rabix run https://s3.amazonaws.com/boysha/pipeline_test_bwa_freebayes.json 
```


Optionally "install" the pipeline (pre-fetch the docker images):

```
$ rabix install https://s3.amazonaws.com/boysha/pipeline_test_bwa_freebayes.json
```

Run the pipeline with some example data. It won't take long:

```
$ rabix run https://s3.amazonaws.com/boysha/pipeline_test_bwa_freebayes.json \
  --reference https://s3.amazonaws.com/boysha/testfiles/example_human_reference.fasta \
  --read https://s3.amazonaws.com/boysha/testfiles/example_human_Illumina.pe_1.fastq \
  --read https://s3.amazonaws.com/boysha/testfiles/example_human_Illumina.pe_2.fastq
```
