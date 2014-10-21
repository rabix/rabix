![Build Status](https://travis-ci.org/rabix/rabix.svg?branch=devel)

## Reproducible Analyses for Bioinformatics 

The goal of this project is to provide an easy way to package, distribute and
run tools and pipelines using [docker](http://docker.com) images.

There are three main components:

* Format and protocol specification.
* Tools to run apps and pipelines.
* An SDK to easily package tools and pipelines into reusable components.

Note that this project is still under development. You can run pipelines, but
can't (easily) make them yet.

### Install

First, [install docker](https://docs.docker.com/installation/#installation) on
a linux machine.
 
Second, install rabix via pip:

```
$ pip install git+https://github.com/rabix/rabix
```

Check if everything works by installing an example tool:

```
$ rabix --install --tool https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json#tool
```

The "install" command simply pre-fetches referenced docker image.
If you don't want to download hundreds of megabytes, you can just attempt to
run the tool, which should fetch only the JSON files and present you with
input options:
 
```
$ rabix --tool https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json#tool 
```

Optionally, run the tool with some example data. It won't take long:

```
$ rabix --tool https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json#tool \
  --reference path/to/reference \
  --reads path/to/read1 \
  --reads path/to/read2
```

You can also provide job JSON file with specified paths to files in it.
For this example you need to be in rabix directory.

```
$ rabix --job https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json#job \ 
```