## Reproducible Analyses for Bioinformatics

Rabix is an open source implementation of the specification being developed on the
[Common Workflow Language mailing list](https://groups.google.com/forum/#!forum/common-workflow-language).

CWL is an informal task force consisting of people from various organizations
that have an interest in portability of bioinformatics workflows.
The goal is to specify a way to describe bioinformatics tools and workflows that is powerful,
easy to use and allows for portability of tools/workflows and reproducibility of runs.

Version 0.7 is approaching full compatibility to
[draft2](http://common-workflow-language.github.io/draft-2)
specification.
To play with describing tools and making workflows visit [rabix.org](http://rabix.org).

This repo includes a local python executor and some utilities for building docker images.


### Install

#### Using VirtualBox and Vagrant

We've pre-installed rabix and dependencies on a VirtualBox machine image.
If you install VirtualBox and Vagrant, you can run the machine using the vagrantfile:

```
wget https://s3.amazonaws.com/rabix/Vagrantfile
vagrant up && vagrant ssh
# Example BWA run with local files
sudo pip install -U rabix
cd rabix/examples
rabix https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json -- --reads test-data/example_human_Illumina.pe_1.fastq --reads test-data/example_human_Illumina.pe_2.fastq --reference test-data/chr20.fa
```

#### On an EC2 instance

If you launch an instance on AWS in the us-east region, you can use the public AMI `ami-60644508` which has rabix and dependencies installed. SSH to the instance using "rabix" as username and password.

To run the BWA example:

```
sudo pip install -U rabix
cd examples
rabix https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json -- --reads test-data/example_human_Illumina.pe_1.fastq --reads test-data/example_human_Illumina.pe_2.fastq --reference test-data/chr20.fa
```

#### Linux

Rabix requires Python 2.7 or 3.x to run.

There are several external dependencies for rabix.
The first one is [Docker](https://docs.docker.com/installation/#installation) (v1.5 or later)
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
$ pip install rabix
```

If you are using Anaconda there might be an issue with a version of `requests`,
so you should create separate environment with requests 2.2.1,
prior to running `pip`:

```
$ conda create -n rabix pip requests=2.2.1
$ source activate rabix
```

Try running `rabix` command to see if everything went well.


### Basic usage

There are two executables installed with rabix package: `rabix` - the executor of apps and workflows, and `rabix-tools` which is command line suit with various utilities.

Main argument for `rabix` command is a URI (local file path or HTTP URL) to a JSON document that describes an app.
Rabix supports
[JSON pointer](http://tools.ietf.org/html/rfc6901)
spec, so you can reference a description within larger JSON document: `rabix "apps.json#my_app_3"`


When you run `rabix` with tool description document, you'll be presented with arguments you need to fill in in order to run it so for example:

```
rabix https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json
```

will produce the following output:

    Usage:
    rabix <tool> [-v...] [-hcI] [-d <dir>] [-i <inp>] [--resources.mem=<int> --resources.cpu=<int>]
      [-- --reads=<file>... --reference=<file> [--minimum_seed_length=<integer>] [--min_std_max_min=<array_number_separator(None)>...]...]


We see a lot of options here, but ideally, we'll be dealing with workflows that have lot of options pre-populated, so that we only need to supply few additional arguments such as yours input files or similar.

Let's try running this tool with some example files:

```
$ rabix -v https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json -- \
 --reference https://s3.amazonaws.com/rabix/rabix-test/chr20.fa \
 --reads https://s3.amazonaws.com/rabix/rabix-test/example_human_Illumina.pe_1.fastq \
 --reads https://s3.amazonaws.com/rabix/rabix-test/example_human_Illumina.pe_2.fastq
```

Once again, we can supply either paths to local files or HTTP URLs.

Of course of we wanted to type down command line arguments for command line programs, we wouldn't need a middleman. Here is a more meaningful example:

```
rabix -v -i https://s3.amazonaws.com/rabix/rabix-test/inputs-workflow-remote.json \
  https://s3.amazonaws.com/rabix/rabix-test/bwa-mem-workflow.json
```

Now we have entire workflow defined in one file and all the options we want to apply to that workflow in another, specified after `-i` option.
These are simple JSON files that you can write yourself, or more conveniently, use UI on [rabix.org](http://rabix.org).

When running an app, rabix will look for a docker images specified in the document, pull them and run according to description.
If for some reason you want to pre-fetch required images, you can use "--install" switch:


```
$ rabix --install https://s3.amazonaws.com/rabix/rabix-test/bwa-mem.json
```


### How to contribute

As mentioned earlier, rabix is an implementation of Common Workflow Language, plus a playground for future ideas.
First question is whether your intended contribution related to CWL in general or rabix in particular.
The way you should decide this is whether it's related to overall functionality of tools, workflows and document formats (CWL)
or some aspect of implementation, auxiliary utilities, etc. (rabix).
If former, you should probably present your idea on the mailing list or submit an issue to the
[Common Workflow Language](https://github.com/common-workflow-language/common-workflow-language)
project.

If you are interested in contributing to rabix, feel free to submit issues and pull requests.
If you are contributing code, pay attention to "devel" branch as it's the place where feature branches get merged.
We will merge thing to master branch periodically in sync with releases on PyPI.
