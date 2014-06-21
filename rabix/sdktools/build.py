from os import makedirs, getenv, chmod
from os.path import exists, join, basename, isdir

import os
import docker
import subprocess
import re
import keyword
import stat

from rabix import __version__


DOCKER_FILE = """
FROM {base}

ENV HOME /root
RUN mkdir /wrappers
ADD . /wrappers
RUN /wrappers/build/build.sh

ENTRYPOINT /usr/local/bin/rabix-adapter
"""

SETUP = """
from distutils.core import setup


setup(
    name="{name}",
    version="0.1.0",
    packages=["{name}"]
)

"""

BUILD_SH = """
#!/bin/sh -e

cd /wrappers/build
tar xzf rabix-lib-{version}.tar.gz
cd rabix-lib-{version}

PYTHON=$(which python || which python3 || which python2)

$PYTHON setup-lib.py install

cd /wrappers
$PYTHON setup.py install

rm -rf /tmp/* /var/tmp/*
rm -rf /wrappers
"""


def init(work_dir, base_image, force=False):

    name = sanitize_name(basename(work_dir))

    build_dir = join(work_dir, "build")
    build_sh_path = join(build_dir, "build.sh")
    dockerfile_path = join(work_dir, "Dockerfile")
    setup_path = join(work_dir, "setup.py")
    package_dir = join(work_dir, name)
    init_path = join(package_dir, "__init__.py")

    paths = [dockerfile_path, setup_path, init_path, build_sh_path]

    dirs = [build_dir, package_dir]

    conflict = any(map(exists, paths)) or \
        any(map(lambda dir: exists(dir) and not isdir(dir), dirs))

    if conflict and not force:
        raise RuntimeError("Build already initialized. Use the force.")

    if not exists(build_dir):
        makedirs(build_dir)

    if not exists(package_dir):
        makedirs(package_dir)

    with open(dockerfile_path, "w") as dockerfile:
        dockerfile.write(DOCKER_FILE.format(base=base_image))

    with open(setup_path, "w") as setup:
        setup.write(SETUP.format(name=name))

    with open(init_path, "w") as init:
        init.write("\n")

    with open(build_sh_path, "w") as build_sh:
        build_sh.write(BUILD_SH.format(version=__version__))
    chmod_plus(build_sh_path, stat.S_IEXEC)
    # TODO: materialize sdk-lib tarbal in build dir somehow

def build(work_dir, tag=None):
    docker_host = getenv("DOCKER_HOST")
    client = docker.Client(docker_host)
    return client.build(work_dir, rm=True, tag=tag)


def sanitize_name(name):
    sanitized = name.replace('-', '_').lower()

    valid = (not keyword.iskeyword(sanitized)) and \
        re.match('^[a-z][a-z0-9_]*$', sanitized)

    if not valid:
        raise RuntimeError("Invalid name. " +
                           "Project name must be a valid Python identifier")
    return sanitized


def chmod_plus(path, mod):
    st = os.stat(path)
    chmod(path, st.st_mode | mod)
