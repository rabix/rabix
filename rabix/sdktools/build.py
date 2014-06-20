from os import makedirs, getenv, chmod
from os.path import exists, join, basename, isdir

import os
import docker
import subprocess
import re
import keyword
import stat

from rabix import VERSION


DOCKER_FILE = """
FROM {base}

ENV HOME /root
RUN mkdir /wrappers
ADD . /wrappers
RUN /wrappers/build/build.sh

ENTRYPOINT /usr/local/bin/rabix-adapter
"""

SETUP = """
import io
from setuptools import setup, find_packages


setup(
    name="{name}",
    version="0.1.0",
    include_package_data=True,
    packages=find_packages(),
    install_requires=[
        x.strip() for x in
        io.open('requirements.txt', 'r', encoding='utf-8')
    ]
)

"""

BUILD_SH = """
#!/bin/sh

pip install --no-index --find-links /wrappers/build/deps /wrappers
"""

FETCH_DEPS = ["pip", "install", "--download", "build/deps",
              "--requirement", "requirements.txt"]


def init(work_dir, base_image, force=False):

    name = sanitize_name(basename(work_dir))

    build_dir = join(work_dir, "build")
    build_sh_path = join(build_dir, "build.sh")
    deps_dir = join(build_dir, "deps")
    dockerfile_path = join(work_dir, "Dockerfile")
    requirements_path = join(work_dir, "requirements.txt")
    setup_path = join(work_dir, "setup.py")
    package_dir = join(work_dir, name)
    init_path = join(package_dir, "__init__.py")

    paths = [dockerfile_path, requirements_path,
             setup_path, init_path, build_sh_path]

    dirs = [build_dir, deps_dir, package_dir]

    conflict = any(map(exists, paths)) or \
        any(map(lambda dir: exists(dir) and not isdir(dir), dirs))

    if conflict and not force:
        raise RuntimeError("Build already initialized. Use the force.")

    if not exists(deps_dir):
        makedirs(deps_dir)

    if not exists(package_dir):
        makedirs(package_dir)

    with open(dockerfile_path, "w") as dockerfile:
        dockerfile.write(DOCKER_FILE.format(base=base_image))

    with open(requirements_path, "w") as requirements:
        requirements.write("rabix=={}".format(VERSION))

    with open(setup_path, "w") as setup:
        setup.write(SETUP.format(name=name))

    with open(init_path, "w") as init:
        init.write("\n")

    with open(build_sh_path, "w") as build_sh:
        build_sh.write(BUILD_SH)
    chmod_plus(build_sh_path, stat.S_IEXEC)
    # fetch_deps(work_dir)


def fetch_deps(work_dir):
    subprocess.check_call(FETCH_DEPS, cwd=work_dir)


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
