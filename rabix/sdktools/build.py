
DOCKER_FILE = """
FROM {base}

ENV HOME /root
RUN mkdir /build
ADD . /build
RUN /build/run.sh
"""


def configure(base):

    # mkdir build
    # write Dockerfile
    # populate build (if not present?)
    #
    pass


def build():
    # run docker build
    pass
