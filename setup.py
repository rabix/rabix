import io
import sys

from os.path import dirname
from setuptools import setup, find_packages
from pip.req import parse_requirements

sys.path.append(dirname(__file__))
from rabix import __version__

install_reqs = parse_requirements(
    'requirements-{}.txt'.format(sys.version_info[0]), session=False
)
requires = [str(ir.req) for ir in install_reqs]

setup(
    name="rabix",
    version=__version__,
    packages=find_packages(),
    entry_points={
        'console_scripts': ['rabix = rabix.main:main',
                            'rabix-tools = rabix.tools.cli:main'],
    },
    install_requires=requires,
    package_data={'': ['*.expr-plugin']},
    long_description=io.open('README.md').read(),
    description='Reproducible Analyses for Bioinformatics',
    zip_safe=False,
    test_suite='tests',
    license='AGPLv3',

    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: ' +
        'GNU Affero General Public License v3 or later (AGPLv3+)'
    ]
)
