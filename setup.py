from setuptools import setup, find_packages
import sys
import os

sys.path.append(os.path.dirname(__file__))
from rabix import VERSION

with open('README.md') as fp:
    README = fp.read()

with open('requirements.txt') as fp:
    REQS = [line.rstrip('\n\r') for line in fp.readlines() if line.rstrip('\n\r')]

setup(
    name="rabix",
    version=VERSION,
    include_package_data=True,
    packages=find_packages(),
    entry_points={
        'console_scripts': ['rabix = rabix.runtime.cli:main'],
    },
    install_requires=REQS,
    long_description=README,
    zip_safe=False,
    test_suite='tests',
    license='AGPLv3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Science/Research',
        'Operating System :: POSIX :: Linux',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7'
        'Topic :: Scientific/Engineering :: Bio-Informatics',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)'
    ]
)
