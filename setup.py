import os
import io
import sys
from setuptools import setup, find_packages

sys.path.append(os.path.dirname(__file__))
from rabix import __version__


setup(
    name="rabix",
    version=__version__,
    include_package_data=True,
    packages=find_packages(),
    entry_points={
        'console_scripts': ['rabix = rabix.runtime.cli:main'],
    },
    install_requires=[
        x.strip() for x in
        io.open('requirements.txt')
    ],
    long_description=io.open('README.md').read(),
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
        'License :: OSI Approved :: ' +
            'GNU Affero General Public License v3 or later (AGPLv3+)'
    ]
)
