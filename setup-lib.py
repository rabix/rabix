from distutils.core import setup
import sys
import os

sys.path.append(os.path.dirname(__file__))
from rabix import __version__


setup(
    name="rabix-lib",
    version=__version__,
    packages=['rabix', 'rabix.common', 'rabix.sdk'],
    scripts=['scripts/rabix-adapter'],
    long_description="",
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
