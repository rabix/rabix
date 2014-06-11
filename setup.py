from setuptools import setup, find_packages


setup(
    name="rabix",
    version='0.1.0',
    include_package_data=True,
    packages=find_packages(),
    entry_points={
        'console_scripts': ['rabix = rabix.runtime.cli:main'],
    },
    install_requires=[
        'nose==1.3.0', 'docker-py==0.3.1', 'docopt==0.6.1', 'requests==2.3.0', 'networkx==1.9rc1', 'pep8==1.5.7'
    ]
)
