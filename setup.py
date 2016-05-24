#!/usr/bin/env python
#
# Example usage
# - Install:
#   python setup.py install
# - Developer install
#   python setup.py develop --user
#
# We use the setup tools import as opposed to the standard "from
# distutils.core import setup" in order to add the bdist_egg command.
#

from setuptools import setup

setup(
    name='pypipeline',
    version='0.1.0',
    description='Python scripts for running pipelines of experiments locally or on Sun Grid Engine.',
    long_description='Python scripts for running pipelines of experiments locally or on Sun Grid Engine.',
    author='Matt Gormley',
    author_email='mrg@cs.jhu.edu',
    url='https://github.com/mgormley/pypipeline',
    license='MIT',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
    ],
    packages=['pypipeline'],
    scripts=['scripts/relaunch.py',
             'scripts/scrape_exps.py'],
    install_requires=['fabric>=1.1.0,<2.0.0'],
    )
