#!/usr/bin/env python
#
# Example usage (to build an egg):
#   python setup.py bdist_egg
#
# We use the setup tools import as opposed to the standard "from
# distutils.core import setup" in order to add the bdist_egg command.
#

from setuptools import setup

setup(
    name='experiments-core',
    version='0.1',
    description='Experiment running library',
    author='Matt Gormley',
    author_email='mrg@cs.jhu.edu',
    url='http://www.cs.jhu.edu/~mrg/',
    packages=['experiments', 'experiments.core'])
