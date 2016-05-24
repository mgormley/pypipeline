PyPipeline
==========

PyPipeline, a suite of python scripts for running pipelines of
experiments locally or on Sun Grid Engine.

Installation
------------

Standard install: This will install the latest version of pypipeline from PyPI.

::

    pip install pypipeline

Development
-----------

If you are actively developing against the GitHub source, you may
prefer a developer install.

Developer install for user: This install will just create a link to the
source directory. On a typical system this will result in the egg link
being installed to ~/.local/lib/python2.7/site-packages/

::

    python setup.py develop --user

For a system wide developer install:

::

    sudo pip install -e .
