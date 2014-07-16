PyPipeline, a suite of python scripts for running pipelines of
experiments locally or on Sun Grid Engine.

Standard install: This will do a standard install of an egg with a fixed version.

    sudo python setup.py install

Developer install for user: This install will just create a link to the source directory.
    
    python setup.py develop --user

For a system wide developer install:
    
    sudo pip install -e .