PyPipeline, a suite of python scripts for running pipelines of
experiments locally or on Sun Grid Engine.

Standard install: This will do a standard install of an egg with a fixed version.

    sudo python setup.py install

Developer install for user: This install will just create a link to
the source directory. On a typical system this will result in the egg
link being installed to ~/.local/lib/python2.7/site-packages/
    
    python setup.py develop --user

For a system wide developer install:
    
    sudo pip install -e .