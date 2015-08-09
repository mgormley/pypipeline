#!/usr/bin/python

import sys
import os
import re
import getopt
import math
import tempfile
import stat
import shlex
import subprocess
from subprocess import Popen
from optparse import OptionParser
import platform
from glob import glob
import shutil
from pypipeline.util import get_new_file, sweep_mult, fancify_cmd,\
    sweep_mult_low
from pypipeline.pipeline import write_script, RootStage, Stage
from pypipeline.relauncher import Relauncher

if __name__ == "__main__":
    usage = "%prog "

    parser = OptionParser(usage=usage)
    parser.add_option(    '--test', action="store_true", help="Run without actually launching anything")
    parser.add_option(    '--tries', type="int", default=1, help="Number of times to attempt launching (for use with unstable SGE)")
    (options, args) = parser.parse_args(sys.argv)

    if len(args) <= 1:
        parser.print_help()
        sys.exit(1)

    relauncher = Relauncher(options.test, options.tries)
    for arg in args[1:]:
        relauncher.relaunch(arg)
