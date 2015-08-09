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

def run_and_get_output(command):
    p = Popen(args=shlex.split(command), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    (stdoutdata, stderrdata) = p.communicate()
    return "\n".join([stdoutdata, stderrdata])

def get_job_name(qsub_file):
    qsub_lines = open(qsub_file, 'r').readlines()
    qsub_string = "".join(qsub_lines)
    match = re.search(" -N (\S+) ", qsub_string)
    if match:
        return match.group(1)
    else:
        return None

def is_running(job_name):
    command = "qstat -j '%s'" % job_name
    p = Popen(args=shlex.split(command), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    retcode = p.wait()
    if (retcode == 0):
        return True
    else:
        return False

class Relauncher:

    def __init__(self, test, tries):
        self.relaunch_count = 0
        self.running_count = 0
        self.done_count = 0
        self.test = test
        self.tries = tries

    def relaunch(self, top_dir):
        top_dir = os.path.abspath(top_dir)
        for exp_dir in sorted(glob(os.path.join(top_dir, "*"))):
            qsub_file = os.path.join(exp_dir, "qsub-script_000.sh")
            
            # Check that the DONE file is not present
            if os.path.exists(os.path.join(exp_dir, "DONE")):
                self.done_count += 1
                continue
            # Skip other cases that don't look like experiment directories.
            if os.path.isfile(exp_dir):
                continue
            if not os.path.exists(qsub_file):
                print "WARN: experiment directory missing qsub script:", exp_dir
                continue
            # Check that the job is not already running
            job_name = get_job_name(qsub_file)
            if is_running(job_name):
                print "Running: ", job_name
                self.running_count += 1
                continue

            self.relaunch_count += 1
            if not self.test:
                print "Removing stdout: "
                # Remove stdout
                os.system("rm -r %s" % (os.path.join(exp_dir, "stdout")))
                # TODO: Remove state files
                #os.system("rm -r %s" % (os.path.join(exp_dir, "state.binary.*")))
                
                # Relaunch
                os.chdir(exp_dir)
                print "Relaunching directory: ",exp_dir
                cmd = "bash %s" % (qsub_file)
                print cmd
                for i in range(self.tries):
                    if subprocess.call(shlex.split(cmd)) == 0:
                        break
                    elif i < self.tries-1:
                        print "Failed to relaunch, trying again..."
                    else:
                        raise Exception("Command returned non-zero exit code: " + cmd)
            else:
                print "Relaunching directory: ",exp_dir

        print "Total: ",self.relaunch_count+self.done_count+self.running_count
        print "Total done: ",self.done_count
        print "Total already running: ",self.running_count
        print "Total relaunched: ",self.relaunch_count

    def cleanup_dir(self, exp_dir):
        ''' OVERRIDE THIS IN SUBCLASS'''
        pass

if __name__ == "__main__":
    pass
