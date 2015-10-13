import sys
import os
import getopt
import math
import tempfile
import stat
import shlex
import subprocess
from subprocess import Popen
from optparse import OptionParser
from pypipeline.util import get_new_file, sweep_mult, fancify_cmd, frange
from pypipeline.util import head_sentences
import platform
from glob import glob
from pypipeline.experiment_runner import ExpParamsRunner, get_subset
from pypipeline import experiment_runner
from pypipeline import pipeline
import re
import random
from pypipeline.pipeline import write_script, RootStage, Stage

class StagePath:
    '''Lazy constructor of relative paths for a stage'''
    
    def __init__(self, stage, rel_path):
        self.stage = stage
        self.rel_path = rel_path
    
    def __str__(self):
        if not hasattr(self.stage, 'exp_dir'):
            raise Exception("stage.exp_dir not initialized")
        return os.path.join(self.stage.exp_dir, self.rel_path)


class ScrapeExps(experiment_runner.PythonExpParams):
    
    def __init__(self, **keywords):
        experiment_runner.PythonExpParams.__init__(self,keywords)
        self.always_relaunch()

    def get_initial_keys(self):
        return []
    
    def get_instance(self):
        return ScrapeExps()
    
    def get_name(self):
        return "scrape_exps"
    
    def create_experiment_script(self, exp_dir):
        self.add_arg(os.path.dirname(exp_dir))
        script = ""
        cmd = "scrape_exps.py %s\n" % (self.get_args())
        script += fancify_cmd(cmd)
        return script
    
    
def get_oome_stages(stage, max_mem=100*1000, max_doubles=4):
    '''Get a new list of stages which are copies of the given stage, 
    except that they double the working memory up to either max_mem, 
    or the max number of doubles. 
    ''' 
    stages = [stage]
    mem = stage.get("work_mem_megs")
    for _ in range(max_doubles):
        mem *= 2
        if mem > max_mem:
            break
        doubled = stage.copy_with(work_mem_megs=mem)
        doubled.set("memory", str(mem)+"M", incl_arg=False)
        doubled.script_fns.append(prereqs_create_experiment_script)        
        doubled.add_prereq(stages[len(stages)-1])
        stages += [doubled]
    return stages
        
def prereqs_create_experiment_script(stage, exp_dir):
        script = "\n"
        for prereq in stage.prereqs:
            if not hasattr(prereq, "exp_dir"):
                # Skip the root stage 
                continue 
            script += "PREREQ_DIR=%s" % (prereq.exp_dir)
            script += '''
echo "Sleeping for 30 seconds"
sleep 30
if [[ -e $PREREQ_DIR/DONE ]] ; then
    echo "Previous stage ran successfully. Marking DONE and exiting."
    touch DONE
    exit 0
elif [[ `tail -n 1000 $PREREQ_DIR/stdout | grep "OutOfMemoryError"` ]] ; then
    echo "Previous stage failed on OutOfMemoryError. Running this stage."
else
    echo "Previous stage failed with a different error. Not marking DONE and exiting."
    exit 1
fi
            ''' 
        return script

        