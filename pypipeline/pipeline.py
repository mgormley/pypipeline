#!/usr/bin/python
#

import os
import sys
import getopt
import math
import tempfile
import stat
import re
import shlex
import time
import subprocess
from subprocess import Popen
import glob
#import topsort
import topological
from util import get_new_directory
from util import get_new_file
import random
from pypipeline.qsub import get_default_qsub_params, get_qsub_args,\
    get_default_qsub_params

def write_script(prefix, script, dir):
    out, script_file = get_new_file(prefix=prefix,suffix=".sh",dir=dir)
    out.write(script)
    out.write("\n")
    out.close()
    os.system("chmod u+x '%s'" % (script_file))
    return script_file

def get_files_in_dir(dirname):
    return [f for f in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, f))]

def create_queue_command(script_file, cwd, name="test", prereqs=[], stdout="stdout", qsub_args=None):
    # Make the stdout file and script_file relative paths to cwd if possible.
    stdout = os.path.relpath(stdout, cwd)
    script_file = os.path.relpath(script_file, cwd)
    # Create the qsub command.
    queue_command = "qsub "
    if qsub_args:
        queue_command += " " + qsub_args + " "
    else:
        #queue_command += " -q cpu.q "
        queue_command += " -q mem.q -q himem.q -l vf=15.5G "
    queue_command += " -cwd -j y -b y -V -N %s -e stderr -o %s " % (name, stdout)        
    if len(prereqs) > 0:
        queue_command += "-hold_jid %s " % (",".join(prereqs))
    queue_command += "\"bash '%s'\"" % (script_file)
    return queue_command

unique_num = 0
def get_unique_name(name):
    global unique_num
    unique_num += 1
    return name + str(unique_num)

def get_cd_to_bash_script_parent():
    return """
# Change directory to the parent directory of the calling bash script.
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo "Changing directory to $DIR"
cd $DIR
"""
    
def dfs_stages(stage):
    stages = set()
    stages.add(stage)
    #print "stage.name",stage.name
    #print "stage.dependents",[x.name for x in stage.dependents]
    for dependent in stage.dependents:
        for s in dfs_stages(dependent):
            if not s in stages:
                stages.add(s)
    return stages

def bfs_stages(stage):
    stages = set()
    queue = [stage]
    while queue != []:
        stage = queue.pop(0)
        if not stage in stages:
            stages.add(stage)
        queue.extend(stage.dependents)
    return stages

class Stage:
    '''A stage in a pipeline to be run after the stages in prereqs and before the 
    stages in dependents.
    
    Attributes:
        cwd: Current working directory for this stage (Set by PipelineRunner).
        serial: True iff this stage will be run in a pipeline using bash, one stage at a time (Set by PipelineRunner).
        dry_run: Whether to just do a dry run which will skip the actual running of script in _run_script() (Set by PipelineRunner).
        root_dir: Path to root directory for this project (Set by PipelineRunner).
        setupenv: Path to setupenv.sh script for setting environment (Set by PipelineRunner).
        work_mem_megs: Megabytes required by this stage (Default provided by PipelineRunner). 
        threads: Number of threads used by this stage (Default provided by PipelineRunner).
        minutes: Number of minutes used by this stage (Default provided by PipelineRunner).
        qsub_args: The SGE qsub arguments for running the job (Set by PipelineRunner).
        qdel_script_file: Path to qdel script for this stage (Set by _run_stage when self.serial is True).
        print_to_console: Whether to print stdout/stderr to the console (Set by PipelineRunner).
        
    Private attributes:
        prereqs: List of stages that should run before this stage.
        dependents: List of stages that should run after this stage.
        completion_indicator: Filename to be created upon successful completion.
    '''
    
    def __init__(self, completion_indicator="DONE"):
        ''' If the default completion_indicator is used, it will be created in the cwd for this stage '''
        self.completion_indicator = completion_indicator
        self.prereqs = []
        self.dependents = []
        self.cwd = None
        self.serial = False
        self.root_dir = None
        self.setupenv = None
        self.work_mem_megs = None
        self.threads = None
        self.minutes = None
        self.qsub_args = None
        self.qdel_script_file = None
        self.print_to_console = None
        # A fixed random number to distinguish this task from
        # other runs of this same task within qsub.
        self.qsub_rand = random.randint(0, sys.maxint)

    def always_relaunch(self):
        # We add a non-canonical completion indicator in order to ensure
        # that this job will always relaunch.
        self.completion_indicator = "DONE_BUT_RELAUNCH"
        
    def add_dependent(self, stage):
        stage.prereqs.append(self)
        self.dependents.append(stage)
    
    def add_dependents(self, stages):
        for stage in stages:
            self.add_dependent(stage)
            
    def add_prereq(self, stage):
        self.prereqs.append(stage)
        stage.dependents.append(self)

    def add_prereqs(self, stages):
        for stage in stages:
            self.add_prereq(stage)
        
    def get_qsub_name(self):
        '''Gets the SGE job name.'''
        # Create a more unique name for qsub so that multiple the kill script only kills its own job
        qsub_name = "%s_%x" % (self.get_name(), self.qsub_rand)
        # If qsub name does not begin with a letter, add an "a"
        matcher = re.compile('^[a-z,A-Z]').search(qsub_name)
        if not matcher:
            qsub_name = 'a'+qsub_name
        return qsub_name
            
    def run_stage(self, exp_dir):
        self.exp_dir = exp_dir
        os.chdir(exp_dir)
        if self._is_already_completed():
            print "Skipping completed stage: name=" + self.get_name() + " completion_indicator=" + self.completion_indicator
            return
        self._run_stage(exp_dir)
        
    def _run_stage(self, exp_dir):
        ''' Overidden by GridShardRunnerStage '''        
        # TODO: This should create another script that calls the experiment
        # script, not modify it.
        #  
        # ulimit doesn't work on Mac OS X or the COE (wisp). So we don't use it anymore.
        # script += "ulimit -v %d\n" % (1024 * self.work_mem_megs)
        # script += "\n"
        
        script = ""
        # Always change directory to the current location of this experiment script.    
        script += get_cd_to_bash_script_parent()
        # Source the setupenv.sh script.
        script += "source %s\n\n" % (self.setupenv)
        # Add the execution.
        script += self.create_stage_script(exp_dir)
        # Touch a file to indicate successful completion.
        script += "\ntouch '%s'\n" % (self.completion_indicator)        
        script_file = write_script("experiment-script", script, exp_dir)
        self._run_script(script_file, exp_dir)

    def __str__(self):
        return self.get_name()

    def _is_already_completed(self):
        if not os.path.exists(self.completion_indicator):
            return False
        for prereq in self.prereqs:
            if not prereq._is_already_completed():
                return False
        return True

    def _run_script(self, script_file, cwd, stdout_filename="stdout"):
        stdout_path = os.path.join(cwd, stdout_filename)
        os.chdir(cwd)
        assert(os.path.exists(script_file))
        if self.serial:
            command = "bash %s" % (script_file)
            print self.get_name(),":",command
            if self.dry_run: return
            stdout = open(stdout_path, 'w')
            if not self.print_to_console:
                # Print stdout only to a file.
                p = Popen(args=shlex.split(command), cwd=cwd, stderr=subprocess.STDOUT, stdout=stdout)
                retcode = p.wait()
            else:
                # Print out stdout to the console and to the file.
                p = Popen(args=shlex.split(command), cwd=cwd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
                for line in iter(p.stdout.readline, ''):
                    try:
                        stdout.write(line)
                        print line,
                    except:
                        pass
                retcode = p.wait()
            stdout.close()
            if (retcode != 0):
                if not self.print_to_console:
                    # Print out the last few lines of the failed stage's stdout file.
                    os.system("tail -n 15 %s" % (stdout_path))
                raise subprocess.CalledProcessError(retcode, command)
            #Old way: subprocess.check_call(shlex.split(command))
        else:
            prereq_names = [prereq.get_qsub_name() for prereq in self.prereqs]
            qsub_script = ""
            qsub_script += get_cd_to_bash_script_parent() + "\n"
            qsub_cmd = create_queue_command(script_file, cwd, self.get_qsub_name(), prereq_names, stdout_path, self.qsub_args)
            qsub_script += qsub_cmd
            qsub_script_file = write_script("qsub-script", qsub_script, cwd)
            print qsub_cmd
            if self.dry_run: return
            subprocess.check_call(shlex.split("bash %s" % (qsub_script_file)))
            qdel_script = "qdel %s" % (self.get_qsub_name())
            self.qdel_script_file = write_script("qdel-script", qdel_script, cwd)
            
    def create_stage_script(self, exp_dir):
        ''' Override this method '''
        return None

    def get_name(self):
        '''Override this method.
        
        Gets display name of this stage and stage directory name.
        '''
        return None

class NamedStage(Stage):
    
    def __init__(self, name, completion_indicator="DONE"):
        Stage.__init__(self, completion_indicator)
        self._name = str(name) #TODO: is this the best way to handle name's type?

    def get_name(self):
        return self._name

class ScriptStringStage(NamedStage):
    
    def __init__(self, name, script, completion_indicator="DONE"):
        NamedStage.__init__(self, name, completion_indicator)
        self.script = script

    def create_stage_script(self, exp_dir):
        return self.script

class RootStage(NamedStage):
    
    def __init__(self):
        NamedStage.__init__(self, "root_stage")
        
    def run_stage(self, exp_dir):
        # Intentionally a no-op
        pass

class PipelineRunner:
    
    def __init__(self,name="experiments", queue=None, print_to_console=False, dry_run=False, rolling=False):
        self.name = name
        self.serial = (queue == None)
        self.root_dir = os.path.abspath(".")
        self.setupenv = os.path.abspath("./setupenv.sh")
        if not os.path.exists(self.setupenv):
            print "ERROR: File not found:", self.setupenv
            print "ERROR: The file setupenv.sh must be located in the current working directory"
            sys.exit(1)
        self.print_to_console = print_to_console
        self.rolling = rolling
        self.dry_run = dry_run
        
        # Setup arguments for qsub
        self.queue = queue
        (threads, work_mem_megs, minutes) = get_default_qsub_params(queue)
        self.threads = threads
        self.work_mem_megs = work_mem_megs
        self.minutes = minutes
        
    def run_pipeline(self, root_stage):
        self._check_stages(root_stage)
        top_dir = os.path.join(self.root_dir, "exp")
        if self.rolling:
            exp_dir = os.path.join(top_dir, self.name)
            os.system("rm -r %s" % (exp_dir))
            # TODO: add support for rolling directories
        else:
            exp_dir = get_new_directory(prefix=self.name, dir=top_dir)
        os.chdir(exp_dir)
        for stage in self.get_stages_as_list(root_stage):
            if isinstance(stage, RootStage):
                continue
            cwd = os.path.join(exp_dir, str(stage.get_name()))
            os.mkdir(cwd)
            self._update_stage(stage, cwd)
            stage.run_stage(cwd)
        if not self.serial:
            # Create a global qdel script
            global_qdel = ""
            for stage in self.get_stages_as_list(root_stage):
                if isinstance(stage, RootStage):
                    continue
                global_qdel += "bash %s\n" % (stage.qdel_script_file)
            write_script("global-qdel-script", global_qdel, exp_dir)
    
    def _update_stage(self, stage, cwd):
        '''Set some additional parameters on the stage.'''
        stage.cwd = cwd
        stage.serial = self.serial
        stage.dry_run = self.dry_run
        stage.root_dir = self.root_dir
        stage.setupenv = self.setupenv
        # Use defaults for threads, work_mem_megs, and minutes if they are not
        # set on the stage.
        if stage.threads is None:
            stage.threads = self.threads
        if stage.work_mem_megs is None:
            stage.work_mem_megs = self.work_mem_megs
        if stage.minutes is None:
            stage.minutes = self.minutes
        if stage.print_to_console is None:
            stage.print_to_console = self.print_to_console
        # Get the stage's qsub args.
        stage.qsub_args = get_qsub_args(self.queue, stage.threads, stage.work_mem_megs, stage.minutes)
        
    def _check_stages(self, root_stage):
        all_stages = self.get_stages_as_list(root_stage)
        names = set()
        for stage in all_stages:
            if stage.get_name() in names:
                print "ERROR: All stage names:\n" + "\n".join([s.get_name() for s in all_stages])
                print "ERROR: Multiple stages have the same name: " + stage.get_name()
                print "ERROR: Num copies:", len([x for x in all_stages if x.get_name() == stage.get_name()]) 
                assert stage.get_name() not in names, "ERROR: Multiple stages have the same name: " + stage.get_name()
            names.add(stage.get_name())
        print "All stages:"
        for stage in all_stages: 
            print "\t",stage.get_name()
        print "Number of stages:", len(all_stages)
            
    def get_stages_as_list(self, root_stage):
        return topological.bfs_topo_sort(root_stage)
        
if __name__ == '__main__':
    print "This script is not to be run directly"
