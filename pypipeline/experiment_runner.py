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
import topological
from util import get_new_directory
from util import get_new_file
from collections import defaultdict
from pypipeline.pipeline import Stage, PipelineRunner, RootStage, NamedStage

def get_subset(expparams_list, **keywords):
    '''Gets the subset of ExpParams objects for which all the keywords specified
    are also parameters for that ExpParams.
    '''
    subset = []
    for expparams in expparams_list:
        # Check whether this stage matches all the key/value pairs specified.
        contains_all = True
        for k,v in keywords.items():
            if not expparams.get(k) == v:
                contains_all = False
                break
        if contains_all:
            subset.append(expparams)
    return subset

class ExpParams(Stage):
    
    def __init__(self, dictionary=None, **keywords):
        Stage.__init__(self)
        self.params = {}
        self.exclude_name_keys = set()
        self.exclude_arg_keys = set()
        if dictionary:
            self.params.update(dictionary)
        if keywords:
            self.params.update(keywords)
        self.kvsep = "\t"
        self.paramsep = "\n"
        self.none_string = ""
        self.key_order = None
        # The prefix for dummy keys
        self.dummy_key_prefix = "__arg__"
        # The separator for key/value parameters in the argument string
        self.args_kvsep = " "
        self.script_fns = []
       
    def __str__(self):
        return "ExpParams[params=%s exclude_name_keys=%s exclude_arg_keys=%s]" % \
            (str(self.params), str(self.exclude_name_keys), str(self.exclude_arg_keys))
    
    def create_stage_script(self, exp_dir):
        '''Creates and returns the experiment script and writes the expparams.txt file.
        Overriding method for Stage.
        '''
        script = ""
        for script_fns in self.script_fns:
            script += script_fns(self, exp_dir)
        # Creates and returns the experiment script string. 
        script += self.create_experiment_script(exp_dir)
        # Write out the experiment parameters to a file
        # Do this after create_experiment_script in case there are additions to the parameters
        # made by that call.
        self.write(os.path.join(exp_dir, "expparams.txt"))
        return script

    def __add__(self, other):
        ''' Overloading operator + '''
        return self.concat(other)
    
    def concat(self, other):
        '''Returns a copy of self plus all the parameters of other.
        Note that other's params override self.
        '''
        if isinstance(other, ExpParams):
            new_exp = other.get_instance()
        else:
            new_exp = self.get_instance()
        new_exp.params.update(self.params)
        new_exp.exclude_name_keys.update(self.exclude_name_keys)
        new_exp.exclude_arg_keys.update(self.exclude_arg_keys)
        if isinstance(other, ExpParams):
            new_exp.params.update(other.params)
            new_exp.exclude_name_keys.update(other.exclude_name_keys)
            new_exp.exclude_arg_keys.update(other.exclude_arg_keys)
        else:
            new_exp.params.update(other)
        return new_exp
    
    def copy_with(self, **keywords):
        return self.concat(keywords)
    
    def update(self, **keywords):
        ''' Adds the keywords as parameters. '''
        self.params.update(keywords)
            
    def set(self, key, value, incl_name=True, incl_arg=True):
        self.params[key] = value
        self.set_incl_name(key, incl_name)
        self.set_incl_arg(key, incl_arg)
    
    def set_incl_name(self, key, incl_name):
        if not incl_name:
            self.exclude_name_keys.add(key)
        elif key in self.exclude_name_keys:
            self.exclude_name_keys.remove(key)
        
    def set_incl_arg(self, key, incl_arg):
        if not incl_arg:
            self.exclude_arg_keys.add(key)
        elif key in self.exclude_arg_keys:
            self.exclude_arg_keys.remove(key)
    
    def remove(self, key):
        if key in self.params:
            del self.params[key]
    
    def get(self, key):
        ''' Returns the value with its true type '''
        return self.params.get(key,None)
    
    def keys(self):
        return self.params.keys()
    
    def getstr(self, key):
        ''' Returns a string version of the value '''
        return self._get_as_str(self.get(key))
    
    def add_arg(self, arg):
        ''' Adds an command line argument which will be printed without its key.'''
        dummy_key = self.dummy_key_prefix + str(len(self.params))        
        self.set(dummy_key, arg, True, True)
    
    def read(self, path):
        ''' Read parameter names and values from a file '''
        filestr = "".join(open(path, 'r').readlines())
        for param in filestr.split(self.paramsep):
            if param == '':
                continue
            key,value,exclude_name,exclude_arg = param.split(self.kvsep)
            self.params[key] = self._attempt_to_coerce(value)
            if exclude_name == "True":
                self.exclude_name_keys.add(key)
            if exclude_arg == "True":
                self.exclude_arg_keys.add(key)

    def write(self, path):
        ''' Write out parameter names and values to a file '''
        out = open(path, 'w')
        for key,value,exclude_name,exclude_arg in self._get_string_params():
            out.write(self.kvsep.join([key, value, exclude_name, exclude_arg]) + self.paramsep) 
        out.close()
        
    def get_name(self):
        ''' Returns the name of this experiment '''
        name = []
        for key in self.get_name_key_order():
            value = self.get(key)
            if key not in self.exclude_name_keys:
                name.append(self._get_as_str(value).replace(",","-"))
        return "_".join(name)
    
    def get_args(self):
        ''' Returns a string consisting of the arguments defined by the parameters of this experiment '''
        args = ""
        # Add the key/value arguments.
        for key,value in sorted(self.params.items()):
            if key not in self.exclude_arg_keys and not key.startswith(self.dummy_key_prefix):
                if value is None:
                    args += "--%s " % (self._get_as_str(key))
                else:
                    args += "--%s%s%s " % (self._get_as_str(key), self.args_kvsep, self._get_as_str(value))
        # Add the additional command line arguments.
        for key,value in self.params.items():
            if key not in self.exclude_arg_keys and key.startswith(self.dummy_key_prefix):
                args += "%s " % (self._get_as_str(value))
        return args
            
    def _get_as_str(self, value):
        ''' Converts the value to a string '''
        if value == None:
            return self.none_string
        if isinstance(value, int):
            return str(value)
        elif isinstance(value, float):
            return "%g" % (value)
        elif isinstance(value, str):
            return value
        else:
            return str(value)
            
    def _attempt_to_coerce(self, value):
        if value == self.none_string:
            return None
        # Note: we could try to first convert to an int,
        # and fall back to a float, but it's probably easier to
        # start with a float and stay there.
        try:
            value = float(value)
        except ValueError:
            pass
        return value
    
    def _get_string_params(self):
        sps = []
        for key in self.params:
            exclude_name = key in self.exclude_name_keys
            exclude_arg = key in self.exclude_arg_keys
            sps.append((key, self.params[key], exclude_name, exclude_arg))
        return map(lambda x:map(self._get_as_str, x), sps)
    
    def get_name_key_order(self):
        '''Gets the order anew or the cached name key order if present.'''
        if self.key_order:
            return self.key_order
        else:
            return self._get_name_key_order()
    
    def _get_name_key_order(self):
        '''Creates and returns the name key order.'''
        key_order = []
        initial_keys = self.get_initial_keys()
        all_keys = sorted(self.params.keys())
        for key in initial_keys:
            if key in all_keys:
                key_order.append(key)
        for key in all_keys:
            if key not in initial_keys:
                key_order.append(key)
        return key_order
    
    def get_initial_keys(self):
        ''' OVERRIDE THIS METHOD '''
        return []
    
    def get_instance(self):
        ''' OVERRIDE THIS METHOD '''
        return ExpParams()
    
    def create_experiment_script(self, exp_dir):
        ''' 
        OVERRIDE THIS METHOD. 
        Returns a str to be written out as the experiment script 
        '''
        pass

class JavaExpParams(ExpParams):
    
    def __init__(self, dictionary=None, **keywords):
        dictionary.update(keywords)
        ExpParams.__init__(self,dictionary)
        self.hprof = None
        self.set("java_args", "", incl_arg=False, incl_name=False)
            
    def get_java_args(self):
        return self._get_java_args(self.work_mem_megs)
    
    def _get_java_args(self, total_work_mem_megs):
        '''Returns reasonable JVM args based on the total megabytes available'''
        work_mem_megs = total_work_mem_megs
        if work_mem_megs >= 512+128+256:
            # Subtract off some overhead for the JVM
            work_mem_megs -= 512
            # Subtract off some overhead for the PermSize
            max_perm_size = 128
            work_mem_megs -= max_perm_size
            assert work_mem_megs >= 256, "work_mem_megs=%f" % (work_mem_megs)
        else:
            work_mem_megs -= 32
            max_perm_size = 32
            
        java_args = " -server -ea -Dfile.encoding=UTF8 "
        java_args += " -Xms%dm -Xmx%dm -Xss4m" % (work_mem_megs, work_mem_megs)
        java_args += " -XX:MaxPermSize=%dm " % (max_perm_size)
        
        # Read more on garbage collection parameters here:
        #     http://www.oracle.com/technetwork/java/javase/gc-tuning-6-140523.html#cms
        threads = self.get("threads")
        if threads <= 1:
            # Added to ensure parallel garbage collection is NOT running.
            java_args += " -XX:-UseParallelGC -XX:-UseParNewGC -XX:+UseSerialGC"
        else:
            # Alt1: java_args += " -XX:ParallelGCThreads=%d -XX:+UseParallelGC -XX:+UseParallelOldGC" % (threads)
            # Alt2: java_args += " -XX:ConcGCThreads=%d -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode" % (threads)
            #
            # Alt1 is best if throughput is the most important and pauses of up to 1 second
            # are acceptable. This is almost always true of experiments. 
            # Alt2 may cause issues on a grid as it could use too much parallelism, since the garbage collection
            # runs concurrently with the other application threads.     
            java_args += " -XX:ParallelGCThreads=%d -XX:+UseParallelGC -XX:+UseParallelOldGC" % (threads)
        #java_args += " -verbose:gc"
        
        if self.hprof == "cpu":
            self.update(java_args = self.get("java_args") + " -agentlib:hprof=cpu=samples,depth=7,interval=10 ")
        elif self.hprof == "heap":
            self.update(java_args = self.get("java_args") + " -agentlib:hprof=heap=sites,depth=7 ")
        elif self.hprof is not None:
            raise Exception("Unknown argument for hprof: " + self.hprof)
                
        extra_java_args = self.get("java_args")
        if extra_java_args is not None:
            java_args += " " + extra_java_args
        
        return java_args
    
    def get_instance(self):
        ''' OVERRIDE THIS METHOD '''
        return JavaExpParams()
    

class PythonExpParams(ExpParams):
    
    def __init__(self, dictionary=None, **keywords):
        dictionary.update(keywords)
        ExpParams.__init__(self,dictionary)
        self.args_kvsep = "="
    
    def get_instance(self):
        ''' OVERRIDE THIS METHOD '''
        return PythonExpParams()

def get_all_keys(expparams):
    '''Gets the set of all keys for these expparams.'''
    all_keys = set()
    for expparam in expparams:
        for key,_ in expparam.params.items():
            all_keys.add(key)
    return all_keys    

def get_nonunique_keys(expparams):
    '''Gets the set of nonunique keys for these expparams.'''
    key2vals = defaultdict(set)
    for expparam in expparams:
        for key,value in expparam.params.items():
            key2vals[key].add(value)
    
    nonunique_keys = set()
    for key in key2vals:
        if len(key2vals[key]) > 1:
            nonunique_keys.add(key)
    return nonunique_keys
    
def get_kept_keys(expparams):
    '''Gets the union of the nonunique keys and the initial keys specified by the ExpParams.'''
    nonunique_keys = get_nonunique_keys(expparams)
    
    kept_keys = set()
    kept_keys.update(nonunique_keys)
    for expparam in expparams:
        kept_keys.update(set(expparam.get_initial_keys()))

    return kept_keys

def get_exclude_name_keys(expparams):
    '''Gets all the keys which are excluded from the name of some ExpParam.'''
    excluded = set()
    for expparam in expparams:
        excluded = excluded.union(expparam.exclude_name_keys)
    return excluded
    
def shorten_names(expparams):
    '''Shortens the names of a set of expparams.'''
    kept_keys = get_kept_keys(expparams)
    for expparam in expparams:
        expparam.key_order = filter(lambda x: x in kept_keys, expparam._get_name_key_order())

class ExpParamsRunner(PipelineRunner):
    
    def __init__(self,name, queue, print_to_console=False, dry_run=False):
        PipelineRunner.__init__(self, name, queue, print_to_console, dry_run)

    def run_experiments(self, exp_stages):
        root_stage = RootStage()
        root_stage.add_dependents(exp_stages)
        self.run_pipeline(root_stage)
        
    def run_pipeline(self, root_stage):
        self.shorten_names_epstages(root_stage)
        PipelineRunner.run_pipeline(self, root_stage)
        
    def shorten_names_epstages(self, root_stage):
        # TODO: note that this is only a class-method only
        #   because get_stages_as_list is a class-method.
        expparams = []
        for stage in self.get_stages_as_list(root_stage):
            if isinstance(stage, ExpParams):
                expparams.append(stage)
        shorten_names(expparams)
        
class ExperimentStage(NamedStage):
    '''Deprecated.'''
        
    def __init__(self, name, experiment, exp_runner):
        NamedStage.__init__(self, name)
        self.exp_runner = exp_runner
        self.experiment = experiment

    def create_stage_script(self, exp_dir):
        return self.exp_runner.create_experiment_script(self.get_name(), self.experiment, exp_dir)

    def __str__(self):
        return self.get_name()

class ExperimentRunner(PipelineRunner):
    '''Deprecated.'''
    
    def __init__(self,name="experiments",serial=False):
        if serial == True:
            queue = None
        else:
            queue = "dummy.q" 
        PipelineRunner.__init__(self, name, queue)
        self.qsub_args = None

    def run_experiments(self, experiments):
        root_stage = RootStage()
        for name,experiment in experiments.items():
            exp_stage = ExperimentStage(name, experiment, self)
            exp_stage.add_prereq(root_stage)
        self.run_pipeline(root_stage)
        
    def get_stages_as_list(self, root_stage):
        '''This method is overriden to give the provided order for experiments'''
        return self.bfs_stages(root_stage)
        
    def create_experiment_script(self, name, experiment, exp_dir):
        ''' Override this method '''
        return None

if __name__ == '__main__':
    print "This script is not to be run directly"
