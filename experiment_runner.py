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
from pipeline import Stage, PipelineRunner, RootStage
from collections import defaultdict
        
class ExpParams:
    
    def __init__(self, dictionary=None, **keywords):
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
        
    def __add__(self, other):
        ''' Overloading operator + '''
        return self.concat(other)
    
    def concat(self, other):
        '''
        Returns a copy of self plus all the parameters of other.
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
        if not incl_name:
            self.exclude_name_keys.add(key)
        if not incl_arg:
            self.exclude_arg_keys.add(key)
    
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
            out.write("".join([key, self.kvsep, value, self.kvsep, exclude_name, self.kvsep, exclude_arg, self.paramsep])) 
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
        for key,value in self.params.items():
            if key not in self.exclude_arg_keys:
                args += "--%s %s " % (self._get_as_str(key), self._get_as_str(value))
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
        if self.key_order:
            return self.key_order
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
    
    def create_experiment_script(self, exp_dir, eprunner):
        ''' 
        OVERRIDE THIS METHOD. 
        Returns a str to be written out as the experiment script 
        '''
        pass

class JavaExpParams(ExpParams):
    
    def __init__(self, dictionary=None, **keywords):
        dictionary.update(keywords)
        ExpParams.__init__(self,dictionary)
            
    def get_java_args(self, eprunner):
        return self._get_java_args(eprunner.work_mem_megs)
    
    def _get_java_args(self, total_work_mem_megs):
        '''Returns reasonable JVM args based on the total megabytes available'''
        work_mem_megs = total_work_mem_megs
        # Subtract off some overhead for the JVM
        work_mem_megs -= 512
        # Subtract off some overhead for the PermSize
        max_perm_size = 128
        work_mem_megs -= max_perm_size
        assert(work_mem_megs >= 256)
        
        java_args = " -server -ea -Dfile.encoding=UTF8 "
        java_args += " -Xms%dm -Xmx%dm " % (work_mem_megs, work_mem_megs)
        java_args += " -XX:MaxPermSize=%dm " % (max_perm_size)
        
        return java_args
    
class ExpParamsStage(Stage):
    
    def __init__(self, expparams, eprunner):
        Stage.__init__(self, expparams.get_name())
        self.expparams = expparams
        self.eprunner = eprunner

    def create_stage_script(self, exp_dir):
        # Create and return the experiment script string
        script = self.expparams.create_experiment_script(exp_dir, self.eprunner)
        # Write out the experiment parameters to a file
        # Do this after create_experiment_script in case there are additions to the parameters
        # made by that call.
        self.expparams.write(os.path.join(exp_dir, "expparams.txt"))
        return script

    def __str__(self):
        return self.name

def shorten_names(expparams):
    ''' Shortens the names of a set of expparams '''
    key2vals = defaultdict(set)
    for expparam in expparams:
        for key,value in expparam.params.items():
            key2vals[key].add(value)
    
    nonunique_keys = set()
    for key in key2vals:
        if len(key2vals[key]) > 1:
            nonunique_keys.add(key)
    
    kept_keys = set()
    kept_keys.update(nonunique_keys)
    for expparam in expparams:
        kept_keys.update(set(expparam.get_initial_keys()))

    for expparam in expparams:
        expparam.key_order = filter(lambda x: x in kept_keys, expparam.get_name_key_order())

class ExpParamsRunner(PipelineRunner):
    
    def __init__(self,name, queue):
        PipelineRunner.__init__(self, name, queue)

    def run_experiments(self, experiments):
        root_stage = RootStage()
        prereqs = self.get_prereqs()
        if prereqs and (not prereqs == []):
            # We call _get_exp_stages for prereqs first so that the dfs in our
            # overrided get_stages_as_list still works correctly
            prereqs = self._get_exp_stages(prereqs, root_stage)
        exp_stages = self._get_exp_stages(experiments, root_stage)
        if prereqs:
            for exp_stage in exp_stages:
                for prereq in prereqs:
                    exp_stage.add_prereq(prereq)
        self.run_pipeline(root_stage)
        
    def run_pipeline(self, root_stage):
        self.shorten_names_epstages(root_stage)
        PipelineRunner.run_pipeline(self, root_stage)
        
    def shorten_names_epstages(self, root_stage):
        # TODO: note that this is awkwardly a class-method only
        #   because get_stages_as_list is a class-method
        # Shorten the names
        experiments = [stage.expparams for stage in self.get_stages_as_list(root_stage) if not isinstance(stage, RootStage)]
        shorten_names(experiments)
        # Update the names on the stages
        for stage in self.get_stages_as_list(root_stage):
            if not isinstance(stage, RootStage):
                stage.set_name(stage.expparams.get_name())
    
    def _get_exp_stages(self, expparams, root_stage):
        exp_stages = []
        for expparam in expparams:
            exp_stage = ExpParamsStage(expparam,self)
            exp_stage.add_prereq(root_stage)
            exp_stages.append(exp_stage)
        return exp_stages
    
    def create_post_processing_stage_script(self, top_dir, all_stages):
        all_stages = all_stages[1:]
        exp_tuples = [(stage.name, stage.expparams) for stage in all_stages]
        return self.create_post_processing_script(top_dir, exp_tuples)
    
    def get_stages_as_list(self, root_stage):
        '''This method is overriden to give the provided order for experiments'''
        return self.bfs_stages(root_stage)
    
    def create_post_processing_script(self, top_dir, exp_tuples):
        ''' Override this method '''
        return None

    def get_prereqs(self):
        ''' Override this method '''
        return None

class ExperimentStage(Stage):
    
    def __init__(self, name, experiment, exp_runner):
        Stage.__init__(self, name)
        self.exp_runner = exp_runner
        self.experiment = experiment

    def create_stage_script(self, exp_dir):
        return self.exp_runner.create_experiment_script(self.name, self.experiment, exp_dir)

    def __str__(self):
        return self.name

class ExperimentRunner(PipelineRunner):
    
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

    def create_post_processing_stage_script(self, top_dir, all_stages):
        all_stages = all_stages[1:]
        exp_tuples = [(stage.name, stage.experiment) for stage in all_stages]
        return self.create_post_processing_script(top_dir, exp_tuples)
    
    def get_stages_as_list(self, root_stage):
        '''This method is overriden to give the provided order for experiments'''
        return self.bfs_stages(root_stage)
        
    def create_experiment_script(self, name, experiment, exp_dir):
        ''' Override this method '''
        return None
    
    def create_post_processing_script(self, top_dir, exp_tuples):
        ''' Override this method '''
        return None

if __name__ == '__main__':
    print "This script is not to be run directly"
