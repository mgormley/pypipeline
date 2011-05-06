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
from experiments.core.pipeline import Stage, PipelineRunner, RootStage
from my_collections.odict import OrderedDict


class ExpParams:
    
    def __init__(self, dictionary=None, **keywords):
        self.params = {} #TODO: OrderedDict()
        self.short_values = {}
        if dictionary:
            self.params.update(dictionary)
        if keywords:
            self.params.update(keywords)
        self.kvsep = "\t"
        self.paramsep = "\n"
        
    def __add__(self, other):
        ''' Overloading operator + '''
        return self.concat(other)
    
    def concat(self, other):
        '''
        Returns a copy of self plus all the parameters of other.
        Note that other's params override self.
        '''
        new_exp = self.get_instance()
        new_exp.params.update(self.params)
        if isinstance(other, ExpParams):
            new_exp.params.update(other.params)
        else:
            new_exp.params.update(other)
        return new_exp
    
    def copy_with(self, **keywords):
        return self.concat(keywords)
    
    def update(self, **keywords):
        ''' Adds the keywords as parameters. '''
        self.params.update(keywords)
            
    def set(self, key, value, short_value=None):
        self.params[key] = value
        if short_value:
            self.short_values[key] = short_value

    def write(self, path):
        ''' Write out parameter names and values to a file '''
        out = open(path, 'w')
        for key,value in self.params.items():
            out.write("%s%s%s%s" % (key, self.kvsep, self._get_as_str(value), self.paramsep)) 
        out.close()
        
    def _get_as_str(self, value):
        ''' Converts the value to a string '''
        if isinstance(value, int):
            return str(value)
        elif isinstance(value, float):
            return "%g" % (value)
        elif isinstance(value, str):
            return value
        else:
            return str(value)
        
    def read(self, path):
        ''' Read parameter names and values from a file '''
        filestr = "".join(open(path, 'r').readlines())
        for param in filestr.split(self.paramsep):
            for key,value in param.split(self.kvsep):
                self.params[key] = self.attempt_to_coerce(value)
    
    def _attempt_to_coerce(self, value):
        # Note: we could try to first convert to an int,
        # and fall back to a float, but it's probably easier to
        # start with a float and stay there.
        try:
            value = float(value)
        except ValueError:
            pass
        return value
    
    def get_name(self):
        ''' Returns the name of this experiment '''
        name = map(self._get_as_str, self.params.values())
        return "_".join(name)
    
    def get_args(self):
        ''' Returns a string consisting of the arguments defined by the parameters of this experiment '''
        args = ""
        for key,value in self.params.items():
            args += "--%s=%s " % (key, self._get_as_str(value))
        return args
        
    def get_instance(self):
        ''' OVERRIDE THIS METHOD '''
        return ExpParams()
    
    def create_experiment_script(self, exp_dir):
        ''' 
        OVERRIDE THIS METHOD. 
        Returns a str to be written out as the experiment script 
        '''
        pass

class ExpParamsStage(Stage):
    
    def __init__(self, experiment):
        Stage.__init__(self, experiment.get_name())
        self.experiment = experiment

    def create_stage_script(self, exp_dir):
        # Write out the experiment parameters to a file
        self.experiment.write(os.path.join(exp_dir, "expparams.txt"))
        # Create and return the experiment script string
        return self.experiment.create_experiment_script(exp_dir)

    def __str__(self):
        return self.name

class ExpParamsRunner(PipelineRunner):
    
    def __init__(self,name="experiments",serial=False):
        PipelineRunner.__init__(self, name, serial)
        self.qsub_args = None

    def run_experiments(self, experiments):
        root_stage = RootStage()
        for experiment in experiments:
            exp_stage = ExpParamsStage(experiment)
            # Give each experiment stage the global qsub_args 
            exp_stage.qsub_args = self.qsub_args
            exp_stage.add_prereq(root_stage)
        self.run_pipeline(root_stage)

    def create_post_processing_stage_script(self, top_dir, all_stages):
        all_stages = all_stages[1:]
        exp_tuples = [(stage.name, stage.experiment) for stage in all_stages]
        return self.create_post_processing_script(top_dir, exp_tuples)
    
    def get_stages_as_list(self, root_stage):
        '''This method is overriden to give the provided order for experiments'''
        return self.dfs_stages(root_stage)
    
    def create_post_processing_script(self, top_dir, exp_tuples):
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
        PipelineRunner.__init__(self, name, serial)
        self.qsub_args = None

    def run_experiments(self, experiments):
        root_stage = RootStage()
        for name,experiment in experiments.items():
            exp_stage = ExperimentStage(name, experiment, self)
            # Give each experiment stage the global qsub_args 
            exp_stage.qsub_args = self.qsub_args
            exp_stage.add_prereq(root_stage)
        self.run_pipeline(root_stage)

    def create_post_processing_stage_script(self, top_dir, all_stages):
        all_stages = all_stages[1:]
        exp_tuples = [(stage.name, stage.experiment) for stage in all_stages]
        return self.create_post_processing_script(top_dir, exp_tuples)
    
    def get_stages_as_list(self, root_stage):
        '''This method is overriden to give the provided order for experiments'''
        return self.dfs_stages(root_stage)
        
    def create_experiment_script(self, name, experiment, exp_dir):
        ''' Override this method '''
        return None
    
    def create_post_processing_script(self, top_dir, exp_tuples):
        ''' Override this method '''
        return None

if __name__ == '__main__':
    print "This script is not to be run directly"
