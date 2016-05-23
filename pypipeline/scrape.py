#!/usr/local/bin/python

import re
import sys
import os
import getopt
import math
import tempfile
import stat
import subprocess
from optparse import OptionParser
from glob import glob
import getpass
from pypipeline.util import get_all_following, get_following, get_time,\
    to_str, get_following_literal, tail, get_group1
from pypipeline.experiment_runner import get_nonunique_keys,\
    get_exclude_name_keys, get_all_keys, ExpParams

class ResultsWriter:
    
    def __init__(self):
        pass
    
    def write_top(self, top_dir):
        pass
    
    def write_readme(self, lines):
        pass
    
    def write_error(self, exp_dir):
        pass
            
    def write_results(self, key_order, values_list, get_as_str):
        pass
    
    def get_all_as_strs(self, key_order, values_list, get_as_str):
        key_order = map(get_as_str, key_order)
        values_list = [map(get_as_str, values) for values in values_list]
        return key_order, values_list

class CsvResultsWriter(ResultsWriter):
    
    def __init__(self, out):
        self.sep = ","
        self.quote = '"'
        self.out = out
        
    def csv_to_str(self, x):
        '''Convert to a quoted string for this writer.'''
        if x.find(self.sep) != -1:
            x = '%s%s%s' % (self.quote, x, self.quote)
        return x
    
    def write_top(self, top_dir):
        self.writeln(top_dir)
    
    def write_readme(self, lines):
        lines = " ".join(lines)
        self.writeln('"' + lines.replace("\n"," ") + '"')
        
    def write_error(self, exp_dir):
        self.writeln(self.sep.join(map(to_str,[exp_dir,"ERROR"])))
       
    def write_results(self, key_order, values_list, get_as_str):
        key_order, values_list = self.get_all_as_strs(key_order, values_list, get_as_str)
        key_order, values_list = self.get_all_as_strs(key_order, values_list, self.csv_to_str)
        self._write_results(key_order, values_list, get_as_str)
        
    def _write_results(self, key_order, values_list, get_as_str):
        # Print exp_list
        self.writeln(self.sep.join(key_order))
        for values in values_list:
            self.writeln(self.sep.join(values))
        self.writeln("")
    
    def writeln(self, line):
        '''Write a line to the output of this Writer.'''
        self.out.write(line + "\n")
    
class RprojResultsWriter(CsvResultsWriter):
        
    def __init__(self, out):
        CsvResultsWriter.__init__(self, out)
        self.sep = "\t"
        self.quote = '"'
        self.whitespace_re = re.compile("\s+")
    
    def csv_to_str(self, x):
        x = self.whitespace_re.sub("_", x)
        if x == "":
            x = "NA"
        return x
    
    def write_top(self, top_dir):
        sys.stderr.write(top_dir + "\n")
        
    def write_readme(self, lines):
        lines = " ".join(lines)
        sys.stderr.write('"' + lines.replace("\n"," ") + '"' + "\n")
        
    def write_error(self, exp_dir):
        sys.stderr.write(self.sep.join(map(to_str,[exp_dir,"ERROR"])) + "\n")
        
    
def add_options(parser):
    '''Takes an OptionParser as input and adds the appropriate options for the Scraper'''
    parser.add_option('--remain', action="store_true", help="Scrape for time remaining only")
    parser.add_option('--errors', action="store_true", help="Scrape for errors only")
    parser.add_option('--tsv_file', help="Out file for R-project")
    parser.add_option('--csv_file', help="Out file for CSV")


class Scraper:
    
    def __init__(self, options):
        self.remain_only = options.remain
        self.errors_only = options.errors
        self.writers = []
        self.closeables = []
        if options.tsv_file:
            tsv_out = open(options.tsv_file, 'w')
            self.writers.append(RprojResultsWriter(tsv_out))
            self.closeables.append(tsv_out)
        if options.csv_file:
            csv_out = open(options.csv_file, 'w')
            self.writers.append(CsvResultsWriter(csv_out))
            self.closeables.append(csv_out)
        if len(self.writers) == 0: 
            self.writers.append(RprojResultsWriter(sys.stdout))

    def read_stdout_lines(self, stdout_file, max_lines=sys.maxint):
        if max_lines == sys.maxint:
            stdout_lines = open(stdout_file, 'r').readlines()
        else:
            stdout_lines = []
            for i,line in enumerate(open(stdout_file, 'r')):
                if i >= max_lines:
                    print "WARN: Read only the first %d lines of the file" % (max_lines)
                    break
                stdout_lines.append(line)
        return stdout_lines

    def scrape(self, top_dir):
        for writer in self.writers:
            writer.write_top(top_dir)
        # Read README
        readme = os.path.join(top_dir, "README")
        if os.path.exists(readme):
            lines = open(readme, 'r').readlines()
            for writer in self.writers:
                writer.write_readme(lines)
                
        exp_dirs = [os.path.join(top_dir,f) for f in os.listdir(top_dir) 
                    if os.path.isdir(os.path.join(top_dir, f)) and f != ".svn"]
        self.scrape_exp_dirs(exp_dirs)
    
    def scrape_exp_dirs(self, exp_dirs):
        # Read experiment directories
        orig_list = [] # List of original expparams objects (used for column ordering).
        exp_list = []  # List of extracted expparams objects.
        for i,exp_dir in enumerate(sorted(exp_dirs)):
            try:
                # Read name
                name = os.path.basename(exp_dir)
                if name.startswith("scrape_") or name.startswith("hyperparam_argmax"):
                    sys.stderr.write("Skipping %s\n" % (name))
                    continue
                sys.stderr.write("Reading %s\n" % (name))
                sys.stderr.flush()

                exp = self.get_exp_params_instance()
                
                stdout_file = os.path.join(exp_dir,"stdout")
                done_file = os.path.join(exp_dir,"DONE")
                is_done = os.path.exists(done_file)
                    
                if self.remain_only:
                    # Really we should only print those that are not completed.
                    # But this is commented out so that we can read off elapsed times as well.
                    #if is_done:
                    #    exp_list.pop()
                    #    continue
                    stdout_lines = self.read_stdout_lines(stdout_file)
                    exp.update(exp_dir=exp_dir)
                    _, _, elapsed = get_time(stdout_lines)
                    exp.update(elapsed = elapsed)
                    exp.update(timeRemaining = get_following_literal(stdout_lines, "Time remaining: ", -1))
                    exp_list.append(exp)
                elif self.errors_only:
                    stdout_lines = self.read_stdout_lines(stdout_file)
                    exp.update(exp_dir=exp_dir)
                    self.scrape_errors(exp, exp_dir, stdout_file)
                    if exp.get("error") is None: continue
                    exp_list.append(exp)
                else:
                    # Read experiment parameters
                    exp.read(os.path.join(exp_dir, "expparams.txt"))                
                    # Append the original parameters
                    orig_list.append(exp + self.get_exp_params_instance())                    
                    # Read the output parameters
                    if os.path.exists(os.path.join(exp_dir, "outparams.txt")):
                        outp = self.get_exp_params_instance()
                        outp.read(os.path.join(exp_dir, "outparams.txt"))
                        exp += outp
                    exp.update(exp_dir=exp_dir)
                    exp.update(is_done=is_done)
                    # Read stdout
                    self.scrape_errors(exp, exp_dir, stdout_file)
                    self.scrape_exp(exp, exp_dir, stdout_file)
                    
                    # Optionally add status lines
                    status_exps = self.scrape_exp_statuses(exp, exp_dir, stdout_file)
                    if status_exps is not None:
                        for status_exp in status_exps:
                            exp_list.append(status_exp)
                    else:
                        exp_list.append(exp)
            except Exception, e:
                for writer in self.writers:
                    writer.write_error(exp_dir)
                import traceback
                sys.stderr.write(str(e) + '\n')
                traceback.print_exc()

        exp_list = self.process_all(orig_list, exp_list)

        # Drop the "old:" prefix for convenience on eval experiments:
        for exp in exp_list:
            #eval wasn't printing expname: if exp.get("expname") == "eval":
            for key in exp.keys():
                if key.startswith("old:"):
                    value = exp.get(key)
                    exp.remove(key)
                    exp.set(key.replace("old:",""), value, False, False)
        
        # Choose column header order
        initial_keys = ["exp_dir", "is_done"]
        exp_orderer = self.get_exp_params_instance()
        for exp in exp_list:
            exp_orderer = exp_orderer.concat(exp)
        exp_orderer.get_initial_keys = lambda : self._get_column_order(initial_keys, orig_list, exp_list)
        key_order = exp_orderer.get_name_key_order()
    
        # Order rows
        values_list = []
        for exp in exp_list:
            values = []
            for key in key_order:
                values.append(exp.get(key))
            values_list.append(values)
        values_list = sorted(values_list)
        
        for writer in self.writers:
            writer.write_results(key_order, values_list, exp_orderer._get_as_str)
            
        # Close any open files.
        for f in self.closeables:
            f.close()
        
    def _get_column_order(self, initial_keys, orig_list, exp_list):
        order = []
        added = set()
        nonunique_keys = get_nonunique_keys(orig_list)
        exclude_name_keys = get_exclude_name_keys(orig_list)
        initial_set = nonunique_keys - exclude_name_keys
        orig_set = get_all_keys(orig_list)
        result_set = get_all_keys(exp_list) - orig_set
        
        unfiltered = initial_keys + list(initial_set) + ["BLANK_COLUMN1"] + self.get_column_order(exp_list) + \
                       ["BLANK_COLUMN2"] + sorted(list(result_set)) + sorted(list(orig_set))
        
        for key in unfiltered:
            if key not in added:
                order.append(key)
                added.add(key)
                
        return order
    
    def scrape_errors(self, exp, exp_dir, stdout_file):
        ''' Optionally override this method '''
        stdout_lines = tail(stdout_file, 500)
        # Skip errors from Shellshock fix for qsub.
        stdout_lines = [x for x in stdout_lines if x.find("module: line 1: syntax error: unexpected end of file") == -1]
        stdout_lines = [x for x in stdout_lines if x.find("error importing function definition for `BASH_FUNC_module'") == -1]
        # Check for errors:
        error = get_following(stdout_lines, "Exception in thread \"main\" ", -1, False)
        if error == None: error = get_group1(stdout_lines, "(.*(Error|Exception):.*)", -1)
        if error == None: error = get_group1(stdout_lines, "(.*[Ee]rror.*)", -1)
        exp.update(error = error)
        
    def get_exp_params_instance(self):
        ''' OVERRIDE THIS METHOD: return an ExpParams object '''
        return ExpParams()

    def get_column_order(self, exp_list):
        ''' OVERRIDE THIS METHOD: return a list of column header strings '''
        return []
    
    def scrape_exp(self, exp, exp_dir, stdout_file):
        ''' OVERRIDE THIS METHOD '''
        pass
    
    def scrape_exp_statuses(self, exp, exp_dir, stdout_file):
        ''' OVERRIDE THIS METHOD: return a list of ExpParam objects '''
        return None
    
    def process_all(self, orig_list, exp_list):
        ''' OVERRIDE THIS METHOD '''
        return exp_list
