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
from experiments.core.util import get_all_following, get_following, get_time,\
    to_str, get_following_literal

class Scraper:
    
    def __init__(self, print_csv=True, write_google=False):
        self.sep = ","
        self.print_csv = print_csv
        self.write_google = write_google

    def scrape(self, top_dir):
        exp_dirs = [os.path.join(top_dir,f) for f in os.listdir(top_dir) 
                    if os.path.isdir(os.path.join(top_dir, f)) and f != ".svn"]
        print top_dir
        # Read README
        readme = os.path.join(top_dir, "README")
        if os.path.exists(readme):
            lines = open(readme, 'r').readlines()
            lines = " ".join(lines)
            print '"' + lines.replace("\n"," ") + '"'
            
        # Read experiment directories
        exp_list = []
        for exp_dir in sorted(exp_dirs):
            try:
                # Read name
                name = os.path.basename(exp_dir)
                sys.stderr.write("Reading %s\n" % (name))
                
                # Read experiment parameters
                exp = self.get_exp_params_instance()
                exp_list.append(exp)
                exp.read(os.path.join(exp_dir, "expparams.txt"))
                
                # Read stdout
                stdout_file = os.path.join(exp_dir,"stdout")

                self.scrape_errors(exp, exp_dir, stdout_file)

                self.scrape_exp(exp, exp_dir, stdout_file)
                
            except Exception, e:
                # TODO: should we post this to Google Spreadsheet?
                print self.sep.join(map(to_str,[exp_dir,"ERROR"]))
                import traceback
                sys.stderr.write(str(e) + '\n')
                traceback.print_exc()
        
        # Drop the "old:" prefix for convenience on eval experiments:
        for exp in exp_list:
            #eval wasn't printing expname: if exp.get("expname") == "eval":
            for key in exp.keys():
                if key.startswith("old:"):
                    value = exp.get(key)
                    exp.remove(key)
                    exp.set(key.replace("old:",""), value, False, False)
        
        # Choose column header order
        exp_orderer = self.get_exp_params_instance()
        for exp in exp_list:
            exp_orderer = exp_orderer.concat(exp)
        exp_orderer.get_initial_keys = lambda : self.get_column_order()
        key_order = exp_orderer.get_name_key_order()
    
        # Order rows
        values_list = []
        for exp in exp_list:
            values = []
            for key in key_order:
                values.append(exp.get(key))
            values_list.append(values)
        values_list = sorted(values_list)
        
        if self.print_csv:
            # Print exp_list
            def csv_to_str(x):
                x = exp_orderer._get_as_str(x)
                if x.find(",") != -1:
                    x = '"%s"' % x
                return x
            print self.sep.join(map(csv_to_str, key_order))
            for values in values_list:
                print self.sep.join(map(csv_to_str, values))
            print ""
        if self.write_google:
            pass
    
    def get_exp_params_instance(self):
        ''' OVERRIDE THIS METHOD: return an ExpParams object '''
        return None

    def get_column_order(self):
        ''' OVERRIDE THIS METHOD: return a list of column header strings '''
        return []
    
    def scrape_exp(self, exp, exp_dir, stdout_file):
        ''' OVERRIDE THIS METHOD '''
        pass
    
    def scrape_errors(self, exp, exp_dir, stdout_file):
        ''' Optionally override this method '''
        # Check for errors:
        error = get_following(stdout_file, "Exception in thread \"main\" ", 0, False)
        if error == None: error = get_following(stdout_file, "Error ", 0, True)
        exp.update(error = error)
