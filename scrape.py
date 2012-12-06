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
from .util import get_all_following, get_following, get_time,\
    to_str, get_following_literal, tail
from .google_spreadsheets import get_spreadsheet_by_title,\
    get_first_worksheet, clear_worksheet, write_row
import getpass
import gdata.spreadsheet.service

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
        # TODO: should we post this to Google Spreadsheet?
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
           
class GoogleResultsWriter(CsvResultsWriter):
    
    def __init__(self):
        CsvResultsWriter.__init__(self)
        # Get Password
        email = 'matthew.gormley@gmail.com'
        print "Enter password for",email
        password = getpass.getpass()
        
        # Establish connection            
        self.gd_client = gdata.spreadsheet.service.SpreadsheetsService()
        self.gd_client.email = email
        self.gd_client.password = password
        self.gd_client.source = 'exampleCo-exampleApp-1'
        self.gd_client.ProgrammaticLogin()

        # Get the worksheet
        self.skey = get_spreadsheet_by_title(self.gd_client, "Temporary Results")
        print "Spreadsheet key:",self.skey
        self.wksht_id = get_first_worksheet(self.gd_client, self.skey)
        print "Worksheet id:",self.wksht_id
        
        # Clear the worksheet
        clear_worksheet(self.gd_client, self.skey, self.wksht_id)
        self.row_num = 1
        #TODO: Use self.gd_client.UpdateWorksheet(worksheet_entry) to update the num_rows/num_cols automatically
        
    def write_row(self, row):
        write_row(self.gd_client, self.skey, self.wksht_id, self.row_num, row) 
        self.row_num += 1    
        
    def _write_results(self, key_order, values_list, get_as_str):
        sys.stderr.write("Writing to Google Spreadsheet...\n")
        # Update the worksheet
        self.write_row(key_order)
        for values in values_list:
            self.write_row(values)
        
    def write_readme(self, lines):
        pass
    
    def write_error(self, exp_dir):
        pass
    
class Scraper:
    
    def __init__(self, print_csv=True, write_google=False, remain_only=False, print_rproj=False, out_file=None):
        self.remain_only = remain_only
        self.writers = []
        if out_file:
            out = open(out_file, 'w')
        else:
            out = sys.stdout
        if print_rproj: self.writers.append(RprojResultsWriter(out))
        if write_google: self.writers.append(GoogleResultsWriter())
        if print_csv or len(self.writers) == 0: self.writers.append(CsvResultsWriter(out))

    def read_stdout_lines(self, stdout_file, max_lines=sys.maxint):
        if max_lines == sys.maxint:
            stdout_lines = open(stdout_file, 'r').readlines()
        else:
            stdout_lines = []
            for i,line in enumerate(open(stdout_file, 'r')):
                if i >= max_lines:
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
        exp_list = []
        for exp_dir in sorted(exp_dirs):
            try:
                # Read name
                name = os.path.basename(exp_dir)
                sys.stderr.write("Reading %s\n" % (name))
                sys.stderr.flush()

                exp = self.get_exp_params_instance()
                
                stdout_file = os.path.join(exp_dir,"stdout")
                done_file = os.path.join(exp_dir,"DONE")

                if self.remain_only:
                    # Really we should only print those that are not completed.
                    # But this is commented out so that we can read off elapsed times as well.
                    #if os.path.exists(done_file):
                    #    exp_list.pop()
                    #    continue
                    stdout_lines = self.read_stdout_lines(stdout_file)
                    exp.update(exp_dir=exp_dir)
                    _, _, elapsed = get_time(stdout_lines)
                    exp.update(elapsed = elapsed)
                    exp.update(timeRemaining = get_following_literal(stdout_lines, "Time remaining: ", -1))
                    exp_list.append(exp)
                else:
                    # Read experiment parameters
                    exp.read(os.path.join(exp_dir, "expparams.txt"))
                    exp.update(exp_dir=exp_dir)
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
        
        for writer in self.writers:
            writer.write_results(key_order, values_list, exp_orderer._get_as_str)

    def get_exp_params_instance(self):
        ''' OVERRIDE THIS METHOD: return an ExpParams object '''
        return None

    def get_column_order(self):
        ''' OVERRIDE THIS METHOD: return a list of column header strings '''
        return []
    
    def scrape_exp(self, exp, exp_dir, stdout_file):
        ''' OVERRIDE THIS METHOD '''
        pass
    
    def scrape_exp_statuses(self, exp, exp_dir, stdout_file):
        ''' OVERRIDE THIS METHOD: return a list of ExpParam objects '''
        return None
    
    def scrape_errors(self, exp, exp_dir, stdout_file):
        ''' Optionally override this method '''
        stdout_lines = tail(stdout_file, 500)
        # Check for errors:
        error = get_following(stdout_lines, "Exception in thread \"main\" ", 0, False)
        if error == None: error = get_following(stdout_lines, "Error ", 0, True)
        exp.update(error = error)
