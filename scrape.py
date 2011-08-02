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
    to_str, get_following_literal
from .google_spreadsheets import get_spreadsheet_by_title,\
    get_first_worksheet, clear_worksheet, write_row
import getpass
import gdata.spreadsheet.service

class Scraper:
    
    def __init__(self, print_csv=True, write_google=False, remain_only=False):
        self.sep = ","
        self.print_csv = print_csv
        self.write_google = write_google
        self.remain_only = remain_only
        if self.write_google:
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
                
                exp = self.get_exp_params_instance()
                exp_list.append(exp)
                
                stdout_file = os.path.join(exp_dir,"stdout")
                done_file = os.path.join(exp_dir,"DONE")

                if self.remain_only:
                    # Really we should only print those that are not completed.
                    # But this is commented out so that we can read off elapsed times as well.
                    #if os.path.exists(done_file):
                    #    exp_list.pop()
                    #    continue
                    exp.update(exp_dir=exp_dir)
                    _, _, elapsed = get_time(stdout_file)
                    exp.update(elapsed = elapsed)
                    exp.update(timeRemaining = get_following(stdout_file, "Time remaining: ", -1))
                else:
                    # Read experiment parameters
                    exp.read(os.path.join(exp_dir, "expparams.txt"))
                    # Read stdout
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
            sys.stderr.write("Writing to Google Spreadsheet...\n")
            # Update the worksheet
            self.write_row(key_order)
            for values in values_list:
                self.write_row(map(csv_to_str, values))

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
