from __future__ import with_statement

import sys
from fabric.state import env
from fabric.api import local, settings, abort, run, cd, lcd
from fabric.contrib.console import confirm
from fabric.context_managers import prefix

def init():
    env.use_ssh_config = True
    env.host_string = "%s:%s" % ("test3", "22")
    run("uname -a")
        
def prep_project_git(name, install_cmd, check_local=True):
    with lcd("~/research/%s" % (name)):
        local("git push")
        if "nothing to commit, working directory clean" not in local("git status", capture=True):
            print "\nERROR: project requires git commit/git push: %s\n" % name
            if check_local:
                sys.exit(1)    
    with cd("~/working/%s" % (name)):
        if "Already up-to-date" not in run("git pull"):
            run(install_cmd)

def prep_project_mvn(name, mvn_cmd, check_local=True):
    install_cmd = "mvn %s -DskipTests" % (mvn_cmd)
    prep_project_git(name, install_cmd, check_local)

def prep_project_py(name, check_local=True):
    install_cmd = "python setup.py develop --user"
    prep_project_git(name, install_cmd, check_local)

def prep_project_make(name, make_cmd, check_local=True):
    install_cmd = "make %s" % (make_cmd)
    prep_project_git(name, install_cmd, check_local)

def run_command(name, argv):
    args = " ".join(argv[1:])
    with cd("~/working/%s" % (name)):
        with prefix("source setupenv.sh"):
            run("%s" % (args))
    