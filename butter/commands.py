import os
import re
import sys
import time
import socket
import shutil
import optparse
import subprocess

import anadama.cli
from anadama.skeleton import make_pipeline_skeleton
from anadama.runner import GRID_RUNNER_MAP

from . import config


class ShellException(Exception):
    pass

def sh(cmd, **kwargs):
    kwargs['stdout'] = kwargs.get('stdout', subprocess.PIPE)
    kwargs['stderr'] = kwargs.get('stderr', subprocess.PIPE)
    proc = subprocess.Popen(cmd, **kwargs)
    ret = proc.communicate()
    if proc.returncode:
        raise ShellException("Command `{}' failed. \nOut: {}\nErr: {}".format(
            cmd, ret[0], ret[1]))
    return ret


cd = os.chdir
touch = lambda f: open(f, 'a').close()


dot_gitfat_template = """[rsync]
remote = {hostname}:{fatstore}
"""

hook_scripts = {
    "hooks/update": """#!/bin/bash
source {virtualenv}/bin/activate;
export GIT_DIR={gdir};
cd "$GIT_DIR/../"
butter update "$@";\n""",

    "hooks/post-receive": """#!/bin/bash
source {virtualenv}/bin/activate;
export GIT_DIR={gdir};
cd "$GIT_DIR/../"
butter post-receive "$@";\n""",
}

commit_scripts = {
    "push.sh": """#!/bin/bash
git fat init
git add .
git commit -m add
git fat push
git push origin master
""",

    "pull.sh": """#!/bin/bash
git fat pull
git pull origin master
"""
}


def _find_input_dirs(products):
    dirs = [ os.path.join("input", p) for p in products ]
    return filter(os.path.isdir, dirs)


def _write_scripts(scripts_dict, *args, **kwargs):
    for name, content in scripts_dict.iteritems():
        with open(name, 'w') as f:
            print >> f, content.format(*args, **kwargs)
        os.chmod(name, 0o755)


def is_autocommit(rev_str):
    commit_msg, _ = sh(["git", "show", "--format='%an %s'", "-s", rev_str])
    return commit_msg == "{} {}".format(config.get("master_user"),
                                        config.get("autocommit_msg"))

def get_commit_hash(rev_str):
    return sh(["git", "show", "--format='%h'", '-s', rev_str])[0]


def get_reporter_url(project_name):
    return config.get("reporter_url").format(project_name)

def get_runner_options():
    runner = config.get("runner")
    n_runners = str(config.get("n_runners"))
    opts = ["--runner", runner, "-n", n_runners ]
    if runner in GRID_RUNNER_MAP:
        partition = config.get("partition")
        opts += ["--partition", partition]
    return opts


def setup_repo(repo_path, pipeline, opt_pipelines, cleanup=True):
    work_path = repo_path
    repo_path = repo_path+'.git'
    work_gitdir = os.path.join(work_path, ".git")
    repo_path, work_path, work_gitdir = map(
        os.path.abspath, (repo_path, work_path, work_gitdir))
    orig_path = os.getcwd()
    if not os.path.exists(config.get("fatstore")):
        os.mkdir(config.get("fatstore"))

    def _do_setup():
        sh(["git", "init", "--bare", "--quiet", repo_path])
        cd(repo_path)
        sh(["git", "fat", "init"])
        cd(orig_path)
        sh(["git", "clone", "--quiet",repo_path, work_path])

        cd(work_path)
        sh(["git", "config", "user.name", config.get("master_user")])
        sh(["git", "config", "user.email", config.get("master_email")])
        sh(["git", "fat", "init"])
        allprods, _, _, = make_pipeline_skeleton(
            pipeline, opt_pipelines, verbose=False)
        with open(".gitfat", 'w') as f:
            print >> f, dot_gitfat_template.format(
                hostname=socket.gethostname(),
                fatstore=config.get("fatstore"))
        with open(".gitattributes", 'w') as f:
            for d in _find_input_dirs(allprods):
                print >> f, d+"/* filter=fat -crlf"
        _write_scripts(commit_scripts)
        sh(["git", "add", "."])
        sh(["git", "commit", "--quiet", "-m", config.get("autocommit_msg")])
        sh(["git", "push", "--quiet", "origin", "master"])

        cd(repo_path)
        _write_scripts(hook_scripts,
                       gdir=work_gitdir, virtualenv=config.get("virtualenv"))
        cd(orig_path)


    try:
        _do_setup()
    except Exception as e:
        if cleanup:
            shutil.rmtree(repo_path, True)
            shutil.rmtree(work_path, True)
        raise
        
    

def setup_cmd(argv):
    HELP = "Create an AnADAMA git repository"

    options = [
        optparse.make_option(
            '-A', '--append', dest="optional_pipelines",
            action="append", type="string", default=[],
            help="Append additional optional pipelines to the repository"),
        optparse.make_option(
            '-p', '--pipeline', dest="main_pipeline",
            action="store", type="string", default=None,
            help="Specify main pipeline"),
        optparse.make_option(
            '-d', '--dir', dest="repo_path",
            action="store", type="string", default=None,
            help="Filesystem path to repository"),
        optparse.make_option(
            '-m', '--messy', dest="be_messy",
            action="store_true", default=False,
            help="Be messy and don't clean up after mistakes")
    ]

    parser = optparse.OptionParser(option_list=options, usage=HELP)
    opts, _ = parser.parse_args(args=argv)

    if not all((opts.main_pipeline, opts.repo_path)):
        parser.print_usage()
        sys.exit(1)

    setup_repo(opts.repo_path, opts.main_pipeline, opts.optional_pipelines,
               cleanup=not opts.be_messy)


def update_hook(argv):
    workdir = os.path.abspath(os.path.join(os.environ['GIT_DIR'], '..'))
    project_name = os.path.basename(workdir).rstrip(".work")
    cd(workdir)
    if is_autocommit("HEAD"):
        sys.exit(0) # this lets autocommits slip by the work in progess check
    if os.path.exists(".workinprogress"):
        print ("ERROR - The current run isn't finished. "
               "Check its status at "+get_reporter_url(project_name))
        sys.exit(1)

    

def find_big_files(startdir, ignore_files):
    ignore = set(ignore_files)
    for _, _, files in os.walk(startdir):
        for fname in files:
            if os.path.exists \
               and os.stat(fname).st_size > config.get("large_file_bytes") \
               and fname not in ignore:
                yield fname


def run_anadama(workdir, project_name):
    os.environ['GIT_WORK_TREE'] = workdir
    gitdir = os.environ['GIT_DIR'] = os.path.join(workdir, ".git")
    os.environ['GIT_INDEX_FILE'] = os.path.join(gitdir, "index")
    os.environ['GIT_OBJECT_DIRECTORY'] = os.path.join(gitdir, "objects")

    cd(workdir)
    sh(["git", "pull", "--quiet", "origin", "master"])
    # sleep to ensure git-fat's utime is in the future
    # c.f. git-fat vd5b388cbe line 416
    time.sleep(1)
    sh(["git", "fat", "pull"])
    touch(".workinprogress")
    try:
        anadama.cli.main(
            argv=["anadama", "run", "--reporter", "web",
                  "--reporter-url", get_reporter_url(project_name)]
            +get_runner_options()
        )
    except:
        import pprint; pprint.pprint(os.environ)
        raise
    finally:
        os.remove(".workinprogress")
    with open(".gitattributes", 'w+') as f:
        current_bigfiles = [ re.sub(r' filter\s*=\s*fat.*$', '', name)
                             for name in f
                             if re.search('filter\s*=\s*fat', name) ]
        for bigfile in find_big_files("anadama_workflows", current_bigfiles):
            print >> f, bigfile+" filter=fat -crlf"
    sh(["git", "add", "anadama_products", ".gitattributes"])
    sh(["git", "commit", "--quiet", "-m", config.get("autocommit_msg")])
    sh(["git", "fat", "push"])
    sh(["git", "push", "--quiet", "origin", "master"])


def daemonize_anadama(workdir, projectname, logfile):
    cd(workdir)
    pid = os.fork()
    if pid == 0:
        os.setsid()
        pid = os.fork() # second child
        if pid == 0:
            os.chdir(workdir)
            # os.umask(0) maybe necessary in the future
        else: # parent of second child
            os._exit(0)
    else: # parent of first child
        os._exit(0)

    # now in second child only
    import resource
    _, max_fds = resource.getrlimit(resource.RLIMIT_NOFILE)
    if max_fds == resource.RLIM_INFINITY:
        max_fds = 1024
    os.closerange(0, max_fds)
    
    fd = os.open(logfile, os.O_CREAT|os.O_RDWR, 0o644)
    os.dup2(fd, 1)
    os.dup2(fd, 2)
    run_anadama(workdir, projectname)
    os.fsync(fd)
    sys.exit()


def post_receive_hook(argv):
    if is_autocommit("HEAD"):
        sys.exit(0)
    workdir = os.path.abspath(os.path.join(os.environ['GIT_DIR'], '..'))
    projectname = os.path.basename(workdir).rstrip(".work")
    logfile = os.path.abspath(os.path.join(workdir, "..", projectname+".log"))
    print "Launching anadama at "+get_reporter_url(projectname)
    daemonize_anadama(workdir, projectname, logfile)
    
