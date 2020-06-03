#!/usr/bin/env python2.7

__version__ = '1.1'

license = """
    Process MeerKAT data via CASA measurement set.
    Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy.
    support@ilifu.ac.za

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <https://www.gnu.org/licenses/>.
"""

import argparse
import os
import sys
import re
import config_parser
from cal_scripts import bookkeeping
from shutil import copyfile
import logging
from time import gmtime
from datetime import datetime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s")

#Set global limits for current ilifu cluster configuration
TOTAL_NODES_LIMIT = 79
CPUS_PER_NODE_LIMIT = 32
NTASKS_PER_NODE_LIMIT = CPUS_PER_NODE_LIMIT
MEM_PER_NODE_GB_LIMIT = 236 #241664 MB
MEM_PER_NODE_GB_LIMIT_HIGHMEM = 482 #493568 MB

#Set global values for paths and file names
THIS_PROG = __file__
SCRIPT_DIR = os.path.dirname(THIS_PROG)
LOG_DIR = 'logs'
CALIB_SCRIPTS_DIR = 'cal_scripts'
AUX_SCRIPTS_DIR = 'aux_scripts'
SELFCAL_SCRIPTS_DIR = 'selfcal_scripts'
CONFIG = 'default_config.txt'
TMP_CONFIG = '.config.tmp'
MASTER_SCRIPT = 'submit_pipeline.sh'

#Set global values for field, crosscal and SLURM arguments copied to config file, and some of their default values
FIELDS_CONFIG_KEYS = ['fluxfield','bpassfield','phasecalfield','targetfields']
CROSSCAL_CONFIG_KEYS = ['minbaselines','preavg','specavg','timeavg','spw','nspw','calcrefant','refant','standard','badants','badfreqranges','keepmms']
SELFCAL_CONFIG_KEYS = ['nloops','restart_no','cell','robust','imsize','wprojplanes','niter','threshold','multiscale','nterms','gridder','deconvolver','solint','calmode','atrous']
SLURM_CONFIG_STR_KEYS = ['container','mpi_wrapper','partition','time','name','dependencies','exclude','account','reservation']
SLURM_CONFIG_KEYS = ['nodes','ntasks_per_node','mem','plane','submit','precal_scripts','postcal_scripts','scripts','verbose'] + SLURM_CONFIG_STR_KEYS
CONTAINER = '/idia/software/containers/casa-stable-5.6.2-2.simg'
MPI_WRAPPER = '/idia/software/pipelines/casa-pipeline-release-5.6.1-8.el7/bin/mpicasa'
PRECAL_SCRIPTS = [('calc_refant.py',False,''),('partition.py',True,'')] #Scripts run before calibration at top level directory when nspw > 1
POSTCAL_SCRIPTS = [('concat.py',False,''),('selfcal_part1.py',True,''),('selfcal_part2.py',False,''),('run_bdsf.py', False, ''),('make_pixmask.py', False, '')] #Scripts run after calibration at top level directory when nspw > 1
SCRIPTS = [ ('validate_input.py',False,''),
            ('flag_round_1.py',True,''),
            ('calc_refant.py',False,''),
            ('setjy.py',True,''),
            ('xx_yy_solve.py',False,''),
            ('xx_yy_apply.py',True,''),
            ('flag_round_2.py',True,''),
            ('xx_yy_solve.py',False,''),
            ('xx_yy_apply.py',True,''),
            ('split.py',True,''),
            ('quick_tclean.py',True,''),
            ('plot_solutions.py',False,'')]


def check_path(path,update=False):

    """Check in specific location for a script or container, including in bash path, and in this pipeline's calibration
    scripts directory (SCRIPT_DIR/{CALIB_SCRIPTS_DIR,AUX_SCRIPTS_DIR}/). If path isn't found, raise IOError, otherwise return the path.

    Arguments:
    ----------
    path : str
        Check for script or container at this path.
    update : bool, optional
        Update the path according to where the file is found.

    Returns:
    --------
    path : str
        Path to script or container (if path found and update=True)."""

    #Attempt to find path in pipeline directories and bash path first
    if not os.path.exists(path) and path != '':
        if os.path.exists('../{0}'.format(path)):
            newpath = '../{0}'.format(path)
        elif os.path.exists(check_bash_path(path)):
            newpath = check_bash_path(path)
        elif os.path.exists('{0}/{1}/{2}'.format(SCRIPT_DIR,CALIB_SCRIPTS_DIR,path)):
            newpath = '{0}/{1}/{2}'.format(SCRIPT_DIR,CALIB_SCRIPTS_DIR,path)
        elif os.path.exists('{0}/{1}/{2}'.format(SCRIPT_DIR,AUX_SCRIPTS_DIR,path)):
            newpath = '{0}/{1}/{2}'.format(SCRIPT_DIR,AUX_SCRIPTS_DIR,path)
        elif os.path.exists('{0}/{1}/{2}'.format(SCRIPT_DIR,SELFCAL_SCRIPTS_DIR,path)):
            newpath = '{0}/{1}/{2}'.format(SCRIPT_DIR,SELFCAL_SCRIPTS_DIR,path)
        else:
            #If it still doesn't exist, throw error
            raise IOError('File "{0}" not found.'.format(path))
    else:
        newpath = path

    if update:
        return newpath
    else:
        return path

def check_bash_path(fname):

    """Check if file is in your bash path and executable (i.e. executable from command line), and prepend path to it if so.

    Arguments:
    ----------
    fname : str
        Filename to check.

    Returns:
    --------
    fname : str
        Potentially updated filename with absolute path prepended."""

    PATH = os.environ['PATH'].split(':')
    for path in PATH:
        if os.path.exists('{0}/{1}'.format(path,fname)):
            if not os.access('{0}/{1}'.format(path,fname), os.X_OK):
                raise IOError('"{0}" found in "{1}" but file is not executable.'.format(fname,path))
            else:
                fname = '{0}/{1}'.format(path,fname)
            break

    return fname

def parse_args():

    """Parse arguments into this script.

    Returns:
    --------
    args : class ``argparse.ArgumentParser``
        Known and validated arguments."""

    def parse_scripts(val):

        """Format individual arguments passed into a list for [ -S --scripts] argument, including paths and boolean values.

        Arguments/Returns:
        ------------------
        val : bool or str
            Path to script or container, or boolean representing whether that script is threadsafe (for MPI)."""

        if val.lower() in ('true','false'):
            return (val.lower() == 'true')
        else:
            return check_path(val)

    parser = argparse.ArgumentParser(prog=THIS_PROG,description='Process MeerKAT data via CASA measurement set. Version: {0}'.format(__version__))

    parser.add_argument("-M","--MS",metavar="path", required=False, type=str, help="Path to measurement set.")
    parser.add_argument("-C","--config",metavar="path", default=CONFIG, required=False, type=str, help="Relative (not absolute) path to config file.")
    parser.add_argument("-N","--nodes",metavar="num", required=False, type=int, default=8,
                        help="Use this number of nodes [default: 8; max: {0}].".format(TOTAL_NODES_LIMIT))
    parser.add_argument("-t","--ntasks-per-node", metavar="num", required=False, type=int, default=4,
                        help="Use this number of tasks (per node) [default: 4; max: {0}].".format(NTASKS_PER_NODE_LIMIT))
    parser.add_argument("-P","--plane", metavar="num", required=False, type=int, default=1,
                            help="Distribute tasks of this block size before moving onto next node [default: 1; max: ntasks-per-node].")
    parser.add_argument("-m","--mem", metavar="num", required=False, type=int, default=MEM_PER_NODE_GB_LIMIT,
                        help="Use this many GB of memory (per node) for threadsafe scripts [default: {0}; max: {0}].".format(MEM_PER_NODE_GB_LIMIT))
    parser.add_argument("-p","--partition", metavar="name", required=False, type=str, default="Main", help="SLURM partition to use [default: 'Main'].")
    parser.add_argument("-T","--time", metavar="time", required=False, type=str, default="12:00:00", help="Time limit to use for all jobs, in the form d-hh:mm:ss [default: '12:00:00'].")
    parser.add_argument("-S","--scripts", action='append', nargs=3, metavar=('script','threadsafe','container'), required=False, type=parse_scripts, default=SCRIPTS,
                        help="Run pipeline with these scripts, in this order, using these containers (3rd value - empty string to default to [-c --container]). Is it threadsafe (2nd value)?")
    parser.add_argument("-b","--precal_scripts", action='append', nargs=3, metavar=('script','threadsafe','container'), required=False, type=parse_scripts, default=PRECAL_SCRIPTS, help="Same as [-S --scripts], but run before calibration.")
    parser.add_argument("-a","--postcal_scripts", action='append', nargs=3, metavar=('script','threadsafe','container'), required=False, type=parse_scripts, default=POSTCAL_SCRIPTS, help="Same as [-S --scripts], but run after calibration.")
    parser.add_argument("-w","--mpi_wrapper", metavar="path", required=False, type=str, default=MPI_WRAPPER,
                        help="Use this mpi wrapper when calling threadsafe scripts [default: '{0}'].".format(MPI_WRAPPER))
    parser.add_argument("-c","--container", metavar="path", required=False, type=str, default=CONTAINER, help="Use this container when calling scripts [default: '{0}'].".format(CONTAINER))
    parser.add_argument("-n","--name", metavar="unique", required=False, type=str, default='', help="Unique name to give this pipeline run (e.g. 'run1_'), appended to the start of all job names. [default: ''].")
    parser.add_argument("-d","--dependencies", metavar="list", required=False, type=str, default='', help="Comma-separated list (without spaces) of SLURM job dependencies (only used when nspw=1). [default: ''].")
    parser.add_argument("-e","--exclude", metavar="nodes", required=False, type=str, default='', help="SLURM worker nodes to exclude [default: ''].")
    parser.add_argument("-A","--account", metavar="group", required=False, type=str, default='b03-idia-ag', help="SLURM accounting group to use (e.g. 'b05-pipelines-ag' - check 'sacctmgr show user $(whoami) -s format=account%%30') [default: 'b03-idia-ag'].")
    parser.add_argument("-r","--reservation", metavar="name", required=False, type=str, default='', help="SLURM reservation to use. [default: ''].")

    parser.add_argument("-l","--local", action="store_true", required=False, default=False, help="Build config file locally (i.e. without calling srun) [default: False].")
    parser.add_argument("-s","--submit", action="store_true", required=False, default=False, help="Submit jobs immediately to SLURM queue [default: False].")
    parser.add_argument("-v","--verbose", action="store_true", required=False, default=False, help="Verbose output? [default: False].")
    parser.add_argument("-q","--quiet", action="store_true", required=False, default=False, help="Activate quiet mode, with suppressed output [default: False].")
    parser.add_argument("-D","--dopol", action="store_true", required=False, default=False, help="Perform polarization calibration in the pipeline [default: False].")
    parser.add_argument("-2","--do2GC", action="store_true", required=False, default=False, help="Perform (2GC) self-calibration in the pipeline [default: False].")
    parser.add_argument("-x","--nofields", action="store_true", required=False, default=False, help="Do not read the input MS to extract field IDs [default: False].")

    #add mutually exclusive group - don't want to build config, run pipeline, or display version at same time
    run_args = parser.add_mutually_exclusive_group(required=True)
    run_args.add_argument("-B","--build", action="store_true", required=False, default=False, help="Build config file using input MS.")
    run_args.add_argument("-R","--run", action="store_true", required=False, default=False, help="Run pipeline with input config file.")
    run_args.add_argument("-V","--version", action="store_true", required=False, default=False, help="Display the version of this pipeline and quit.")
    run_args.add_argument("-L","--license", action="store_true", required=False, default=False, help="Display this program's license and quit.")

    args, unknown = parser.parse_known_args()

    if len(unknown) > 0:
        parser.error('Unknown input argument(s) present - {0}'.format(unknown))

    if args.run:
        if args.config is None:
            parser.error("You must input a config file [--config] to run the pipeline.")
        if not os.path.exists(args.config):
            parser.error("Input config file '{0}' not found. Please set [-C --config].".format(args.config))

    #if user inputs a list a scripts, remove the default list
    if len(args.scripts) > len(SCRIPTS):
        [args.scripts.pop(0) for i in range(len(SCRIPTS))]
    if len(args.precal_scripts) > len(PRECAL_SCRIPTS):
        [args.precal_scripts.pop(0) for i in range(len(PRECAL_SCRIPTS))]
    if len(args.postcal_scripts) > len(POSTCAL_SCRIPTS):
        [args.postcal_scripts.pop(0) for i in range(len(POSTCAL_SCRIPTS))]

    # If --dopol is passed in, replace second call of xx_yy* with xy_yx*
    if args.dopol:
        count = 0
        for ind, ss in enumerate(SCRIPTS):
            if ss[0] == 'xx_yy_solve.py' or ss[0] == 'xx_yy_apply.py':
                count += 1

            if count > 2:
                if ss[0] == 'xx_yy_solve.py':
                    SCRIPTS[ind] = ('xy_yx_solve.py', False, '')
                if ss[0] == 'xx_yy_apply.py':
                    SCRIPTS[ind] = ('xy_yx_apply.py', True, '')


    #validate arguments before returning them
    validate_args(vars(args),args.config,parser=parser)
    return args

def raise_error(config,msg,parser=None):

    """Raise error with specified message, either as parser error (when option passed in via command line),
    or ValueError (when option passed in via config file).

    Arguments:
    ----------
    config : str
        Path to config file.
    msg : str
        Error message to display.
    parser : class ``argparse.ArgumentParser``, optional
        If this is input, parser error will be raised."""

    if parser is None:
        raise ValueError("Bad input found in '{0}' -- {1}".format(config,msg))
    else:
        parser.error(msg)

def validate_args(args,config,parser=None):

    """Validate arguments, coming from command line or config file. Raise relevant error (parser error or ValueError) if invalid argument found.

    Arguments:
    ----------
    args : dict
        Dictionary of slurm arguments from command line or config file.
    config : str
        Path to config file.
    parser : class ``argparse.ArgumentParser``, optional
        If this is input, parser error will be raised."""

    if parser is None or args['build']:
        if args['MS'] is None and not args['nofields']:
            msg = "You must input an MS [-M --MS] to build the config file."
            raise_error(config, msg, parser)

        if args['MS'] not in [None,'None'] and not os.path.isdir(args['MS']):
            msg = "Input MS '{0}' not found.".format(args['MS'])
            raise_error(config, msg, parser)

    if args['ntasks_per_node'] > NTASKS_PER_NODE_LIMIT:
        msg = "The number of tasks per node [-t --ntasks-per-node] must not exceed {0}. You input {1}.".format(NTASKS_PER_NODE_LIMIT,args['ntasks_per_node'])
        raise_error(config, msg, parser)

    if args['nodes'] > TOTAL_NODES_LIMIT:
        msg = "The number of nodes [-N --nodes] per node must not exceed {0}. You input {1}.".format(TOTAL_NODES_LIMIT,args['nodes'])
        raise_error(config, msg, parser)

    if args['mem'] > MEM_PER_NODE_GB_LIMIT:
        if args['partition'] != 'HighMem':
            msg = "The memory per node [-m --mem] must not exceed {0} (GB). You input {1} (GB).".format(MEM_PER_NODE_GB_LIMIT,args['mem'])
            raise_error(config, msg, parser)
        elif args['mem'] > MEM_PER_NODE_GB_LIMIT_HIGHMEM:
            msg = "The memory per node [-m --mem] must not exceed {0} (GB) when using 'HighMem' partition. You input {1} (GB).".format(MEM_PER_NODE_GB_LIMIT_HIGHMEM,args['mem'])
            raise_error(config, msg, parser)

    if args['plane'] > args['ntasks_per_node']:
        msg = "The value of [-P --plane] cannot be greater than the tasks per node [-t --ntasks-per-node] ({0}). You input {1}.".format(args['ntasks_per_node'],args['plane'])
        raise_error(config, msg, parser)

    if args['account'] not in ['b03-idia-ag','b05-pipelines-ag']:
        from platform import node
        if node() == 'slurm-login' or 'slwrk' in node():
            accounts=os.popen("for f in $(sacctmgr show user $(whoami) -s format=account%30 | grep -v 'Account\|--'); do echo -n $f,; done").read()[:-1].split(',')
            if args['account'] not in accounts:
                msg = "Accounting group '{0}' not recognised. Please select one of the following from your groups: {1}.".format(args['account'],accounts)
                raise_error(config, msg, parser)
        else:
            msg = "Accounting group '{0}' not recognised. You're not using a SLURM node, so cannot query your accounts.".format(args['account'])
            raise_error(config, msg, parser)


def write_command(script,args,name='job',mpi_wrapper=MPI_WRAPPER,container=CONTAINER,casa_script=True,casacore=False,logfile=True,plot=False,SPWs=''):

    """Write bash command to call a script (with args) directly with srun, or within sbatch file, optionally via CASA.

    Arguments:
    ----------
    script : str
        Path to script called (assumed to exist or be in PATH or calibration scripts directory).
    args : str
        Arguments to pass into script. Use '' for no arguments.
    name : str, optional
        Name of this job, to append to CASA output name.
    mpi_wrapper : str, optional
        MPI wrapper for this job. e.g. 'srun', 'mpirun', 'mpicasa' (may need to specify path).
    container : str, optional
        Path to singularity container used for this job.
    casa_script : bool, optional
        Is the script that is called within this job a CASA script?
    casacore : bool, optional
        Is the script that is called within this job a casacore script?
    logfile : bool, optional
        Write the CASA output to a log file? Only used if casa_script==True.
    plot : bool, optional
        This job is a plotting task that needs to call xvfb-run.
    SPWs : str, optional
        Comma-separated list of spw ranges.

    Returns:
    --------
    command : str
        Bash command to call with srun or within sbatch file."""

    arrayJob = ',' in SPWs and 'partition' in script

    #Store parameters passed into this function as dictionary, and add to it
    params = locals()
    params['LOG_DIR'] = LOG_DIR
    params['job'] = '${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}' if arrayJob else '${SLURM_JOB_ID}'
    params['job'] = '${SLURM_JOB_NAME}-' + params['job']
    params['casa_call'] = ''
    params['casa_log'] = '--nologfile'
    params['plot_call'] = ''
    command = ''

    #If script path doesn't exist and is not in user's bash path, assume it's in the calibration scripts directory (TODO: don't assume this!)
    if not os.path.exists(script):
        params['script'] = check_path(script, update=True)

    #If specified by user, call script via CASA, call with xvfb-run, and write output to log file
    if plot:
        params['plot_call'] = 'xvfb-run -a'
    if logfile:
        params['casa_log'] = '--logfile {LOG_DIR}/{job}.casa'.format(**params)
    if casa_script:
        params['casa_call'] = "{plot_call} casa --nologger --nogui {casa_log} -c".format(**params)
    if casacore:
        params['singularity_call'] = 'run' #points to python that comes with CASA, including casac
    else:
        params['singularity_call'] = 'exec'
    if not casa_script and not casacore:
        params['casa_call'] = 'python'

    if arrayJob:
        command += """#Iterate over SPWs in job array, launching one after the other
        SPWs="%s"
        arr=($SPWs)
        cd ${arr[SLURM_ARRAY_TASK_ID]}

        """ % SPWs.replace(',',' ').replace('0:','')

    command += "{mpi_wrapper} singularity {singularity_call} {container} {casa_call} {script} {args}".format(**params)

    #Get rid of annoying msmd output from casacore call
    if casacore:
        command += " 2>&1 | grep -v 'msmetadata_cmpt.cc::open\|MSMetaData::_computeScanAndSubScanProperties'"
    if arrayJob:
        command += '\ncd ..\n'

    return command


def write_sbatch(script,args,nodes=8,tasks=4,mem=MEM_PER_NODE_GB_LIMIT,name="job",runname='',plane=1,exclude='',mpi_wrapper=MPI_WRAPPER,
                container=CONTAINER,partition="Main",time="12:00:00",casa_script=True,casacore=False,SPWs='',account='b03-idia-ag',reservation=''):

    """Write a SLURM sbatch file calling a certain script (and args) with a particular configuration.

    Arguments:
    ----------
    script : str
        Path to script called within sbatch file (assumed to exist or be in PATH or calibration directory).
    args : str
        Arguments passed into script called within this sbatch file. Use '' for no arguments.
    time : str, optional
        Time limit on this job.
    nodes : int, optional
        Number of nodes to use for this job.
    tasks : int, optional
        The number of tasks per node to use for this job.
    mem : int, optional
        The memory in GB (per node) to use for this job.
    name : str, optional
        Name for this job, used in naming the various output files.
    runname : str, optional
        Unique name to give this pipeline run, appended to the start of all job names.
    plane : int, optional
        Distrubute tasks for this job using this block size before moving onto next node.
    exclude : str, optional
        SLURM worker nodes to exclude.
    mpi_wrapper : str, optional
        MPI wrapper for this job. e.g. 'srun', 'mpirun', 'mpicasa' (may need to specify path).
    container : str, optional
        Path to singularity container used for this job.
    partition : str, optional
        SLURM partition to use (default: "Main").
    time : str, optional
        Time limit to use for this job, in the form d-hh:mm:ss.
    casa_script : bool, optional
        Is the script that is called within this job a CASA script?
    casacore : bool, optional
        Is the script that is called within this job a casacore script?
    SPWs : str, optional
        Comma-separated list of spw ranges.
    account : str, optional
        SLURM accounting group for sbatch jobs.
    reservation : str, optional
        SLURM reservation to use."""

    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    #Store parameters passed into this function as dictionary, and add to it
    params = locals()
    params['LOG_DIR'] = LOG_DIR

    #Use multiple CPUs for tclean and paratition scripts
    params['cpus'] = 1
    if 'partition' in script and tasks*4 <= CPUS_PER_NODE_LIMIT:
        params['cpus'] = 4 #hard-code for 2/4 polarisations (TODO: check MS actually has 4, requiring CASA)
    if 'tclean' in script or 'selfcal' in script or 'bdsf' in script:
        params['cpus'] = int(CPUS_PER_NODE_LIMIT/tasks)

    #If requesting all CPUs, user may as well use all memory
    if params['cpus'] * tasks == CPUS_PER_NODE_LIMIT:
        params['mem'] = MEM_PER_NODE_GB_LIMIT

    #Use xvfb for plotting scripts, casacore for validate_input.py, and just python for run_bdsf.py
    plot = ('plot' in script)
    if script == 'validate_input.py':
        casa_script = False
        casacore = True
    elif script == 'run_bdsf.py':
        casa_script = False
        casacore = False

    params['command'] = write_command(script,args,name=name,mpi_wrapper=mpi_wrapper,container=container,casa_script=casa_script,plot=plot,SPWs=SPWs,casacore=casacore)
    if 'partition' in script and ',' in SPWs:
        params['ID'] = '%A_%a'
        params['array'] = '\n#SBATCH --array=0-{0}%2'.format(len(SPWs.split(','))-1)
    else:
        params['ID'] = '%j'
        params['array'] = ''
    params['exclude'] = '\n#SBATCH --exclude={0}'.format(exclude) if exclude != '' else ''
    params['reservation'] = '\n#SBATCH --reservation={0}'.format(reservation) if reservation != '' else ''

    contents = """#!/bin/bash{array}{exclude}{reservation}
    #SBATCH --account={account}
    #SBATCH --nodes={nodes}
    #SBATCH --ntasks-per-node={tasks}
    #SBATCH --cpus-per-task={cpus}
    #SBATCH --mem={mem}GB
    #SBATCH --job-name={runname}{name}
    #SBATCH --distribution=plane={plane}
    #SBATCH --output={LOG_DIR}/%x-{ID}.out
    #SBATCH --error={LOG_DIR}/%x-{ID}.err
    #SBATCH --partition={partition}
    #SBATCH --time={time}

    export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK

    {command}"""

    #insert arguments and remove whitespace
    contents = contents.format(**params).replace("    ","")

    #write sbatch file
    sbatch = '{0}.sbatch'.format(name)
    config = open(sbatch,'w')
    config.write(contents)
    config.close()

    logger.debug('Wrote sbatch file "{0}"'.format(sbatch))

def write_spw_master(filename,config,SPWs,precal_scripts,postcal_scripts,submit,dir='jobScripts',pad_length=5,dependencies='',timestamp=''):

    """Write master master script, which separately calls each of the master scripts in each SPW directory.

    filename : str
        Name of master pipeline submission script.
    config : str
        Path to config file.
    SPWs : str
        Comma-separated list of spw ranges.
    precal_scripts : list, optional
        List of sbatch scripts to call in order, before running pipeline in SPW directories.
    postcal_scripts : list, optional
        List of sbatch scripts to call in order, after running pipeline in SPW directories.
    submit : bool, optional
        Submit jobs to SLURM queue immediately?
    dir : str, optional
        Name of directory to output ancillary job scripts.
    pad_length : int, optional
        Length to pad the SLURM sacct output columns.
    dependencies : str, optional
        Comma-separated list of SLURM job dependencies.
    timestamp : str, optional
        Timestamp to put on this run and related runs in SPW directories."""

    master = open(filename,'w')
    master.write('#!/bin/bash\n')
    SPWs = SPWs.replace('0:','')
    toplevel = len(precal_scripts + postcal_scripts) > 0

    scripts = precal_scripts[:]
    if len(scripts) > 0:
        command = 'sbatch'
        if dependencies != '':
            master.write('\n#Run after these dependencies\nDep={0}\n'.format(dependencies))
            command += ' -d afterok:$Dep --kill-on-invalid-dep=yes'
            dependencies = '' #Remove dependencies so it isn't fed into launching SPW scripts
        master.write('\n#{0}\n'.format(scripts[0]))
        master.write("allSPWIDs=$({0} {1} | cut -d ' ' -f4)\n".format(command,scripts[0]))
        scripts.pop(0)
    for script in scripts:
        command = 'sbatch -d afterok:$allSPWIDs --kill-on-invalid-dep=yes'
        master.write('\n#{0}\n'.format(script))
        master.write("allSPWIDs+=,$({0} {1} | cut -d ' ' -f4)\n".format(command,script))

    if 'calc_refant.sbatch' in precal_scripts:
        master.write('echo Calculating reference antenna, and copying result to SPW directories.\n')
    if 'partition.sbatch' in precal_scripts:
        master.write('echo Running partition job array, iterating over {0} SPWs.\n'.format(len(SPWs.split(','))))

    partition = len(precal_scripts) > 0 and 'partition' in precal_scripts[-1]
    if partition:
        master.write('\npartitionID=$(echo $allSPWIDs | cut -d , -f{0})\n'.format(len(precal_scripts)))

    #Add time as extn to this pipeline run, to give unique filenames
    killScript = 'killJobs'
    summaryScript = 'summary'
    fullSummaryScript = 'fullSummary'
    errorScript = 'findErrors'
    timingScript = 'displayTimes'
    master.write('\n#Add time as extn to this pipeline run, to give unique filenames')
    master.write("\nDATE={0}\n".format(timestamp))
    master.write('mkdir -p {0}\n'.format(dir))
    master.write('mkdir -p {0}\n\n'.format(LOG_DIR))
    extn = '_$DATE.sh'

    for i,spw in enumerate(SPWs.split(',')):
        master.write('echo Running pipeline in directory "{0}" for spectral window 0:{0}\n'.format(spw))
        master.write('cd {0}\n'.format(spw))
        master.write('output=$({0} --config ./{1} --run --submit --quiet'.format(os.path.split(THIS_PROG)[1],config))
        if partition:
            master.write(' --dependencies=$partitionID\_{0}'.format(i))
        elif toplevel:
            master.write(' --dependencies=$allSPWIDs')
        elif dependencies != '':
            master.write(' --dependencies={0}'.format(dependencies))
        master.write(')\necho $output\n')
        if i == 0:
            master.write("IDs=$(echo $output | cut -d ' ' -f7)")
        else:
            master.write("IDs+=,$(echo $output | cut -d ' ' -f7)")
        master.write('\ncd ..\n\n')

    if 'concat.sbatch' in postcal_scripts:
        master.write('echo Will concatenate MSs/MMSs and create quick-look continuum cube across all SPWs for target and calibrator fields from \"{0}\".\n'.format(config))
    scripts = postcal_scripts[:]

    #Hack to perform correct number of selfcal loops
    if 'selfcal_part1.sbatch' in scripts and 'selfcal_part2.sbatch' in scripts and 'run_bdsf.sbatch' in scripts and 'make_pixmask.sbatch' in scripts:
        selfcal_loops = config_parser.parse_config(config)[0]['selfcal']['nloops']
        scripts.extend(['selfcal_part1.sbatch','selfcal_part2.sbatch','run_bdsf.sbatch','make_pixmask.sbatch']*(selfcal_loops))
        scripts.append('selfcal_part1.sbatch')

    if len(scripts) > 0:
        command = 'sbatch -d afterany:$IDs {0}'.format(scripts[0])
        master.write('\n#{0}\n'.format(scripts[0]))
        scripts.pop(0)
        if len(precal_scripts) == 0:
            master.write("allSPWIDs=$({0} | cut -d ' ' -f4)\n".format(command))
        else:
            master.write("allSPWIDs+=,$({0} | cut -d ' ' -f4)\n".format(command))
        for script in scripts:
            command = 'sbatch -d afterok:$allSPWIDs'
            master.write('\n#{0}\n'.format(script))
            master.write("allSPWIDs+=,$({0} {1} | cut -d ' ' -f4)\n".format(command,script))
    master.write('\necho Submitted the following jobIDs within the {0} SPW directories: $IDs\n'.format(len(SPWs.split(','))))

    prefix = ''
    #Write bash job scripts for the jobs run in this top level directory
    if toplevel:
        master.write('\necho Submitted the following jobIDs over all SPWs: $allSPWIDs\n')
        master.write('\necho For jobs over all SPWs:\n')
        prefix = 'allSPW_'
        write_all_bash_jobs_scripts(master,extn,IDs='allSPWIDs',dir=dir,prefix=prefix,pad_length=pad_length)
        master.write('\nln -f -s {1}{2}{3} {0}/{1}{4}{3}\n'.format(dir,prefix,summaryScript,extn,fullSummaryScript))

    master.write('\necho For all jobs within the {0} SPW directories:\n'.format(len(SPWs.split(','))))
    header = '-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------' + '-'*pad_length
    do = """echo "for f in {%s,}; do cd \$f; ./%s/%s%s; cd ..; done;""" % (SPWs,dir,killScript,extn)
    if not toplevel:
        do += ' \"'
    write_bash_job_script(master, killScript, extn, do, 'kill all the jobs', dir=dir,prefix=prefix)

    do = """echo "counter=1; for f in {%s,}; do echo -n SPW \#\$counter:; echo -n ' '; cd \$f; pwd; ./%s/%s%s %s; cd ..; counter=\$((counter+1)); echo '%s'; done; """
    if toplevel:
        do += "echo -n 'All SPWs: '; pwd; "
    else:
        do += ' \"'
    write_bash_job_script(master, summaryScript, extn, do % (SPWs,dir,summaryScript,extn,"| grep -v 'PENDING\|COMPLETED'",header), 'view the progress \(for running or failed jobs\)', dir=dir,prefix=prefix)
    write_bash_job_script(master, fullSummaryScript, extn, do % (SPWs,dir,summaryScript,extn,'',header), 'view the progress \(for all jobs\)', dir=dir,prefix=prefix)
    header = '------------------------------------------------------------------------------------------' + '-'*pad_length
    write_bash_job_script(master, errorScript, extn, do % (SPWs,dir,errorScript,extn,'',header), 'find errors \(after pipeline has run\)', dir=dir,prefix=prefix)
    write_bash_job_script(master, timingScript, extn, do % (SPWs,dir,timingScript,extn,'',header), 'display start and end timestamps \(after pipeline has run\)', dir=dir,prefix=prefix)

    #Close master submission script and make executable
    master.close()
    os.chmod(filename, 509)

    #Submit script or output that it will not run
    if submit:
        logger.info('Running master script "{0}"'.format(filename))
        os.system('./{0}'.format(filename))
    else:
        logger.info('Master script "{0}" written, but will not run.'.format(filename))

def write_master(filename,config,scripts=[],submit=False,dir='jobScripts',pad_length=5,verbose=False, echo=True, dependencies=''):

    """Write master pipeline submission script, calling various sbatch files, and writing ancillary job scripts.

    Arguments:
    ----------
    filename : str
        Name of master pipeline submission script.
    config : str
        Path to config file.
    scripts : list, optional
        List of sbatch scripts to call in order.
    submit : bool, optional
        Submit jobs to SLURM queue immediately?
    dir : str, optional
        Name of directory to output ancillary job scripts.
    pad_length : int, optional
        Length to pad the SLURM sacct output columns.
    verbose : bool, optional
        Verbose output (inserted into master script)?
    echo : bool, optional
        Echo the pupose of each job script for the user?
    dependencies : str, optional
        Comma-separated list of SLURM job dependencies."""

    master = open(filename,'w')
    master.write('#!/bin/bash\n')
    timestamp = config_parser.get_key(config,'run','timestamp')
    if timestamp == '':
        timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        config_parser.overwrite_config(config, conf_dict={'timestamp' : "'{0}'".format(timestamp)}, conf_sec='run')

    #Copy config file to TMP_CONFIG and inform user
    if verbose:
        master.write("\necho Copying \'{0}\' to \'{1}\', and using this to run pipeline.\n".format(config,TMP_CONFIG))
    master.write('cp {0} {1}\n'.format(config, TMP_CONFIG))

    #Hack to perform correct number of selfcal loops
    if 'selfcal_part1.sbatch' in scripts and 'selfcal_part2.sbatch' in scripts and 'run_bdsf.sbatch' in scripts and 'make_pixmask.sbatch' in scripts:
        selfcal_loops = config_parser.parse_config(config)[0]['selfcal']['nloops']
        scripts.extend(['selfcal_part1.sbatch','selfcal_part2.sbatch','run_bdsf.sbatch','make_pixmask.sbatch']*(selfcal_loops))
        scripts.append('selfcal_part1.sbatch')

    command = 'sbatch'

    if dependencies != '':
        master.write('\n#Run after these dependencies\nDep={0}\n'.format(dependencies))
        command += ' -d afterok:$Dep --kill-on-invalid-dep=yes'
    master.write('\n#{0}\n'.format(scripts[0]))
    if verbose:
        master.write('echo Submitting {0} SLURM queue with following command:\necho {1}\n'.format(scripts[0],command))
    master.write("IDs=$({0} {1} | cut -d ' ' -f4)\n".format(command,scripts[0]))
    scripts.pop(0)


    #Submit each script with dependency on all previous scripts, and extract job IDs
    for script in scripts:
        command = 'sbatch -d afterok:$IDs --kill-on-invalid-dep=yes'
        master.write('\n#{0}\n'.format(script))
        if verbose:
            master.write('echo Submitting {0} SLURM queue with following command\necho {1} {0}\n'.format(script,command))
        master.write("IDs+=,$({0} {1} | cut -d ' ' -f4)\n".format(command,script))

    master.write('\n#Output message and create {0} directory\n'.format(dir))
    master.write('echo Submitted sbatch jobs with following IDs: $IDs\n')
    master.write('mkdir -p {0}\n'.format(dir))

    #Add time as extn to this pipeline run, to give unique filenames
    master.write('\n#Add time as extn to this pipeline run, to give unique filenames')
    master.write("\nDATE={0}".format(timestamp))
    extn = '_$DATE.sh'

    #Copy contents of config file to jobScripts directory
    master.write('\n#Copy contents of config file to {0} directory\n'.format(dir))
    master.write('cp {0} {1}/{2}_$DATE.txt\n'.format(config,dir,os.path.splitext(config)[0]))

    #Write each job script - kill script, summary script, error script, and timing script
    write_all_bash_jobs_scripts(master,extn,IDs='IDs',dir=dir,echo=echo,pad_length=pad_length)

    #Close master submission script and make executable
    master.close()
    os.chmod(filename, 509)

    #Submit script or output that it will not run
    if submit:
        if echo:
            logger.info('Running master script "{0}"'.format(filename))
        os.system('./{0}'.format(filename))
    else:
        logger.info('Master script "{0}" written, but will not run.'.format(filename))

def write_all_bash_jobs_scripts(master,extn,IDs,dir='jobScripts',echo=True,prefix='',pad_length=5):

    """Write all the bash job scripts for a given set of job IDs.

    Arguments:
    ----------
    master : class ``file``
        Master script to which to write contents.
    extn : str
        Extension to append to this job script (e.g. date & time).
    IDs : str
        Comma-separated list of job IDs
    dir : str, optional
        Directory to write this script into.
    echo : bool, optional
        Echo what this job script does for the user?
    prefix : str, optional
        Additional prefix to place on the beginning of these script names.
    pad_length : int, optional
        Length to pad the SLURM sacct output columns."""

    #Add time as extn to this pipeline run, to give unique filenames
    killScript = prefix + 'killJobs'
    summaryScript = prefix + 'summary'
    errorScript = prefix + 'findErrors'
    timingScript = prefix + 'displayTimes'

    #Write each job script - kill script, summary script, and error script
    write_bash_job_script(master, killScript, extn, 'echo scancel ${0}'.format(IDs), 'kill all the jobs', dir=dir, echo=echo)
    do = """echo sacct -j ${0} --units=G -o "JobID%-15,JobName%-{1},Partition,Elapsed,NNodes%6,NTasks%6,NCPUS%5,MaxDiskRead,MaxDiskWrite,NodeList%20,TotalCPU,CPUTime,MaxRSS,State,ExitCode" """.format(IDs,15+pad_length)
    write_bash_job_script(master, summaryScript, extn, do, 'view the progress', dir=dir, echo=echo)
    do = """echo "for ID in {$%s,}; do ls %s/*\$ID*; cat %s/*\$ID* | grep -i 'severe\|error' | grep -vi 'mpi\|The selected table has zero rows'; done" """ % (IDs,LOG_DIR,LOG_DIR)
    write_bash_job_script(master, errorScript, extn, do, 'find errors \(after pipeline has run\)', dir=dir, echo=echo)
    do = """echo "for ID in {$%s,}; do ls %s/*\$ID*; cat %s/*\$ID* | grep INFO | head -n 1 | cut -d 'I' -f1; cat %s/*\$ID* | grep INFO | tail -n 1 | cut -d 'I' -f1; done" """ % (IDs,LOG_DIR,LOG_DIR,LOG_DIR)
    write_bash_job_script(master, timingScript, extn, do, 'display start and end timestamps \(after pipeline has run\)', dir=dir, echo=echo)

def write_bash_job_script(master,filename,extn,do,purpose,dir='jobScripts',echo=True,prefix=''):

    """Write bash job script (e.g. jobs summary, kill all jobs, etc).

    Arguments:
    ----------
    master : class ``file``
        Master script to which to write contents.
    filename : str
        Filename of this job script.
    extn : str
        Extension to append to this job script (e.g. date & time).
    do : str
        Bash command to run in this job script.
    purpose : str
        Purpose of this script to append as comment.
    dir : str, optional
        Directory to write this script into.
    echo : bool, optional
        Echo what this job script does for the user?
    prefix : str, optional
        Additional prefix to place on the beginning of the script, called from the top level directory (instead of SPW directories)."""

    fname = '{0}/{1}{2}'.format(dir,filename,extn)
    do2 = './{0}/{1}{2}{3} \"'.format(dir,prefix,filename,extn) if prefix != '' else ' '
    master.write('\n#Create {0}.sh file, make executable and symlink to current version\n'.format(filename))
    master.write('echo "#!/bin/bash" > {0}\n'.format(fname))
    master.write('{0}{1}>> {2}\n'.format(do,do2,fname))
    master.write('chmod u+x {0}\n'.format(fname))
    master.write('ln -f -s {0} {1}.sh\n'.format(fname,filename))
    if echo:
        master.write('echo Run ./{0}.sh to {1}.\n'.format(filename,purpose))

def write_jobs(config, scripts=[], threadsafe=[], containers=[], num_precal_scripts=0, mpi_wrapper=MPI_WRAPPER, nodes=8, ntasks_per_node=4, mem=MEM_PER_NODE_GB_LIMIT,plane=1,
               partition='Main', time='12:00:00', submit=False, name='', verbose=False, quiet=False, dependencies='', exclude='', account='b03-idia-ag', reservation='', timestamp=''):

    """Write a series of sbatch job files to calibrate a CASA measurement set.

    Arguments:
    ----------
    config : str
        Path to config file.
    scripts : list (of paths), optional
        List of paths to scripts (assumed to be python -- i.e. extension .py) to call within seperate sbatch jobs.
    threadsafe : list (of bools), optional
        Are these scripts threadsafe (for MPI)? List assumed to be same length as scripts.
    containers : list (of paths), optional
        List of paths to singularity containers to use for each script. List assumed to be same length as scripts.
    num_precal_scripts : int, optional
        Number of precal scripts.
    mpi_wrapper : str, optional
        Path to MPI wrapper to use for threadsafe tasks (otherwise srun used).
    nodes : int, optional
        Number of nodes to use for this job.
    tasks : int, optional
        The number of tasks per node to use for this job.
    mem : int, optional
        The memory in GB (per node) to use for this job.
    plane : int, optional
        Distrubute tasks for this job using this block size before moving onto next node.
    partition : str, optional
        SLURM partition to use (default: "Main").
    time : str, optional
        Time limit to use for all jobs, in the form d-hh:mm:ss.
    submit : bool, optional
        Submit jobs to SLURM queue immediately?
    name : str, optional
        Unique name to give this pipeline run, appended to the start of all job names.
    verbose : bool, optional
        Verbose output?
    quiet : bool, optional
        Activate quiet mode, with suppressed output?
    dependencies : str, optional
        Comma-separated list of SLURM job dependencies.
    exclude : str, optional
        SLURM worker nodes to exclude.
    account : str, optional
        SLURM accounting group for sbatch jobs.
    reservation : str, optional
        SLURM reservation to use.
    timestamp : str, optional
        Timestamp to put on this run and related runs in SPW directories."""

    crosscal_kwargs = get_config_kwargs(config, 'crosscal', CROSSCAL_CONFIG_KEYS)
    pad_length = len(name)

    #Write sbatch file for each input python script
    for i,script in enumerate(scripts):
        jobname = os.path.splitext(os.path.split(script)[1])[0]

        #Use input SLURM configuration for threadsafe tasks, otherwise call srun with single node and single thread
        if threadsafe[i]:
            write_sbatch(script,'--config {0}'.format(TMP_CONFIG),nodes=nodes,tasks=ntasks_per_node,mem=mem,plane=plane,exclude=exclude,mpi_wrapper=mpi_wrapper,
                        container=containers[i],partition=partition,time=time,name=jobname,runname=name,SPWs=crosscal_kwargs['spw'],account=account,reservation=reservation)
        else:
            write_sbatch(script,'--config {0}'.format(TMP_CONFIG),nodes=1,tasks=1,mem=mem,plane=1,mpi_wrapper='srun',container=containers[i],
                        partition=partition,time=time,name=jobname,runname=name,SPWs=crosscal_kwargs['spw'],exclude=exclude,account=account,reservation=reservation)

    #Replace all .py with .sbatch
    scripts = [os.path.split(scripts[i])[1].replace('.py','.sbatch') for i in range(len(scripts))]
    precal_scripts = scripts[:num_precal_scripts]
    postcal_scripts = scripts[num_precal_scripts:]
    echo = False if quiet else True

    if crosscal_kwargs['nspw'] > 1:
        #Build master master script, calling each of the separate SPWs at once, precal scripts before this, and postcal scripts after this
        write_spw_master(MASTER_SCRIPT,config,SPWs=crosscal_kwargs['spw'],precal_scripts=precal_scripts,postcal_scripts=postcal_scripts,submit=submit,pad_length=pad_length,dependencies=dependencies,timestamp=timestamp)
    else:
        #Build master pipeline submission script
        write_master(MASTER_SCRIPT,config,scripts=scripts,submit=submit,pad_length=pad_length,verbose=verbose,echo=echo,dependencies=dependencies)


def default_config(arg_dict):

    """Generate default config file in current directory, pointing to MS, with fields and SLURM parameters set.

    Arguments:
    ----------
    arg_dict : dict
        Dictionary of arguments passed into this script, which is inserted into the config file under various sections."""

    filename = arg_dict['config']
    MS = arg_dict['MS']

    #Copy default config to current location
    copyfile('{0}/{1}'.format(SCRIPT_DIR,CONFIG),filename)

    #Add SLURM arguments to config file under section [slurm]
    slurm_dict = get_slurm_dict(arg_dict,SLURM_CONFIG_KEYS)
    for key in SLURM_CONFIG_STR_KEYS:
        if key in slurm_dict.keys(): slurm_dict[key] = "'{0}'".format(slurm_dict[key])

    #Overwrite parameters in config under section [slurm]
    config_parser.overwrite_config(filename, conf_dict=slurm_dict, conf_sec='slurm')

    #Add MS to config file under section [data] and dopol under section [run]
    config_parser.overwrite_config(filename, conf_dict={'vis' : "'{0}'".format(MS)}, conf_sec='data')
    config_parser.overwrite_config(filename, conf_dict={'dopol' : arg_dict['dopol']}, conf_sec='run')

    if not arg_dict['do2GC']:
        config_parser.remove_section(filename, 'selfcal')
        scripts = arg_dict['postcal_scripts']
        i = 0
        while i < len(scripts):
            if 'selfcal' in scripts[i][0] or 'bdsf' in scripts[i][0] or scripts[i][0] == 'make_pixmask.py':
                scripts.pop(i)
                i -= 1
            i += 1

        config_parser.overwrite_config(filename, conf_dict={'postcal_scripts' : scripts}, conf_sec='slurm')

    if not arg_dict['nofields']:
        #Don't call srun if option --local used
        if arg_dict['local']:
            mpi_wrapper = ''
        else:
            mpi_wrapper = 'srun --qos qos-interactive --nodes=1 --ntasks=1 --time=10 --mem=4GB --partition={0}'.format(arg_dict['partition'])
            if arg_dict['exclude'] != '':
                mpi_wrapper += ' --exclude={0}'.format(arg_dict['exclude'])
            if arg_dict['reservation'] != '':
                mpi_wrapper += ' --reservation={0}'.format(arg_dict['reservation'])

        #Write and submit srun command to extract fields, and insert them into config file under section [fields]
        params =  '-B -M {MS} -C {config} -N {nodes} -t {ntasks_per_node}'.format(**arg_dict)
        command = write_command('get_fields.py', params, mpi_wrapper=mpi_wrapper, container=arg_dict['container'],logfile=False,casa_script=False,casacore=True)
        logger.info('Extracting field IDs from measurement set "{0}" using CASA.'.format(MS))
        logger.debug('Using the following command:\n\t{0}'.format(command))
        os.system(command)
    else:
        #Skip extraction of field IDs and assume we're not processing multiple SPWs
        logger.info('Skipping extraction of field IDs and assuming nspw=1.')
        config_parser.overwrite_config(filename, conf_dict={'nspw' : 1}, conf_sec='crosscal')

    logger.info('Config "{0}" generated.'.format(filename))

def get_slurm_dict(arg_dict,slurm_config_keys):

    """Build a slurm dictionary to be inserted into config file, using specified keys.

    Arguments:
    ----------
    arg_dict : dict
        Dictionary of arguments passed into this script, which is inserted into the config file under section [slurm].
    slurm_config_keys : list
        List of keys from arg_dict to insert into config file.

    Returns:
    --------
    slurm_dict : dict
        Dictionary to insert into config file under section [slurm]."""

    slurm_dict = {key:arg_dict[key] for key in slurm_config_keys}
    return slurm_dict

def pop_script(kwargs,script):

    """Pop script from list of scripts, list of threadsafe tasks, and list of containers.

    Arguments:
    ----------
    kwargs :  : dict
        Keyword arguments extracted from [slurm] section of config file, to be passed into write_jobs() function.
    script : str
        Name of script.

    Returns:
    --------
    popped : bool
        Was the script popped?"""

    popped = False
    if script in kwargs['scripts']:
        index = kwargs['scripts'].index(script)
        kwargs['scripts'].pop(index)
        kwargs['threadsafe'].pop(index)
        kwargs['containers'].pop(index)
        popped = True
    return popped

def format_args(config,submit,quiet,dependencies):

    """Format (and validate) arguments from config file, to be passed into write_jobs() function.

    Arguments:
    ----------
    config : str
        Path to config file.
    submit : bool
        Allow user to force submitting to queue immediately.
    quiet : bool
        Activate quiet mode, with suppressed output?
    dependencies : str
        Comma-separated list of SLURM job dependencies.
    selfcal : bool
        Is selfcal being performed?

    Returns:
    --------
    kwargs : dict
        Keyword arguments extracted from [slurm] section of config file, to be passed into write_jobs() function."""

    #Ensure all keys exist in these sections
    kwargs = get_config_kwargs(config,'slurm',SLURM_CONFIG_KEYS)
    data_kwargs = get_config_kwargs(config,'data',['vis'])
    get_config_kwargs(config, 'fields', FIELDS_CONFIG_KEYS)
    crosscal_kwargs = get_config_kwargs(config, 'crosscal', CROSSCAL_CONFIG_KEYS)

    #Check selfcal params
    if config_parser.has_section(config,'selfcal'):
        selfcal_kwargs = get_config_kwargs(config, 'selfcal', SELFCAL_CONFIG_KEYS)
        bookkeeping.get_selfcal_params()

    #Force submit=True if user has requested it during [-R --run]
    if submit:
        kwargs['submit'] = True

    #Ensure nspw is integer
    if type(crosscal_kwargs['nspw']) is not int:
        logger.warn("Argument 'nspw'={0} in '{1}' is not an integer. Will set to integer ({2}).".format(crosscal_kwargs['nspw']),config,int(crosscal_kwargs['nspw']))
        crosscal_kwargs['nspw'] = int(crosscal_kwargs['nspw'])

    spw = crosscal_kwargs['spw']
    nspw = crosscal_kwargs['nspw']
    mem = int(kwargs['mem'])

    if nspw > 1 and len(kwargs['scripts']) == 0:
        logger.warn('Setting nspw=1, since no "scripts" parameter in "{0}" is empty, so there\'s nothing run inside SPW directories.'.format(config))
        config_parser.overwrite_config(config, conf_dict={'nspw' : 1}, conf_sec='crosscal')
        nspw = 1

    #If nspw = 1 and precal or postcal scripts present, overwrite config and reload
    if nspw == 1:
        if len(kwargs['precal_scripts']) > 0 or len(kwargs['postcal_scripts']) > 0:
            logger.warn('Appending "precal_scripts" to beginning of "scripts", and "postcal_script" to end of "scripts", since nspw=1. Overwritting this in "{0}".'.format(config))
            scripts = kwargs['precal_scripts'] + kwargs['scripts'] + kwargs['postcal_scripts']
            config_parser.overwrite_config(config, conf_dict={'scripts' : scripts}, conf_sec='slurm')
            config_parser.overwrite_config(config, conf_dict={'precal_scripts' : []}, conf_sec='slurm')
            config_parser.overwrite_config(config, conf_dict={'postcal_scripts' : []}, conf_sec='slurm')
            kwargs = get_config_kwargs(config,'slurm',SLURM_CONFIG_KEYS)
        else:
            scripts = kwargs['scripts']
    else:
        scripts = kwargs['precal_scripts'] + kwargs['postcal_scripts']

    kwargs['num_precal_scripts'] = len(kwargs['precal_scripts'])

    # Validate kwargs along with MS
    kwargs['MS'] = data_kwargs['vis']
    validate_args(kwargs,config)

    #Reformat scripts tuple/list, to extract scripts, threadsafe, and containers as parallel lists
    #Check that path to each script and container exists or is ''
    kwargs['scripts'] = [check_path(i[0]) for i in scripts]
    kwargs['threadsafe'] = [i[1] for i in scripts]
    kwargs['containers'] = [check_path(i[2]) for i in scripts]

    #Only reduce the memory footprint if we're not using all CPUs on each node
    if kwargs['ntasks_per_node'] < NTASKS_PER_NODE_LIMIT:
        mem = mem // nspw

    includes_partition = any('partition' in script for script in kwargs['scripts'])
    #If single correctly formatted spw, split into nspw directories, and process each spw independently
    if nspw > 1:
        #Write timestamp to this pipeline run
        kwargs['timestamp'] = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
        config_parser.overwrite_config(config, conf_dict={'timestamp' : "'{0}'".format(kwargs['timestamp'])}, conf_sec='run')
        nspw = spw_split(spw, nspw, config, mem, crosscal_kwargs['badfreqranges'],kwargs['MS'],includes_partition)
        config_parser.overwrite_config(config, conf_dict={'nspw' : "{0}".format(nspw)}, conf_sec='crosscal')

    #Set threadsafe=False for split if keepmms=False. Assume it won't be in precal or postcal scripts
    if 'split.py' in kwargs['scripts'] and not crosscal_kwargs['keepmms']:
        kwargs['threadsafe'][kwargs['scripts'].index('split.py')] = False

    #Pop script to calculate reference antenna if calcrefant=False. Assume it won't be in postcal scripts
    if not crosscal_kwargs['calcrefant']:
        if pop_script(kwargs,'calc_refant.py'):
            kwargs['num_precal_scripts'] -= 1

    #Replace empty containers with default container and remove unwanted kwargs
    for i in range(len(kwargs['containers'])):
        if kwargs['containers'][i] == '':
            kwargs['containers'][i] = kwargs['container']
    kwargs.pop('container')
    kwargs.pop('MS')
    kwargs.pop('precal_scripts')
    kwargs.pop('postcal_scripts')
    kwargs['quiet'] = quiet

    #Force overwrite of dependencies
    if dependencies != '':
        kwargs['dependencies'] = dependencies

    #If everything up until here has passed, we can copy config file to TMP_CONFIG (in case user runs sbatch manually) and inform user
    logger.debug("Copying '{0}' to '{1}', and using this to run pipeline.".format(config,TMP_CONFIG))
    copyfile(config, TMP_CONFIG)
    if not quiet:
        logger.warn("Changing [slurm] section in your config will have no effect unless you [-R --run] again.")

    if len(kwargs['scripts']) == 0:
        logger.error('Nothing to do. Please insert scripts into "scripts" parameter in "{0}".'.format(config))
        sys.exit(1)

    return kwargs

def linspace(lower,upper,length):

    """Basically np.linspace, but without needing to import numpy..."""

    return [lower + x*(upper-lower)/float(length-1) for x in range(length)]

def get_spw_bounds(spw):

    """Get upper and lower bounds of spw.

    Arguments:
    ----------
    spw : str
        CASA spectral window in MHz.

    Returns:
    --------
    low : float
        Lower bound of spw.
    high : float
        Higher bound of spw.
    unit : str
        Unit of spw.
    func : function
        Function to apply to spectral window (i.e. int for SPW channel range, otherwise float)."""

    bounds = spw.split(':')[-1].split('~')
    if ',' not in spw and ':' in spw and '~' in spw and len(bounds) == 2 and bounds[1] != '':
        high,unit=re.search(r'(\d+\.*\d*)(\w*)',bounds[1]).groups()
        func = int if unit == '' else float
        low = func(bounds[0])
        high = func(high)

        if unit != 'MHz':
            logger.warn('Please use SPW unit "MHz", to ensure the best performance (e.g. not processing entirely flagged frequency ranges).')
        # Can only do when using CASA
        # if unit == '':
        #     msmd.open(MS)
        #     low_MHz = msmd.chanfreqs(0)[low] / 1e9
        #     high_MHz = msmd.chanfreqs(0)[high] / 1e9
        #     msmd.done()
        # else:
        #     low_MHz=qa.convertfreq('{0}{1}'.format(low,unit),'MHz')['value']
        #     high_MHz=qa.convertfreq('{0}{1}'.format(high,unit),'MHz')['value']
    else:
        return None

    return low,high,unit,func

def spw_split(spw,nspw,config,mem,badfreqranges,MS,partition):

    """Split into N SPWs, placing an instance of the pipeline into N directories, each with 1 Nth of the bandwidth.

    Arguments:
    ----------
    spw : str
        spw parameter from config.
    nspw : int
        Number of spectral windows to split into.
    config : str
        Path to config file.
    mem : int
        Memory in GB to use per instance.
    badfreqranges : list
        List of bad frequency ranges in MHz.
    MS : str
        Path to CASA Measurement Set.
    partition : bool
        Does this run include the partition step?

    Returns:
    --------
    nspw : int
        New nspw, potentially a lower value than input (if any SPWs completely encompassed by badfreqranges)."""

    if get_spw_bounds(spw) != None:
        #Write nspw frequency ranges
        low,high,unit,func = get_spw_bounds(spw)
        interval=func((high-low)/float(nspw))
        lo=linspace(low,high-interval,nspw)
        hi=linspace(low+interval,high,nspw)
        SPWs=[]

        #Remove SPWs entirely encompassed by bad frequency ranges (only for MHz unit)
        for i in range(len(lo)):
            SPWs.append('0:{0}~{1}{2}'.format(func(lo[i]),func(hi[i]),unit))

    elif ',' in spw:
        SPWs = spw.split(',')
        unit = get_spw_bounds(SPWs[0])[2]
        if len(SPWs) != nspw:
            logger.error("nspw ({0}) not equal to number of separate SPWs ({1} in '{2}') from '{3}'. Setting to nspw={1}.".format(nspw,len(SPWs),spw,config))
            nspw = len(SPWs)
    else:
        logger.error("Can't split into {0} SPWs using SPW format '{1}'. Using nspw=1 in '{2}'.".format(nspw,spw,config))
        return 1

    #Remove any SPWs completely encompassed by bad frequency ranges
    i=0
    while i < nspw:
        badfreq = False
        low,high = get_spw_bounds(SPWs[i])[0:2]
        if unit == 'MHz':
            for freq in badfreqranges:
                bad_low,bad_high = get_spw_bounds('0:{0}'.format(freq))[0:2]
                if low > bad_low and high < bad_high:
                    logger.info("Won't process spw '0:{0}~{1}{2}', since it's completely encompassed by bad frequency range '{3}'.".format(low,high,unit,freq))
                    badfreq = True
                    break
        if badfreq:
            SPWs.pop(i)
            i -= 1
            nspw -= 1
        i += 1

    #Overwrite config with new SPWs
    config_parser.overwrite_config(config, conf_dict={'spw' : "'{0}'".format(','.join(SPWs))}, conf_sec='crosscal')

    #Create each spw as directory and place config in there
    logger.info("Making {0} directories for SPWs ({1}) and copying '{2}' to each of them.".format(nspw,SPWs,config))
    for spw in SPWs:
        spw_config = '{0}/{1}'.format(spw.replace('0:',''),config)
        if not os.path.exists(spw.replace('0:','')):
            os.mkdir(spw.replace('0:',''))
        copyfile(config, spw_config)
        config_parser.overwrite_config(spw_config, conf_dict={'spw' : "'{0}'".format(spw)}, conf_sec='crosscal')
        config_parser.overwrite_config(spw_config, conf_dict={'nspw' : 1}, conf_sec='crosscal')
        config_parser.overwrite_config(spw_config, conf_dict={'mem' : mem}, conf_sec='slurm')
        config_parser.overwrite_config(spw_config, conf_dict={'calcrefant' : False}, conf_sec='crosscal')
        config_parser.overwrite_config(spw_config, conf_dict={'precal_scripts' : []}, conf_sec='slurm')
        config_parser.overwrite_config(spw_config, conf_dict={'postcal_scripts' : []}, conf_sec='slurm')
        #Look 1 directory up when using relative path
        if MS[0] != '/':
            config_parser.overwrite_config(spw_config, conf_dict={'vis' : "'../{0}'".format(MS)}, conf_sec='data')
        elif not partition:
            basename, ext = os.path.splitext(MS.rstrip('/ '))
            filebase = os.path.split(basename)[1]
            vis = '{0}.{1}.mms'.format(filebase,spw.replace('0:',''))
            logger.warn("Since script with 'partition' in its name isn't present in '{0}', assuming partition has already been done, and setting vis='{1}' in '{2}'. If '{1}' doesn't exist, please update '{2}', as the pipeline will not launch successfully.".format(config,vis,spw_config))
            orig_vis = config_parser.get_key(spw_config, 'data', 'vis')
            config_parser.overwrite_config(spw_config, conf_dict={'orig_vis' : "'{0}'".format(orig_vis)}, conf_sec='data')
            config_parser.overwrite_config(spw_config, conf_dict={'vis' : "'{0}'".format(vis)}, conf_sec='data')

    return nspw

def get_config_kwargs(config,section,expected_keys):

    """Return kwargs from config section. Check section exists, and that all expected keys are present, otherwise raise KeyError.

    Arguments:
    ----------
    config : str
        Path to config file.
    section : str
        Config section from which to extract kwargs.
    expected_keys : list
        List of expected keys.

    Returns:
    --------
    kwargs : dict
        Keyword arguments from this config section."""

    config_dict = config_parser.parse_config(config)[0]

    #Ensure section exists, otherwise raise KeyError
    if section not in config_dict.keys():
        raise KeyError("Config file '{0}' has no section [{1}]. Please insert section or build new config with [-B --build].".format(config,section))

    kwargs = config_dict[section]

    #Check for any unknown keys and display warning
    unknown_keys = list(set(kwargs) - set(expected_keys))
    if len(unknown_keys) > 0:
        logger.warn("Unknown keys {0} present in section [{1}] in '{2}'.".format(unknown_keys,section,config))

    #Check that expected keys are present, otherwise raise KeyError
    missing_keys = list(set(expected_keys) - set(kwargs))
    if len(missing_keys) > 0:
        raise KeyError("Keys {0} missing from section [{1}] in '{2}'. Please add these keywords to '{2}', or else run [-B --build] step again.".format(missing_keys,section,config))

    return kwargs

def setup_logger(config,verbose=False):

    """Setup logger at debug or info level according to whether verbose option selected (via command line or config file).

    Arguments:
    ----------
    config : str
        Path to config file.
    verbose : bool
        Verbose output? This will display all logger debug output."""

    #Overwrite with verbose mode if set to True in config file
    if not verbose:
        config_dict = config_parser.parse_config(config)[0]
        if 'slurm' in config_dict.keys() and 'verbose' in config_dict['slurm']:
            verbose = config_dict['slurm']['verbose']

    loglevel = logging.DEBUG if verbose else logging.INFO
    logger.setLevel(loglevel)

def main():

    #Parse command-line arguments, and setup logger
    args = parse_args()
    setup_logger(args.config,args.verbose)

    #Mutually exclusive arguments - display version, build config file or run pipeline
    if args.version:
        logger.info('This is version {0}'.format(__version__))
    if args.license:
        logger.info(license)
    if args.build:
        default_config(vars(args))
    if args.run:
        kwargs = format_args(args.config,args.submit,args.quiet,args.dependencies)
        write_jobs(args.config, **kwargs)

if __name__ == "__main__":
    main()
