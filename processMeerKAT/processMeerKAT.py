#!/usr/bin/env python2.7

__version__ = '1.0'

import argparse
import os
import sys
import config_parser
from shutil import copyfile
import logging
logger = logging.getLogger(__name__)

#Set global limits for current ilifu cluster configuration
TOTAL_NODES_LIMIT = 35
NTASKS_PER_NODE_LIMIT = 128
MEM_PER_NODE_GB_LIMIT = 236 #241827 MB

#Set global values for paths and file names
THIS_PROG = sys.argv[0]
SCRIPT_DIR = os.path.dirname(THIS_PROG)
LOG_DIR = 'logs'
CALIB_SCRIPTS_DIR = 'cal_scripts'
CONFIG = 'default_config.txt'
TMP_CONFIG = '.config.tmp'
MASTER_SCRIPT = 'submit_pipeline.sh'

#Set global values for field, crosscal and SLURM arguments copied to config file, and some of their default values
FIELDS_CONFIG_KEYS = ['fluxfield','bpassfield','phasecalfield','targetfields']
CROSSCAL_CONFIG_KEYS = ['minbaselines','specavg','timeavg','spw','calcrefant','refant','standard','badants','badfreqranges']
SLURM_CONFIG_KEYS = ['nodes','ntasks_per_node','mem','plane','submit','scripts','verbose','container','mpi_wrapper','partition']
CONTAINER = '/data/exp_soft/pipelines/casameer-5.4.1.xvfb.simg'
MPI_WRAPPER = '/data/exp_soft/pipelines/casa-prerelease-5.3.0-115.el7/bin/mpicasa'
SCRIPTS = [ ('validate_input.py',False,''),
            ('partition.py',True,''),
            ('calc_refant.py',False,''),
            ('flag_round_1.py',True,''),
            ('setjy.py',True,''),
            ('xx_yy_solve.py',False,''),
            ('xx_yy_apply.py',True,''),
            ('flag_round_2.py',True,''),
            ('setjy.py',True,''),
            ('xy_yx_solve.py',False,''),
            ('xy_yx_apply.py',True,''),
            ('split.py',True,''),
            ('plot_solutions.py',False,''),
            ('quick_tclean.py',True,'')]


def check_path(path):

    """Check in specific location for a script or container, including in bash path, and in this pipeline's calibration
    scripts directory (SCRIPT_DIR/CALIB_SCRIPTS_DIR/). If path isn't found, raise IOError, otherwise return the path.

    Arguments:
    ----------
    path : str
        Check for script or container at this path.

    Returns:
    --------
    path : str
        Path to script or container (if path found)."""

    #check if path is in bash path first
    if '/' not in path and path != '':
        path = check_bash_path(path)
    if path != '' and not os.path.exists(path) and not os.path.exists('{0}/{1}/{2}'.format(SCRIPT_DIR,CALIB_SCRIPTS_DIR,path)):
        raise IOError('File "{0}" not found.'.format(path))
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
    parser.add_argument("-C","--config",metavar="path", default=CONFIG, required=False, type=str, help="Path to config file.")
    parser.add_argument("-N","--nodes",metavar="num", required=False, type=int, default=8,
                        help="Use this number of nodes [default: 8; max: {0}].".format(TOTAL_NODES_LIMIT))
    parser.add_argument("-t","--ntasks-per-node", metavar="num", required=False, type=int, default=4,
                        help="Use this number of tasks (per node) [default: 4; max: {0}].".format(NTASKS_PER_NODE_LIMIT))
    parser.add_argument("-P","--plane", metavar="num", required=False, type=int, default=2,
                            help="Distribute tasks of this block size before moving onto next node [default: 2; max: ntasks-per-node].")
    parser.add_argument("-m","--mem", metavar="num", required=False, type=int, default=MEM_PER_NODE_GB_LIMIT,
                        help="Use this many GB of memory (per node) for threadsafe scripts [default: {0}; max: {0}.".format(MEM_PER_NODE_GB_LIMIT))
    parser.add_argument("-p","--partition", metavar="name", required=False, type=str, default="Main", help="SLURM partition to use [default: 'Main'].")
    parser.add_argument("-S","--scripts", action='append', nargs=3, metavar=('script','threadsafe','container'), required=False, type=parse_scripts, default=SCRIPTS,
                        help="Run pipeline with these scripts, in this order, using this container (3rd value - empty string to default to [-c --container]). Is it threadsafe (2nd value)?")
    parser.add_argument("-w","--mpi_wrapper", metavar="path", required=False, type=str, default=MPI_WRAPPER,
                        help="Use this mpi wrapper when calling threadsafe scripts [default: '{0}'].".format(MPI_WRAPPER))
    parser.add_argument("-c","--container", metavar="path", required=False, type=str, default=CONTAINER, help="Use this container when calling scripts [default: '{0}'].".format(CONTAINER))

    parser.add_argument("-l","--local", action="store_true", required=False, default=False, help="Build config file locally (i.e. without calling srun) [default: False].")
    parser.add_argument("-s","--submit", action="store_true", required=False, default=False, help="Submit jobs immediately to SLURM queue [default: False].")
    parser.add_argument("-v","--verbose", action="store_true", required=False, default=False, help="Verbose output? [default: False].")

    #add mutually exclusive group - don't want to build config, run pipeline, or display version at same time
    run_args = parser.add_mutually_exclusive_group(required=True)
    run_args.add_argument("-B","--build", action="store_true", required=False, default=False, help="Build config file using input MS.")
    run_args.add_argument("-R","--run", action="store_true", required=False, default=False, help="Run pipeline with input config file.")
    run_args.add_argument("-V","--version", action="store_true", required=False, default=False, help="Display the version of this pipeline and quit.")

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
        if args['MS'] is None:
            msg = "You must input an MS [-M --MS] to build the config file."
            raise_error(config, msg, parser)

        if not os.path.isdir(args['MS']):
            msg = "Input MS '{0}' not found.".format(args['MS'])
            raise_error(config, msg, parser)

    if args['ntasks_per_node'] > NTASKS_PER_NODE_LIMIT:
        msg = "The number of tasks per node [-t --ntasks-per-node] must not exceed {0}. You input {1}.".format(NTASKS_PER_NODE_LIMIT,args['ntasks_per_node'])
        raise_error(config, msg, parser)

    if args['nodes'] > TOTAL_NODES_LIMIT:
        msg = "The number of nodes [-N --nodes] per node must not exceed {0}. You input {1}.".format(TOTAL_NODES_LIMIT,args['nodes'])
        raise_error(config, msg, parser)

    if args['mem'] > MEM_PER_NODE_GB_LIMIT:
        msg = "The memory per node [-m --mem] must not exceed {0} (GB). You input {1} (GB).".format(MEM_PER_NODE_GB_LIMIT,args['mem'])
        raise_error(config, msg, parser)

    if args['plane'] > args['ntasks_per_node']:
        msg = "The value of [-P --plane] cannot be greater than the tasks per node [-t --ntasks-per-node] ({0}). You input {1}.".format(args['ntasks_per_node'],args['plane'])
        raise_error(config, msg, parser)

def write_command(script,args,name='job',mpi_wrapper=MPI_WRAPPER,container=CONTAINER,casa_script=True,logfile=True):

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
    logfile : bool, optional
        Write the CASA output to a log file? Only used if casa_script==True.

    Returns:
    --------
    command : str
        Bash command to call with srun or within sbatch file."""

    #Store parameters passed into this function as dictionary, and add to it
    params = locals()
    params['LOG_DIR'] = LOG_DIR
    params['job'] = '${SLURM_JOB_ID}'
    params['casa_call'] = ''
    params['casa_log'] = '--nologfile'

    #If script path doesn't exist and is not in user's bash path, assume it's in the calibration scripts directory
    if not os.path.exists(script):
        params['script'] = '{0}/{1}/{2}'.format(SCRIPT_DIR, CALIB_SCRIPTS_DIR, script)

    #If specified by user, call script via CASA and write output to log file
    if logfile:
        params['casa_log'] = '--logfile {LOG_DIR}/{name}-{job}.casa'.format(**params)
    if casa_script:
        params['casa_call'] = "xvfb-run -d casa --nologger --nogui {casa_log} -c".format(**params)

    command = "{mpi_wrapper} singularity exec {container} {casa_call} {script} {args}".format(**params)
    return command


def write_sbatch(script,args,time="00:10:00",nodes=15,tasks=16,mem=MEM_PER_NODE_GB_LIMIT,name="job",
                plane=1,mpi_wrapper=MPI_WRAPPER,container=CONTAINER,partition="Main",casa_script=True):

    """Write a SLURM sbatch file calling a certain script (and args) with a particular configuration.

    Arguments:
    ----------
    script : str
        Path to script called within sbatch file (assumed to exist or be in PATH or calibration directory).
    args : str
        Arguments passed into script called within this sbatch file. Use '' for no arguments.
    time : str, optional
        Time limit on this job (option not currently used).
    nodes : int, optional
        Number of nodes to use for this job.
    tasks : int, optional
        The number of tasks per node to use for this job.
    mem : int, optional
        The memory in GB (per node) to use for this job.
    name : str, optional
        Name for this job, used in naming the various output files.
    plane : int, optional
        Distrubute tasks for this job using this block size before moving onto next node.
    mpi_wrapper : str, optional
        MPI wrapper for this job. e.g. 'srun', 'mpirun', 'mpicasa' (may need to specify path).
    container : str, optional
        Path to singularity container used for this job.
    partition : str, optional
        SLURM partition to use (default: "Main").
    casa_script : bool, optional
        Is the script that is called within this job a CASA script?"""

    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    #Store parameters passed into this function as dictionary, and add to it
    params = locals()
    params['LOG_DIR'] = LOG_DIR
    params['job'] = '${SLURM_JOB_ID}'
    params['command'] = write_command(script,args,name=name,mpi_wrapper=mpi_wrapper,container=container,casa_script=casa_script)
    params['cpus'] = 4 if 'tclean' in script else 1

    #SBATCH --time={time}
    contents = """#!/bin/bash
    #SBATCH --nodes={nodes}
    #SBATCH --ntasks-per-node={tasks}
    #SBATCH --cpus-per-task={cpus}
    #SBATCH --mem={mem}GB
    #SBATCH --job-name={name}
    #SBATCH --distribution=plane={plane}
    #SBATCH --output={LOG_DIR}/{name}-%j.out
    #SBATCH --error={LOG_DIR}/{name}-%j.err
    #SBATCH --partition={partition}

    export OMP_NUM_THREADS={cpus}

    {command}"""

    #insert arguments and remove whitespace
    contents = contents.format(**params).replace("    ","")

    #write sbatch file
    sbatch = '{0}.sbatch'.format(name)
    config = open(sbatch,'w')
    config.write(contents)
    config.close()

    logger.debug('Wrote sbatch file "{0}"'.format(sbatch))

def write_master(filename,config,scripts=[],submit=False,dir='jobScripts',verbose=False):

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
    verbose : bool, optional
        Verbose output (inserted into master script)?"""

    master = open(filename,'w')
    master.write('#!/bin/bash\n')

    #Copy config file to TMP_CONFIG and inform user
    if verbose:
        master.write("\necho Copying \'{0}\' to \'{1}\', and using this to run pipeline.\n".format(config,TMP_CONFIG))
    master.write('cp {0} {1}\n'.format(config, TMP_CONFIG))

    #Submit first script with no dependencies and extract job ID
    command = 'sbatch {0}'.format(scripts[0])
    master.write('\n#{0}\n'.format(scripts[0]))
    if verbose:
        master.write('echo Submitting {0} SLURM queue with following command:\necho {1}\n'.format(scripts[0],command))
    master.write("IDs=$({0} | cut -d ' ' -f4)\n".format(command))
    scripts.pop(0)

    #Submit each script with dependency on all previous scripts, and extract job IDs
    for script in scripts:
        command = 'sbatch -d afterok:$IDs'
        master.write('\n#{0}\n'.format(script))
        if verbose:
            master.write('echo Submitting {0} SLURM queue with following command\necho {1} {0}\n'.format(script,command))
        master.write("IDs+=,$({0} {1} | cut -d ' ' -f4)\n".format(command,script))

    master.write('\n#Output message and create {0} directory\n'.format(dir))
    master.write('echo Submitted sbatch jobs with following IDs: $IDs\n')
    master.write('mkdir -p {0}\n'.format(dir))

    #Add time as extn to this pipeline run, to give unique filenames
    killScript = 'killJobs'
    summaryScript = 'summary'
    errorScript = 'findErrors'
    timingScript = 'displayTimes'
    master.write('\n#Add time as extn to this pipeline run, to give unique filenames')
    master.write("\nDATE=$(date '+%Y-%m-%d-%H-%M-%S')\n")
    extn = '_$DATE.sh'

    #Copy contents of config file to jobScripts directory
    master.write('\n#Copy contents of config file to {0} directory\n'.format(dir))
    master.write('cp {0} {1}/{2}_$DATE.txt\n'.format(config,dir,os.path.splitext(config)[0]))

    #Write each job script - kill script, summary script, and error script
    write_bash_job_script(master, killScript, extn, 'echo scancel $IDs', 'kill all the jobs', dir=dir)
    write_bash_job_script(master, summaryScript, extn, 'echo sacct -j $IDs', 'view the progress', dir=dir)
    do = """echo "for ID in {$IDs,}; do ls %s/*\$ID.out; cat %s/*\$ID.{out,err,casa} | grep 'SEVERE\|rror' | grep -v 'mpi\|MPI'; done" """ % (LOG_DIR,LOG_DIR)
    write_bash_job_script(master, errorScript, extn, do, 'find errors \(after pipeline has run\)', dir=dir)
    do = """echo "for ID in {$IDs,}; do ls %s/*\$ID.casa; cat %s/*\$ID.casa | grep INFO | head -n 1 | cut -d 'I' -f1; cat %s/*\$ID.casa | grep INFO | tail -n 1 | cut -d 'I' -f1; done" """ % (LOG_DIR,LOG_DIR,LOG_DIR)
    write_bash_job_script(master, timingScript, extn, do, 'display start and end timestamps \(after pipeline has run\)', dir=dir)

    #Close master submission script and make executable
    master.close()
    os.chmod(filename, 509)

    #Submit script or output that it will not run
    if submit:
        logger.info('Running master script "{0}"'.format(filename))
        os.system('./{0}'.format(filename))
    else:
        logger.info('Master script "{0}" written, but will not run.'.format(filename))

def write_bash_job_script(master,filename,extn,do,purpose,dir='jobScripts'):

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
        Directory to write this script into."""

    fname = '{0}/{1}{2}'.format(dir,filename,extn)
    master.write('\n#Create {0}.sh file, make executable and simlink to current version\n'.format(filename))
    master.write('echo "#!/bin/bash" > {0}\n'.format(fname))
    master.write('{0} >> {1}\n'.format(do,fname))
    master.write('chmod u+x {0}\n'.format(fname))
    master.write('ln -f -s {0} {1}.sh\n'.format(fname,filename))
    master.write('echo Run {0}.sh to {1}.\n'.format(filename,purpose))

def write_jobs(config, scripts=[], threadsafe=[], containers=[], mpi_wrapper=MPI_WRAPPER, nodes=15,
                ntasks_per_node=16, mem=MEM_PER_NODE_GB_LIMIT, plane=4, partition='Main', submit=False, verbose=False):

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
    mpi_wrapper : str, optional
        Path to MPI wrapper to use for threadsafe tasks (otherwise srun used).
    nodes : int, optional
        Number of nodes to use for this job.
    tasks : int, optional
        The number of tasks per node to use for this job.
    mem : int, optional
        The memory in GB (per node) to use for this job.
    name : str, optional
        Name for this job, used in naming the various output files.
    plane : int, optional
        Distrubute tasks for this job using this block size before moving onto next node.
    partition : str, optional
        SLURM partition to use (default: "Main").
    submit : bool, optional
        Submit jobs to SLURM queue immediately?
    verbose : bool, optional
        Verbose output?"""

    #Write sbatch file for each input python script
    for i,script in enumerate(scripts):
        name = os.path.splitext(os.path.split(script)[1])[0]

        #Use input SLURM configuration for threadsafe tasks, otherwise call srun with single node and single thread
        if threadsafe[i]:
            write_sbatch(script,'--config {0}'.format(TMP_CONFIG),time="01:00:00",nodes=nodes,tasks=ntasks_per_node,
                        mem=mem,plane=plane,mpi_wrapper=mpi_wrapper,container=containers[i],partition=partition,name=name)
        else:
            write_sbatch(script,'--config {0}'.format(TMP_CONFIG),time="01:00:00",nodes=1,tasks=1,mem=100,plane=1,
                        mpi_wrapper='srun',container=containers[i],partition=partition,name=name)

    #Build master pipeline submission script, replacing all .py with .sbatch
    scripts = [os.path.split(scripts[i])[1].replace('.py','.sbatch') for i in range(len(scripts))]
    write_master(MASTER_SCRIPT,config,scripts=scripts,submit=submit,verbose=verbose)


def default_config(arg_dict):

    """Generate default config file in current directory, pointing to MS, with fields and SLURM parameters set.

    Arguments:
    ----------
    arg_dict : dict
        Dictionary of arguments passed into this script, which is inserted into the config file under section [slurm]."""

    filename = arg_dict['config']
    MS = arg_dict['MS']

    #Copy default config to current location
    copyfile('{0}/{1}'.format(SCRIPT_DIR,CONFIG),filename)

    #Add SLURM arguments to config file under section [slurm]
    slurm_dict = get_slurm_dict(arg_dict,SLURM_CONFIG_KEYS)
    for key in ['container','mpi_wrapper','partition']:
        if key in slurm_dict.keys(): slurm_dict[key] = "'{0}'".format(slurm_dict[key])

    #Overwrite parameters in config under section [slurm]
    config_parser.overwrite_config(filename, conf_dict=slurm_dict, conf_sec='slurm')

    #Add MS to config file under section [data]
    config_parser.overwrite_config(filename, conf_dict={'vis' : "'{0}'".format(MS)}, conf_sec='data')

    #Don't call srun if option --local used
    mpi_wrapper = '' if arg_dict['local'] else 'srun --nodes=1 --ntasks=1 --time=5 --mem=4GB --partition={0}'.format(arg_dict['partition'])

    #Write and submit srun command to extract fields, and insert them into config file under section [fields]
    params =  '-B -M {MS} -C {config} -n {nodes} -t {ntasks_per_node}'.format(**arg_dict)
    command = write_command('get_fields.py', params, mpi_wrapper=mpi_wrapper, container=arg_dict['container'],logfile=False)
    logger.info('Extracting field IDs from measurement set "{0}" using CASA.'.format(MS))
    logger.debug('Using the following command:\n\t{0}'.format(command))
    os.system(command)

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

def format_args(config):

    """Format (and validate) arguments from config file, to be passed into write_jobs() function.

    Arguments:
    ----------
    config : str
        Path to config file.

    Returns:
    --------
    kwargs : dict
        Keyword arguments extracted from [slurm] section of config file, to be passed into write_jobs() function."""

    #Ensure all keys exist in these sections
    kwargs = get_config_kwargs(config,'slurm',SLURM_CONFIG_KEYS)
    data_kwargs = get_config_kwargs(config,'data',['vis'])
    get_config_kwargs(config, 'fields', FIELDS_CONFIG_KEYS)
    get_config_kwargs(config, 'crosscal', CROSSCAL_CONFIG_KEYS)

    # Validate kwargs along with MS
    kwargs['MS'] = data_kwargs['vis']
    validate_args(kwargs,config)

    #Reformat scripts tuple/list, to extract scripts, threadsafe, and containers as parallel lists
    #Check that path to each script and container exists or is ''
    scripts = kwargs['scripts']
    kwargs['scripts'] = [check_path(i[0]) for i in scripts]
    kwargs['threadsafe'] = [i[1] for i in scripts]
    kwargs['containers'] = [check_path(i[2]) for i in scripts]

    #Replace empty containers with default container and remove unwanted kwargs
    for i in range(len(kwargs['containers'])):
        if kwargs['containers'][i] == '':
            kwargs['containers'][i] = kwargs['container']
    kwargs.pop('container')
    kwargs.pop('MS')

    #If everything up until here has passed, we can copy config file to TMP_CONFIG (in case user runs sbatch manually) and inform user
    logger.debug("Copying '{0}' to '{1}', and using this to run pipeline.".format(config,TMP_CONFIG))
    copyfile(config, TMP_CONFIG)

    return kwargs

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
        raise KeyError("Keys {0} missing from section [{1}] in '{2}'.".format(missing_keys,section,config))

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
    logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=loglevel)

def main():

    #Parse command-line arguments, and setup logger
    args = parse_args()
    setup_logger(args.config,args.verbose)

    #Mutually exclusive arguments - display version, build config file or run pipeline
    if args.version:
        logger.info('This is version {0}'.format(__version__))
    if args.build:
        default_config(vars(args))
    if args.run:
        kwargs = format_args(args.config)
        write_jobs(args.config, **kwargs)

if __name__ == "__main__":
    main()
