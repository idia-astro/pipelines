#!/usr/bin/python2.7

import argparse
import os
import sys
import config_parser
from shutil import copyfile

#Set global limits for cluster configuration
TOTAL_NODES_LIMIT = 7
NTASKS_PER_NODE_LIMIT = 28
CPUS_PER_NODE_LIMIT = 64
MEM_PER_NODE_GB_LIMIT = 230

#Set global values for paths and file names
THIS_PROG = sys.argv[0]
SCRIPT_DIR = os.path.dirname(THIS_PROG)
LOG_DIR = 'logs'
PLOT_DIR = 'plots'
CALIBR_SCRIPTS_DIR = 'cal_scripts'
CONFIG = 'default_config.txt'
TMP_CONFIG = '.config.tmp'
MASTER_SCRIPT = 'submit_pipeline.sh'

#Set global values for arguments copied to config file, and some of their default values
SLURM_CONFIG_KEYS = ['nodes','ntasks_per_node','cpus_per_task','mem_per_cpu','plane','submit','scripts','verbose']
CONTAINER = '/data/exp_soft/pipelines/casameer-5.4.0.simg'
MPI_WRAPPER = '/data/exp_soft/pipelines/casa-prerelease-5.3.0-115.el7/bin/mpicasa'
SCRIPTS = [ ('partition.py',True,''),
            ('flag_round_1.py',True,''),
            ('parallel_cal.py',False,''),
            ('flag_round_2.py',True,''),
            ('cross_cal.py',False,''),
            ('split.py',True,'')]

def parse_args():

    """Parse arguments into this script."""

    parser = argparse.ArgumentParser(prog=THIS_PROG,description='Process MeerKAT data via CASA measurement set.')

    parser.add_argument("-M","--MS",metavar="path", required=False, type=str, help="Path to measurement set.")
    parser.add_argument("--config",metavar="path", default=CONFIG, required=False, type=str, help="Path to config file.")
    parser.add_argument("-N","--nodes",metavar="num", required=False, type=int, default=4,
                        help="Use this number of nodes [default: 4; max: {0}].".format(TOTAL_NODES_LIMIT))
    parser.add_argument("-t","--ntasks-per-node", metavar="num", required=False, type=int, default=8,
                        help="Use this number of tasks (per node) [default: 8; max: {0}].".format(NTASKS_PER_NODE_LIMIT))
    parser.add_argument("-C","--cpus-per-task", metavar="num", required=False, type=int, default=3,
                        help="Use this number of CPUs (per task) [default: 3; max: {0} / ntasks-per-node].".format(CPUS_PER_NODE_LIMIT))
    parser.add_argument("-m","--mem-per-cpu", metavar="num", required=False, type=int, default=4096,
                        help="Use this many MB of memory (per core) [default: 4096; max: {0} GB / (ntasks-per-node * cpus-per-task)].".format(MEM_PER_NODE_GB_LIMIT))
    parser.add_argument("-p","--plane", metavar="num", required=False, type=int, default=4,
                        help="Distrubute tasks of this block size before moving onto next node [default: 4; max: ntasks-per-node].")
    parser.add_argument("-s","--scripts", metavar="list", required=False, type=list, default=SCRIPTS,
                        help="Run pipeline with these scripts, in this order, using this container (3nd tuple value - empty string to default to [--container]). Is it threadsafe (2nd tuple value)?")
    parser.add_argument("--mpi_wrapper", metavar="path", required=False, type=str, default=MPI_WRAPPER,
                        help="Use this mpi wrapper when calling scripts.")
    parser.add_argument("--container", metavar="path", required=False, type=str, default=CONTAINER, help="Use this container when calling scripts.")
    parser.add_argument("-c","--CASA", metavar="bogus", required=False, type=str, help="Bogus argument to swallow up CASA call.")

    parser.add_argument("-n","--submit", action="store_true", required=False, default=False, help="Don't submit jobs to SLURM queue [default: False].")
    parser.add_argument("-v","--verbose", action="store_true", required=False, default=False, help="Verbose output? [default: False].")

    #add mutually exclusive group - don't want to build config, and run pipeline at same time
    run_args = parser.add_mutually_exclusive_group(required=True)
    run_args.add_argument("-B","--build", action="store_true", required=False, default=False, help="Build default config file using input MS.")
    run_args.add_argument("-R","--run", action="store_true", required=False, default=False, help="Run pipeline with input config file.")

    args, unknown = parser.parse_known_args()

    if len(unknown) > 0:
        parser.error('Unknown input argument(s) present - {0}'.format(unknown))

    if args.build:
        if args.MS is None:
            parser.error("You must input an MS [-M --MS] to build the config file.")
        if not os.path.isdir(args.MS):
            parser.error("Input MS '{0}' not found.".format(args.MS))

    if args.run:
        if args.config is None:
            parser.error("You must input a config file [--config] to run the pipeline.")
        if not os.path.exists(args.config):
            parser.error("Input config file '{0}' not found.".format(args.config))

    if args.ntasks_per_node > NTASKS_PER_NODE_LIMIT:
        parser.error("The number of tasks [-t --ntasks-per-node] per node must not exceed {0}. You input {1}.".format(NTASKS_PER_NODE_LIMIT,args.ntasks-per-node))

    if args.nodes > TOTAL_NODES_LIMIT:
        parser.error("The number of nodes [-n --nodes] per node must not exceed {0}. You input {1}.".format(TOTAL_NODES_LIMIT,args.nodes))

    if args.cpus_per_task * args.ntasks_per_node > CPUS_PER_NODE_LIMIT:
        parser.error("The number of cpus per node [-t --ntasks-per-node] * [-c --cpus-per-task] must not exceed {0}. You input {1}.".format(CPUS_PER_NODE_LIMIT,args.cpus_per_node))

    if args.mem_per_cpu * args.cpus_per_task * args.ntasks_per_node > MEM_PER_NODE_GB_LIMIT * 1024:
        parser.error("The memory per node [-m --mem-per-cpu] * [-t --ntasks-per-node] * [-C --cpus-per-task] must not exceed {0}. You input {1}.".format(MEM_PER_NODE_GB_LIMIT,args.mem_per_cpu))

    return args,vars(args)

def write_command(script,args,name="job",mpi_wrapper="/data/exp_soft/pipelines/casa-prerelease-5.3.0-115.el7/bin/mpicasa",
                container=CONTAINER,casa_task=True,logfile=True):

    params = locals()
    params['LOG_DIR'] = LOG_DIR
    params['job'] = '${SLURM_JOB_ID}'
    params['casa_call'] = ''
    params['casa_log'] = ''

    #if script path doesn't exist and it's not in user's path, assume it's in the calibration directory
    if not os.path.exists(script) and script not in os.environ['PATH']:
        params['script'] = '{0}/{1}/{2}'.format(SCRIPT_DIR, CALIBR_SCRIPTS_DIR, script)

    if logfile:
        params['casa_log'] = '--logfile {LOG_DIR}/{name}-{job}.casa'.format(**params)
    if casa_task:
        params['casa_call'] = """"casa" --nologger --nogui {casa_log} -c""".format(**params)

    return "{mpi_wrapper} /usr/bin/singularity exec {container} {casa_call} {script} {args}".format(**params)


def write_sbatch(script,args,time="00:10:00",nodes=4,tasks=16,cpus=4,mem=4096,name="job",plane=1,
                mpi_wrapper=MPI_WRAPPER,container=CONTAINER,casa_task=True,verbose=False):

    """Write a SLURM sbatch file calling a certain script with a particular configuration.

    Arguments:
    ----------
    script : str
        Path to script that is called within sbatch file.
    args : str
        Arguments passed into script that is called within sbatch file.
    time : str
        Time limit on this job (not currently used).
    nodes : str
        Number of nodes to use for this job - e.g. '2-2' meaning minimum and maximum of 2 nodes.
    tasks : int
        The number of tasks per node for this job.
    cpus : int
        The number of CPUs per task for this job.
    mem : int
        The memory in MB to use per CPU for this job.
    name : str
        Name for this job. This gets used in naming the various output files, as well as deciding which (e.g. CASA) functions to call within this module.
    plane : int
        Distrubute tasks of this block size before moving onto next node, for this job.
    mpi_wrapper : str
        MPI wrapper for this job. e.g. 'srun', 'mpirun', 'mpicasa' (may need to specify path).
    container : str
        Path to singularity container used for this job.
    casa_task : bool
        Is the script that is called within this job a CASA task?
    verbose : bool
        Verbose output?"""

    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    #store parameters passed into this function as dictionary, and add to it
    params = locals()
    params['LOG_DIR'] = LOG_DIR
    params['job'] = '${SLURM_JOB_ID}'
    params['command'] = write_command(script,args,name=name,mpi_wrapper=mpi_wrapper,container=container,casa_task=casa_task)

    #SBATCH --time={time}
    contents = """#!/bin/bash
    #SBATCH -N {nodes}
    #SBATCH --ntasks-per-node={tasks}
    #SBATCH -c {cpus}
    #SBATCH --mem-per-cpu={mem}
    #SBATCH -J {name}
    #SBATCH -m plane={plane}
    #SBATCH -o {LOG_DIR}/{name}-%j.out
    #SBATCH -e {LOG_DIR}/{name}-%j.err

    {command}"""

    #insert arguments and remove whitespace
    contents = contents.format(**params).replace("    ","")

    #write sbatch file
    sbatch = '{0}.sbatch'.format(name)
    config = open(sbatch,'w')
    config.write(contents)
    config.close()

    if verbose:
        print 'Wrote sbatch file "{0}"'.format(sbatch)

def write_master(filename,scripts=[],submit=False,verbose=False):

    master = open(filename,'w')
    master.write('#!/bin/bash\n')

    command = 'sbatch {0}'.format(scripts[0])
    master.write('\n#{0}\n'.format(scripts[0]))
    if verbose:
        master.write('echo Submitting {0} SLURM queue with following command\necho {1}\n'.format(scripts[0],command))
    master.write("IDs=$({0} | cut -d ' ' -f4)\n".format(command))
    scripts.pop(0)


    for script in scripts:
        command = 'sbatch -d afterok:$IDs'
        master.write('\n#{0}\n'.format(script))
        if verbose:
            master.write('echo Submitting {0} SLURM queue with following command\necho {1} {0}\n'.format(script,command))
        master.write("IDs+=,$({0} {1} | cut -d ' ' -f4)\n".format(command,script))

    master.write('\n#Output message\n')
    master.write('echo Submitted scripts with following IDs: $IDs\n')

    #Add time as extn to this pipeline run, to give unique ID
    if not os.path.exists('jobScripts'):
        os.mkdir('jobScripts')
    master.write('\n#Add time as extn to this pipeline run, to give unique ID')
    master.write("\nDATE=$(date '+%Y-%m-%d-%H-%M-%S')\n")
    extn = '_$DATE.sh'
    killScript = 'jobScripts/killJobs' + extn
    summaryScript = 'jobScripts/summary' + extn
    errorScript = 'jobScripts/findErrors' + extn

    #Write each job script
    write_bash_job_script(master, killScript, 'echo scancel $IDs', 'kill all the jobs')
    write_bash_job_script(master, summaryScript, 'echo sacct -j $IDs', 'view the progress')
    do = """echo "for ID in {$IDs}; do cat %s/*\$ID.{out,err,casa} | grep 'SEVERE\|rror' | grep -v 'mpi\|MPI'; done" """ % LOG_DIR
    write_bash_job_script(master, errorScript, do, 'find errors')
    master.close()

    os.chmod(filename, 509)
    if submit:
        print 'Running master script "{0}"'.format(filename)
        os.system('./{0}'.format(filename))
    else:
        print 'Master script "{0}" written, but will not run.'.format(filename)

def write_bash_job_script(master,fname,do,purpose):

    master.write('\n#Create {0} file\n'.format(fname))
    master.write('echo "#!/bin/bash" > {0}\n'.format(fname))
    master.write('{0} >> {1}\n'.format(do,fname))
    master.write('chmod u+x {0}\n'.format(fname))
    master.write('echo Run {0} to {1}.\n'.format(fname,purpose))

def write_jobs(config, scripts=[], threadsafe=[], containers=[], mpi_wrapper=MPI_WRAPPER, nodes=4, ntasks_per_node=16,
                cpus_per_task=4, mem_per_cpu=4096, plane=1, submit=False, verbose=False):

    """Write a series of sbatch job files to calibrate a CASA measurement set.

    Arguments:
    ----------
    MS : str
        Path to measurement set.
    nodes : int
        The number of nodes to use for all thread-safe CASA tasks.
    tasks : int
        The number of tasks per node to use for all thread-safe CASA tasks.
    submit : bool
        Don't submit the sbatch job to the SLURM queue, only write the file.
    verbose : bool
        Verbose output?"""

    for i,script in enumerate(scripts):
        if threadsafe[i]:
            write_sbatch(script,'--config {0}'.format(config),time="01:00:00",nodes=nodes,tasks=ntasks_per_node,cpus=cpus_per_task,
                        mem=mem_per_cpu,plane=plane,mpi_wrapper=mpi_wrapper,container=containers[i],name=os.path.splitext(script)[0],verbose=verbose)
        else:
            write_sbatch(script,'--config {0}'.format(config),time="01:00:00",nodes=1,tasks=1,cpus=1,mem=8192,plane=1,
                        mpi_wrapper=mpi_wrapper,container=containers[i],name=os.path.splitext(script)[0],verbose=verbose)

    #Build master submission script, replacing all .py with .sbatch
    scripts = [scripts[i].replace('.py','.sbatch') for i in range(len(scripts))]
    write_master(MASTER_SCRIPT,scripts=scripts,submit=submit,verbose=verbose)


def default_config(arg_dict,filename,verbose=False):

    """Generate default config file in current directory, pointing to MS, with fields and SLURM parameters set."""

    #Copy default config to current location
    copyfile('{0}/{1}'.format(SCRIPT_DIR,CONFIG),filename)

    #Add following SLURM arguments to config file
    slurm_dict = get_slurm_dict(arg_dict,SLURM_CONFIG_KEYS)
    config_parser.overwrite_config(filename, conf_dict=slurm_dict, conf_sec='slurm')

    #Add MS to config file
    config_parser.overwrite_config(filename, conf_dict={'vis' : "'{0}'".format(arg_dict['MS'])}, conf_sec='data')

    #Write and submit command to extract fields
    params =  '-B -M {0} --config {1}'.format(arg_dict['MS'],filename)
    command = write_command('get_fields.py', params, mpi_wrapper="srun", container=arg_dict['container'],logfile=False)
    if verbose:
        print 'Extracting fields using the following command:\n{0}'.format(command)
    os.system(command)

    print 'Config "{0}" generated.'.format(filename)


def get_slurm_dict(arg_dict,slurm_config_keys):

    slurm_dict = {key:arg_dict[key] for key in slurm_config_keys}
    return slurm_dict

def format_args(args):

    config_dict = config_parser.parse_config(args.config)[0]
    copyfile(args.config, TMP_CONFIG)
    if 'slurm' in config_dict.keys():
        kwargs = config_dict['slurm']
    else:
        kwargs = get_slurm_dict(arg_dict)

    #Reformat scripts, to extract scripts, threadsafe, and containers
    scripts = kwargs['scripts']
    kwargs['scripts'] = [i[0] for i in scripts]
    kwargs['threadsafe'] = [i[1] for i in scripts]
    kwargs['containers'] = [i[2] for i in scripts]

    #Replace empty containers with default container and remove unwanted kwarg
    for i in range(len(kwargs['containers'])):
        if kwargs['containers'][i] == '':
            kwargs['containers'][i] = kwargs['container']
    kwargs.pop('container')

    return kwargs

def main():

    #Parse command-line arguments
    args,arg_dict = parse_args()

    if args.build:
        default_config(arg_dict,args.config,args.verbose)
    elif args.run:
        #Copy args from config file to TMP_CONFIG and use to write jobs
        kwargs = format_args(args)
        write_jobs(TMP_CONFIG, **kwargs)

if __name__ == "__main__":
    main()
