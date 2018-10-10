#!/usr/bin/python2.7

import argparse
import os
import sys
import casa_functions

TOTAL_NODES_LIMIT = 7
NTASKS_PER_NODE_LIMIT = 28
CPUS_PER_NODE_LIMIT = 64
MEM_PER_NODE_GB_LIMIT = 512

THIS_PROG = sys.argv[0]
SCRIPT_DIR = os.path.dirname(THIS_PROG)
LOG_DIR = 'logs'
PLOT_DIR = 'plots'
CONFIG = 'default_config.ini'

threadsafe_tasks = ['flagdata', 'setjy', 'applycal', 'hanningsmooth', 'cvel2', 'uvcontsub',
                    'mstransform', 'partition', 'split', 'tclean']

def parse_args():

    """Parse arguments into this script."""

    parser = argparse.ArgumentParser(prog=THIS_PROG,description='Processes MeerKAT data via CASA measurement set.')

    parser.add_argument("-M","--MS",metavar="path", required=False, type=str, help="Path to measurement set.")
    parser.add_argument("--config",metavar="path", required=False, type=str, help="Path to config file.")
    parser.add_argument("-N","--nodes",metavar="num", required=False, type=int, default=4, help="Use this number of nodes [default: 4; max: {0}].".format(TOTAL_NODES_LIMIT))
    parser.add_argument("-t","--ntasks-per-node", metavar="num", required=False, type=int, default=16, help="Use this number of tasks (per node) [default: 16; max: {0}].".format(NTASKS_PER_NODE_LIMIT))
    parser.add_argument("-C","--cpus-per-task", metavar="num", required=False, type=int, default=4, help="Use this number of CPUs (per task) [default: 4; max: {0} / ntasks-per-node].".format(CPUS_PER_NODE_LIMIT))
    parser.add_argument("-m","--mem-per-cpu", metavar="num", required=False, type=int, default=4096, help="Use this many MB of memory (per core) [default: 4096; max: {0} GB / (ntasks-per-node * cpus-per-task)].".format(MEM_PER_NODE_GB_LIMIT))
    parser.add_argument("-p","--plane", metavar="num", required=False, type=int, default=4, help="Distrubute tasks of this block size before moving onto next node [default: 4; max: ntasks-per-node].")
    parser.add_argument("-c","--CASA", metavar="bogus", required=False, type=str, help="Bogus argument to swallow up CASA call.")

    parser.add_argument("-n","--nosubmit", action="store_true", required=False, default=False, help="Don't submit jobs to SLURM queue [default: False].")
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
            parser.error("Input config file '{0}' not found.".format(args.run))

    if args.ntasks_per_node > NTASKS_PER_NODE_LIMIT:
        parser.error("The number of tasks [-t --ntasks-per-node] per node must not exceed {0}. You input {1}.".format(NTASKS_PER_NODE_LIMIT,args.ntasks-per-node))

    if args.nodes > TOTAL_NODES_LIMIT:
        parser.error("The number of nodes [-n --nodes] per node must not exceed {0}. You input {1}.".format(TOTAL_NODES_LIMIT,args.nodes))

    if args.cpus_per_task * args.ntasks_per_node > CPUS_PER_NODE_LIMIT:
        parser.error("The number of cpus per node [-t --ntasks-per-node] * [-c --cpus-per-task] must not exceed {0}. You input {1}.".format(CPUS_PER_NODE_LIMIT,args.ntasks_per_node))

    if args.mem_per_cpu * args.cpus_per_task * args.ntasks_per_node > 512 * 1024:
        parser.error("The memory per node [-m --mem-per-cpu] * [-t --ntasks-per-node] * [-C --cpus-per-task] must not exceed {0}. You input {1}.".format(NTASKS_PER_NODE_LIMIT,args.ntasks_per_node))

    return args

def write_command(script,args,mpi_wrapper="/data/users/frank/casa-cluster/casa-prerelease-5.3.0-115.el7/bin/mpicasa",
                container="/users/frank/casameer.simg",casa_task=True):

    params = locals()
    params['SCRIPT_DIR'] = SCRIPT_DIR
    if casa_task:
        params['casa_call'] = """"casa" --nologger --nogui --logfile {LOG_DIR}/casa-{job}.log -c"""
    else:
        params['casa_call'] = ''

    return "{mpi_wrapper} /usr/bin/singularity exec {container} {casa_call} {SCRIPT_DIR}/{script} {args}".format(**params)

def write_sbatch(script,args,step,time="00:10:00",nodes=4,tasks=16,cpus=4,mem=4096,name="job",plane=1,
                mpi_wrapper="/data/users/frank/casa-cluster/casa-prerelease-5.3.0-115.el7/bin/mpicasa",
                container="/users/frank/casameer.simg",jobIDs=[],casa_task=True,noSubmit=False,verbose=False):

    """Write a SLURM sbatch file calling a certain script with a particular configuration.

    Arguments:
    ----------
    script : str
        Path to script that is called within sbatch file.
    args : str
        Arguments passed into script that is called within sbatch file.
    step : int
        The step within the overall set of sbatch jobs submitted to the SLURM queue.
    time : str
        Time limit on this job.
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
    jobIDs : list
        List of job IDs for SLURM jobs needing to finish before this one is run.
    casa_task : bool
        Is the script that is called within this job a CASA task?
    noSubmit : bool
        Don't submit the sbatch job to the SLURM queue, only write the file.
    verbose : bool
        Verbose output?

    Returns:
    --------
    jobIDs : list
        Input job IDs with this job ID appended if noSubmit is False.
    step : int
        Input step + 1."""

    if not os.path.exists(LOG_DIR):
        os.mkdir(LOG_DIR)

    #store parameters passed into this function as dictionary, and add to it
    params = locals()
    params['LOG_DIR'] = LOG_DIR
    params['job'] = '${SLURM_JOB_ID}'
    params['command'] = write_command(**params)

    contents = """#!/bin/bash
    #SBATCH --time={time}
    #SBATCH -N {nodes}
    #SBATCH --ntasks-per-node={tasks}
    #SBATCH -J {name}
    #SBATCH -m plane={plane}
    #SBATCH -o {LOG_DIR}/{name}-%j.out
    #SBATCH -e {LOG_DIR}/{name}-%j.err

    {command}"""
    #--nologfile --log2term 2> {LOG_DIR}/{name}-{job}.stderr 1> {LOG_DIR}/{name}-{job}.stdout

    #insert arguments and remove whitespace
    contents = contents.format(**params).replace("    ","")

    #write sbatch file
    sbatch = '{0}_{1}.sbatch'.format(name,step)
    config = open(sbatch,'w')
    config.write(contents)
    config.close()

    #submit sbatch file to SLURM queue
    out = 'out.tmp'
    command = 'sbatch'
    if len(jobIDs) > 0:
        command += " -d afterok:"
        command += ','.join(str(jobID) for jobID in jobIDs)

    command += ' {0} | tee {1}'.format(sbatch,out)

    if noSubmit:
        print 'Not submitting {0} to SLURM queue'.format(sbatch)
    else:
        if verbose:
            print 'Running following command:\n{0}'.format(command)
        print 'Submitting {0} to the SLURM queue.'.format(sbatch)
        os.system(command)

        #check output from calling sbatch
        output = open(out).read()
        if output == '':
            print 'There was an error submitting {0} to the slurm queue. Please check this file.'.format(sbatch)
            sys.exit(0)

        #extract jobIDs
        jobID = int(output.split(' ')[-1])
        jobIDs.append(jobID)

    step += 1
    return jobIDs,step


def write_jobs(MS, nodes=2, tasks=8, noSubmit=False, verbose=False):

    """Write a series of sbatch job files to calibrate a CASA measurement set.

    Arguments:
    ----------
    MS : str
        Path to measurement set.
    nodes : int
        The number of nodes to use for all thread-safe CASA tasks.
    tasks : int
        The number of tasks per node to use for all thread-safe CASA tasks.
    noSubmit : bool
        Don't submit the sbatch job to the SLURM queue, only write the file.
    verbose : bool
        Verbose output?"""

    jobIDs = []

    print "RUNNING PIPELINE BRAH!"

    #TODO: Create sbatch files pointing to Krishna's scripts here
    # #create directory for logs and plots if doesn't exist yet
    # for dir in [LOG_DIR,PLOT_DIR]:
    #     if not os.path.exists(DIR):
    #         os.mkdir(DIR)
    #
    # #partition the data
    # jobIDs = write_sbatch(THIS_PROG,args,step,time="00:10:00",nodes=nodes,tasks_per_node=tasks,name="partition",jobIDs=jobIDs,noSubmit=noSubmit,verbose=verbose)
    #
    # #flag the data
    # jobIDs = write_sbatch(THIS_PROG,args,step,time="00:30:00",nodes=nodes,tasks_per_node=tasks,name="inital_flag",jobIDs=jobIDs,noSubmit=noSubmit,verbose=verbose)


def default_config(MS,filename,verbose=False):

    """Generate default config file in current directory, pointing to MS."""

    os.system('cp {0}/{1} .'.format(SCRIPT_DIR,filename))
    args =  '-R -M {0} --config {1}'.format(MS,filename)

    #write and submit command
    command = write_command('get_fields.py', args, mpi_wrapper="srun", container="/users/frank/casameer.simg")
    if verbose:
        print command
    os.system(command)


#if __name__ == "__main__":
def main():

    args = parse_args()
    subms = args.nodes*args.ntasks_per_node

    if args.build:
        default_config(args.MS,CONFIG,args.verbose)
    elif args.run:
        write_jobs(args.MS, nodes=args.nodes, tasks=args.ntasks_per_node, noSubmit=args.nosubmit, verbose=args.verbose)

