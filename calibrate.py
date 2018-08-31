#!/usr/bin/python2.7

import argparse
import os
import sys
import casa_functions

NTASKS_PER_NODE_LIMIT = 28
TOTAL_NODES = 7
THIS_PROG = sys.argv[0]
LOG_DIR = 'logs'
PLOT_DIR = 'plots'

def parse_args():

    """Parse arguments into this script."""

    parser = argparse.ArgumentParser(prog=THIS_PROG,description='Calibrate a CASA measurement set.')
    tasks = ['script','flag','partition','plotms','setjy','bandpass','fastplot']
    known_intents = ['CALIBRATE_FLUX','CALIBRATE_BANDPASS','CALIBRATE_PHASE','CALIBRATE_AMPLI','TARGET']

    #control arguments
    parser.add_argument("-n","--nodes",metavar="num",required=False, type=int, default=4, help="Use this number of nodes [default: 4; max: {0}].".format(TOTAL_NODES))
    parser.add_argument("-t","--tasks",metavar="num",required=False, type=int, default=24, help="Use this number of tasks (per node) [default: 24; max: {0}].".format(NTASKS_PER_NODE_LIMIT))
    parser.add_argument("-r","--run",metavar="type",required=False, type=str, default='script', help="What to run? {0}? [default: 'script'].".format(tasks))
    parser.add_argument("-f","--fname",metavar="path",required=False, type=str, default='plot.png', help="Filename to write to disc (e.g. for plotms) [default: 'plot.png'].")
    parser.add_argument("-m","--flagmode",metavar="mode",required=False, type=str, default='tfcrop', help="Flagging mode for this run [default: 'tfcrop'].")
    parser.add_argument("-N","--nosubmit",action="store_true",required=False, default=False, help="Don't submit jobs to SLURM queue [default: False].")
    parser.add_argument("-v","--verbose",action="store_true",required=False, default=False, help="Verbose output? [default: False].")
    parser.add_argument("-l","--logy",action="store_true",required=False, default=False, help="Log the y axis of output plot [default: False].")

    #data selection arguments
    parser.add_argument("-i","--intent",metavar="type",required=False, type=str, default='TARGET', help="Select field that has this intent [default: 'TARGET'].")
    parser.add_argument("-x","--xaxis",metavar="type",required=False, type=str, default='freq', help="X-axis on which some functions operate (e.g. plotms) [default: 'frequency'].")
    parser.add_argument("-y","--yaxis",metavar="type",required=False, type=str, default='amp', help="Y-axis on which some functions operate (e.g. plotms) [default: 'amplitude'].")
    parser.add_argument("-p","--corr",metavar="type",required=False, type=str, default='xx,yy', help="Correlation to select [default: 'xx,yy'].")
    parser.add_argument("-e","--extent",metavar="num",required=False, type=float, default=0.1, help="Scale limits of x-axis by this fraction max value. [default: 0.1].")
    parser.add_argument("-F","--field",metavar="num",required=False, type=str, default='0', help="Select this field [default: '0'].")
    parser.add_argument("-C","--column",metavar="type",required=False, type=str, default='data', help="Data column to select [default: 'data'].")
    parser.add_argument("-T","--table",metavar="type",required=False, type=str, help="Table to use or write [default: ''].")
    parser.add_argument("-M","--MS",metavar="path",required=False, type=str, help="Path to measurement set.")

    args, unknown = parser.parse_known_args()

    #TODO FIX THIS
    # if len(unknown) > 0:
    #     #remove arguments stored from calling this program from casa
    #     if '-c' in unknown:
    #         unknown.remove('-c')
    #         unknown.remove(parser.prog)
    #     if len(unknown) > 0:
    #         parser.error('Unknown input argument(s) present - {0}'.format(unknown))

    if args.MS is None and args.table is None:
        parser.error("You need to input a MS [-M --MS] or table [-T --table].")

    if args.MS is not None and not os.path.isdir(args.MS):
        parser.error("Input MS '{0}' not found.".format(args.MS))
    if args.table is not None and not os.path.isdir(args.table):
        parser.error("Input table '{0}' not found.".format(args.table))

    if args.tasks > NTASKS_PER_NODE_LIMIT:
        parser.error("The number of tasks [-t --tasks] per node must not exceed {0}. You input {1}.".format(NTASKS_PER_NODE_LIMIT,args.tasks))

    if args.nodes > TOTAL_NODES:
        parser.error("The number of nodes [-n --nodes] per node must not exceed {0}. You input {1}.".format(TOTAL_NODES,args.nodes))

    if args.run.lower() not in tasks:
        parser.error("Unknown option for argument [-r --run]. You input '{0}'. Please input one of the following: {1}.".format(args.run,tasks))

    if args.intent.upper() not in known_intents:
        parser.error("Unknown intent. You input '{0}', but known intents are {1}.".format(args.intent,known_intents))

    return args

def write_sbatch(script,args,step,time="00:10:00",nodes="2-2",tasks_per_node=8,name="job",mode="cyclic",
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
    tasks_per_node : int
        The number of tasks per node for this job.
    name : str
        Name for this job. This gets used in naming the various output files, as well as deciding which (e.g. CASA) functions to call within this module.
    mode : str
        Mode for this job. Use 'plane=N' or 'cyclic'.
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
    if casa_task:
        params['casa_call'] = """"casa" --nologger --nogui --logfile {LOG_DIR}/casa-{job}.log -c"""
    else:
        params['casa_call'] = ''

    contents = """#!/bin/bash
    #SBATCH --time={time}
    #SBATCH -N {nodes}
    #SBATCH --ntasks-per-node={tasks_per_node}
    #SBATCH -J {name}
    #SBATCH -m {mode}
    #SBATCH -o {LOG_DIR}/{name}-%j.out
    #SBATCH -e {LOG_DIR}/{name}-%j.err

    {mpi_wrapper} /usr/bin/singularity exec {container} {casa_call} {script} -r {name} {args}"""
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

def calibrate_data(MS, basename, nodes=2, tasks=8, noSubmit=False, verbose=False):

    """Write a series of sbatch job files to calibrate a CASA measurement set.

    Arguments:
    ----------
    MS : str
        Path to measurement set.
    basename : str
        Basename of output files.
    nodes : int
        The number of nodes to use for all thread-safe CASA tasks.
    tasks : int
        The number of tasks per node to use for all thread-safe CASA tasks.
    noSubmit : bool
        Don't submit the sbatch job to the SLURM queue, only write the file.
    verbose : bool
        Verbose output?"""

    #count steps for unique sbatch file names
    step = 0
    jobIDs = []

    #create names for output data
    MMS = '{0}.mms'.format(basename)
    BPtable = '{0}.b0'.format(basename)
    Gtable = '{0}.g0'.format(basename)

    #create directory for plots if doesn't exist yet
    if not os.path.exists(PLOT_DIR):
        os.mkdir(PLOT_DIR)

    #partition the data
    args = '-M {0} -T {1}'.format(MS,MMS)
    jobIDs,step = write_sbatch(THIS_PROG,args,step,time="00:10:00",nodes=nodes,tasks_per_node=tasks,name="partition",jobIDs=jobIDs,noSubmit=noSubmit,verbose=verbose)

    #point to bandpass calibrator for following steps
    intent = 'CALIBRATE_BANDPASS'
    args = '-i {0} -M {1}'.format(intent,MMS)

    #flag the data
    jobIDs,step = write_sbatch(THIS_PROG,args,step,time="00:30:00",nodes=nodes,tasks_per_node=tasks,name="flag",jobIDs=jobIDs,noSubmit=noSubmit,verbose=verbose)

    #TODO check that source is known source for setjy
    #set absolute flux scale
    jobIDs,step = write_sbatch(THIS_PROG,args,step,time="00:20:00",nodes=nodes,tasks_per_node=tasks,name="setjy",jobIDs=jobIDs,noSubmit=noSubmit,verbose=verbose)

    #solve for bandpass
    jobIDs,step = write_sbatch(THIS_PROG,args,step,time="00:10:00",nodes=1,tasks_per_node=1,name="bandpass",jobIDs=jobIDs,noSubmit=noSubmit,verbose=verbose)

    #plot bandpass model
    container = '/data/exp_soft/containers/kern-2.img'
    args = '-f fastplot_amp.png -x Chan -y Amp -C CPARAM -T {0}'.format(BPtable)
    jobIDs,step = write_sbatch(THIS_PROG,args,step,time="00:01:00",nodes=1,tasks_per_node=1,name="fastplot",mpi_wrapper='srun',container=container,jobIDs=jobIDs,casa_task=False,noSubmit=noSubmit,verbose=verbose)

    #plot bandpass model
    args = '-f fastplot_phase.png -x Chan -y Phase -C CPARAM -T {0}'.format(BPtable)
    jobIDs,step = write_sbatch(THIS_PROG,args,step,time="00:01:00",nodes=1,tasks_per_node=1,name="fastplot",mpi_wrapper='srun',container=container,jobIDs=jobIDs,casa_task=False,noSubmit=noSubmit,verbose=verbose)

    #flag the data
    args = '-i {0} -m {1} -M {2}'.format(intent,'rflag',MMS)
    jobIDs,step = write_sbatch(THIS_PROG,args,step,time="00:30:00",nodes=nodes,tasks_per_node=tasks,name="flag",jobIDs=jobIDs,noSubmit=noSubmit,verbose=verbose)


if __name__ == "__main__":

    args = parse_args()
    subms = args.nodes*args.tasks
    task = args.run.lower()
    if args.MS is not None:
        basename = os.path.splitext(os.path.basename(args.MS))[0]
    else:
        basename = os.path.splitext(os.path.basename(args.table))[0]

    if task == 'script':
        calibrate_data(args.MS, basename, args.nodes, args.tasks, args.nosubmit, args.verbose)
    elif task == 'partition':
        casa_functions.split(args.MS, args.table, subms, args.column)
    elif task == 'flag':
        casa_functions.flag(args.MS,args.intent,mode=args.flagmode,corr=args.corr)
    elif task == 'plotms':
        casa_functions.plot(args.MS,intent=args.intent,fname=args.fname)
    elif task == 'fastplot':
        import fastplot
        table = args.MS if args.table is None else args.table
        fastplot.msvis(table, col=args.column, field=args.field, xaxis=args.xaxis, yaxis=args.yaxis, extent=args.extent, logy=args.logy, fname='{0}/{1}_{2}'.format(PLOT_DIR,basename,args.fname))
    elif task == 'setjy':
        casa_functions.boot(args.MS, intent=args.intent)
    elif task == 'bandpass':
        casa_functions.bpass(args.MS,intent=args.intent,caltable=args.table)


