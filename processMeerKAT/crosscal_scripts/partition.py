#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

"""
Runs partition on the input MS
"""
import sys
import os

# Adapt PYTHONPATH to include processMeerKAT
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))


import config_parser
from config_parser import validate_args as va
import read_ms
import processMeerKAT
import bookkeeping

from casatasks import *
logfile=casalog.logfile()
from casatools import msmetadata
import casampi
msmd = msmetadata()

def do_partition(visname, spw, preavg, CPUs, include_crosshand, createmms, spwname):
    # Get the .ms bit of the filename, case independent
    basename, ext = os.path.splitext(visname)
    filebase = os.path.split(basename)[1]
    extn = 'mms' if createmms else 'ms'

    mvis = '{0}.{1}.{2}'.format(filebase,spwname,extn)
    nscan = 1 if not createmms else msmd.nscans()
    chanaverage = True if preavg > 1 else False
    correlation = '' if include_crosshand else 'XX,YY'

    mstransform(vis=visname, outputvis=mvis, spw=spw, createmms=createmms, datacolumn='DATA', chanaverage=chanaverage, chanbin=preavg,
                numsubms=nscan, separationaxis='scan', keepflags=True, usewtspectrum=True, nthreads=CPUs, antenna='*&', correlation=correlation)

    return mvis

def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool, default=False)
    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')
    spw = va(taskvals, 'crosscal', 'spw', str, default='')
    nspw = va(taskvals, 'crosscal', 'nspw', int, default='')
    tasks = va(taskvals, 'slurm', 'ntasks_per_node', int)
    preavg = va(taskvals, 'crosscal', 'chanbin', int, default=1)
    include_crosshand = va(taskvals, 'run', 'dopol', bool, default=False)
    createmms = va(taskvals, 'crosscal', 'createmms', bool, default=True)

    # HPC Specific Configuration
    known_hpc_path = os.path.dirname(SCRIPT_DIR)+"/known_hpc.cfg"
    KNOWN_HPCS, HPC_CONFIG = config_parser.parse_config(known_hpc_path)
    HPC_NAME = taskvals["run"]["hpc"]
    HPC_NAME = HPC_NAME if HPC_NAME in KNOWN_HPCS.keys() else "unknown"
    CPUS_PER_NODE_LIMIT = va(KNOWN_HPCS, HPC_NAME, "CPUS_PER_NODE_LIMIT".lower(), dtype=int)

    if nspw > 1:
        casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_ARRAY_JOB_ID}_{SLURM_ARRAY_TASK_ID}.casa'.format(**os.environ))
    else:
        logfile=casalog.logfile()
        casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))

    if ',' in spw:
        low,high,unit,dirs = config_parser.parse_spw(args['config'])
        spwname = '{0:.0f}~{1:.0f}MHz'.format(min(low),max(high))
    else:
        spwname = spw.replace('*:','')

    msmd.open(visname)
    npol = msmd.ncorrforpol()[0]

    if not include_crosshand and npol == 4:
        npol = 2
    CPUs = npol if tasks*npol <= CPUS_PER_NODE_LIMIT else 1 #hard-code for number of polarisations

    mvis = do_partition(visname, spw, preavg, CPUs, include_crosshand, createmms, spwname)
    mvis = "'{0}'".format(mvis)
    vis = "'{0}'".format(visname)

    config_parser.overwrite_config(args['config'], conf_sec='data', conf_dict={'vis':mvis})
    config_parser.overwrite_config(args['config'], conf_sec='run', sec_comment='# Internal variables for pipeline execution', conf_dict={'orig_vis':vis})
    msmd.done()

if __name__ == '__main__':

    bookkeeping.run_script(main,logfile)
