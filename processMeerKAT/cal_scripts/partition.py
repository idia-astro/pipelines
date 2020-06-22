#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

"""
Runs partition on the input MS
"""
import sys
import os

import config_parser
from config_parser import validate_args as va
from cal_scripts import get_fields
import processMeerKAT
from cal_scripts import bookkeeping

def do_partition(visname, spw, preavg, CPUs, include_crosshand):
    # Get the .ms bit of the filename, case independent
    basename, ext = os.path.splitext(visname)
    filebase = os.path.split(basename)[1]

    mvis = '{0}.{1}.mms'.format(filebase,spw.replace('0:',''))
    nscan = msmd.nscans()
    chanaverage = True if preavg > 1 else False
    correlation = '' if include_crosshand else 'XX,YY'

    mstransform(vis=visname, outputvis=mvis, spw=spw, createmms=True, datacolumn='DATA', chanaverage=chanaverage, chanbin=preavg,
                numsubms=nscan, separationaxis='scan', keepflags=False, usewtspectrum=True, nthreads=CPUs, antenna='*&', correlation=correlation)

    return mvis

def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool, default=False)
    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')
    spw = va(taskvals, 'crosscal', 'spw', str, default='')
    tasks = va(taskvals, 'slurm', 'ntasks_per_node', int)
    preavg = va(taskvals, 'crosscal', 'chanbin', int, default=1)
    include_crosshand = va(taskvals, 'run', 'dopol', bool, default=False)

    msmd.open(visname)
    npol = msmd.ncorrforpol()[0]

    if not include_crosshand and npol == 4:
        npol = 2
    CPUs = npol if tasks*npol <= processMeerKAT.CPUS_PER_NODE_LIMIT else 1 #hard-code for number of polarisations

    mvis = do_partition(visname, spw, preavg, CPUs, include_crosshand)
    mvis = "'{0}'".format(mvis)
    vis = "'{0}'".format(visname)

    config_parser.overwrite_config(args['config'], conf_sec='data', conf_dict={'vis':mvis})
    config_parser.overwrite_config(args['config'], conf_sec='run', sec_comment='# Internal variables for pipeline execution', conf_dict={'orig_vis':vis})
    msmd.done()

if __name__ == '__main__':

    bookkeeping.run_script(main)