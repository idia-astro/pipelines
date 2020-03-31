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

def do_partition(visname, spw, preavg, CPUs):
    # Get the .ms bit of the filename, case independent
    basename, ext = os.path.splitext(visname)
    filebase = os.path.split(basename)[1]

    mvis = '{0}.{1}.mms'.format(filebase,spw.replace('0:',''))
    nscan = msmd.nscans()
    chanaverage = True if preavg > 1 else False

    mstransform(vis=visname, outputvis=mvis, spw=spw, createmms=True, datacolumn='DATA', chanaverage=chanaverage, chanbin=preavg,
                numsubms=nscan, separationaxis='scan', keepflags=False, usewtspectrum=True, nthreads=CPUs)

    return mvis

def main():

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool, default=False)
    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')
    spw = va(taskvals, 'crosscal', 'spw', str, default='')
    tasks = va(taskvals, 'slurm', 'ntasks_per_node', int)
    preavg = va(taskvals, 'crosscal', 'preavg', int, default=1)

    msmd.open(visname)
    npol = msmd.ncorrforpol()[0]
    CPUs = npol if tasks*npol <= processMeerKAT.CPUS_PER_NODE_LIMIT else 1 #hard-code for number of polarisations

    mvis = do_partition(visname, spw, preavg, CPUs)
    mvis = "'{0}'".format(mvis)
    vis = "'{0}'".format(visname)

    config_parser.overwrite_config(args['config'], conf_sec='data', conf_dict={'vis':mvis})
    config_parser.overwrite_config(args['config'], conf_sec='data', conf_dict={'orig_vis':vis})
    msmd.done()

if __name__ == '__main__':
    main()

