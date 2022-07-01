#Copyright (C) 2022 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os

import config_parser
from config_parser import validate_args as va
import bookkeeping

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
import casampi

def do_pre_flag_2(visname, fields):

    calfields = ','.join(set([i for i in (','.join([fields.gainfields] + [fields.extrafields]).split(',')) if i])) #remove duplicate and empty fields

    # Flag using 'tfcrop' option for flux, phase cal and extra fields tight flagging
    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected",
            field=calfields, ntime="scan", timecutoff=6.0,
            freqcutoff=5.0, timefit="line", freqfit="line",
            flagdimension="freqtime", extendflags=False, timedevscale=5.0,
            freqdevscale=5.0, extendpols=False, growaround=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # now flag using 'rflag' option  for flux, phase cal and extra fields tight flagging
    flagdata(vis=visname, mode="rflag", datacolumn="corrected",
            field=calfields, timecutoff=5.0, freqcutoff=5.0,
            timefit="poly", freqfit="line", flagdimension="freqtime",
            extendflags=False, timedevscale=4.0, freqdevscale=4.0,
            spectralmax=500.0, extendpols=False, growaround=False,
            flagneartime=False, flagnearfreq=False, action="apply",
            flagbackup=True, overwrite=True, writeflags=True)

    ## Now extend the flags (70% more means full flag, change if required)
    flagdata(vis=visname, mode="extend", field=calfields,
            datacolumn="corrected", clipzeros=True, ntime="scan",
            extendflags=False, extendpols=False, growtime=90.0, growfreq=90.0,
            growaround=False, flagneartime=False, flagnearfreq=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # Now flag for target - moderate flagging, more flagging in self-cal cycles
    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected",
            field=fields.targetfield, ntime="scan", timecutoff=6.0, freqcutoff=5.0,
            timefit="poly", freqfit="line", flagdimension="freqtime",
            extendflags=False, timedevscale=5.0, freqdevscale=5.0,
            extendpols=False, growaround=False, action="apply", flagbackup=True,
            overwrite=True, writeflags=True)

    # now flag using 'rflag' option
    flagdata(vis=visname, mode="rflag", datacolumn="corrected",
            field=fields.targetfield, timecutoff=5.0, freqcutoff=5.0, timefit="poly",
            freqfit="poly", flagdimension="freqtime", extendflags=False,
            timedevscale=5.0, freqdevscale=5.0, spectralmax=500.0,
            extendpols=False, growaround=False, flagneartime=False,
            flagnearfreq=False, action="apply", flagbackup=True, overwrite=True,
            writeflags=True)

    # Now summary
    flagdata(vis=visname, mode="summary", datacolumn="corrected",
            extendflags=True, name=visname + 'summary.split', action="apply",
            flagbackup=True, overwrite=True, writeflags=True)

def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    do_pre_flag_2(visname, fields)

if __name__ == '__main__':

    bookkeeping.run_script(main,logfile)
