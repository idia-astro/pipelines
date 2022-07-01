#Copyright (C) 2022 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os

import config_parser
import bookkeeping
from config_parser import validate_args as va

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
import casampi

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def do_parallel_cal_apply(visname, fields, calfiles):

    if len(fields.gainfields.split(',')) > 1:
        fluxfile = calfiles.fluxfile
    else:
        fluxfile = calfiles.gainfile

    logger.info(" applying calibration -> primary calibrator")
    applycal(vis=visname, field=fields.fluxfield,
            selectdata=False, calwt=False, gaintable=[calfiles.kcorrfile, calfiles.bpassfile, fluxfile],
            gainfield=[fields.kcorrfield, fields.bpassfield, fields.fluxfield], parang=False, interp='linear,linearflag')

    logger.info(" applying calibration -> phase calibrator, targets and extra fields")
    field = ','.join(set([i for i in (','.join([fields.secondaryfield] + [fields.targetfield] + [fields.extrafields]).split(',')) if i])) #remove duplicate and empty fields
    applycal(vis=visname, field=field, selectdata=False, calwt=False, gaintable=[calfiles.kcorrfile, calfiles.bpassfile, fluxfile],
            gainfield=[fields.kcorrfield, fields.bpassfield, fields.secondaryfield], parang=False, interp='linear,linearflag')

def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')
    minbaselines = va(taskvals, 'crosscal', 'minbaselines', int, default=4)

    do_parallel_cal_apply(visname, fields, calfiles)

if __name__ == '__main__':

    bookkeeping.run_script(main,logfile)
