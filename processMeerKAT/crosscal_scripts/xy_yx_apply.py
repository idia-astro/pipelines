#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os
import shutil

import config_parser
import bookkeeping, read_ms
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


def do_cross_cal_apply(visname, fields, calfiles, caldir):

    fluxfile = calfiles.fluxfile

    polfield = bookkeeping.polfield_name(visname)
    if polfield == '':
        polfield = fields.secondaryfield

    base = visname.replace('.ms', '')
    xy0ambpfile = os.path.join(caldir, base+'.xyambcal')
    xy0pfile    = os.path.join(caldir, base+'.xycal')

    if polfield == fields.secondaryfield:
        # Cannot resolve XY ambiguity so write into final file directly
        xyfile = xy0pfile
    else:
        xyfile = xy0ambpfile

    calfiles = calfiles._replace(xpolfile=xyfile)

    logger.info(" applying calibration: primary calibrator")
    applycal(vis=visname, field=fields.fluxfield,
            selectdata=False, calwt=False, gaintable=[calfiles.bpassfile, fluxfile, calfiles.dpolfile,calfiles.xpolfile],
            gainfield=[fields.bpassfield, fields.fluxfield, fields.bpassfield, polfield],
            parang=True, interp='nearest,linearflag,nearest,nearest')

    if polfield != fields.secondaryfield:
        logger.info(" applying calibration: polarization calibrator")
        applycal(vis=visname, field=polfield,
                selectdata=False, calwt=False, gaintable=[calfiles.bpassfile, fluxfile, calfiles.dpolfile,calfiles.xpolfile],
                gainfield=[fields.bpassfield, polfield, fields.bpassfield, polfield],
                parang=True, interp='nearest,linearflag,nearest,nearest')

    logger.info(" applying calibrations: phase calibrator, targets and extra fields")
    field = ','.join(set([i for i in (','.join([fields.secondaryfield] + [fields.targetfield] + [fields.extrafields]).split(',')) if i])) #remove duplicate and empty fields
    applycal(vis=visname, field=field, selectdata=False, calwt=False,
            gaintable=[calfiles.bpassfile, fluxfile, calfiles.dpolfile, calfiles.xpolfile],
            gainfield=[fields.bpassfield,fields.secondaryfield,fields.bpassfield,polfield],
            parang=True, interp='nearest,nearest,nearest,nearest')


def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    do_cross_cal_apply(visname, fields, calfiles, caldir)

if __name__ == '__main__':

    bookkeeping.run_script(main,logfile)
