#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

from __future__ import print_function

import sys
import os

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def do_parallel_cal_apply(visname, fields, calfiles):

    if len(fields.gainfields) > 1:
        fluxfile = calfiles.fluxfile
    else:
        fluxfile = calfiles.gainfile

    logger.info(" applying calibration -> primary calibrator")
    applycal(vis=visname, field=fields.fluxfield, selectdata=False,
            calwt=False, gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                fluxfile], gainfield=[fields.kcorrfield,
                    fields.bpassfield, fields.fluxfield], parang=True)

    logger.info(" applying calibration -> secondary calibrator")
    applycal(vis=visname, field=fields.secondaryfield,
            selectdata=False, calwt=False, gaintable=[calfiles.kcorrfile,
                calfiles.bpassfile, fluxfile],
            gainfield=[fields.kcorrfield, fields.bpassfield,
                fields.secondaryfield], parang=True)

    logger.info(" applying calibration -> target calibrator")
    applycal(vis=visname, field=fields.targetfield, selectdata=False,
            calwt=False, gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                fluxfile], gainfield=[fields.kcorrfield,
                    fields.bpassfield, fields.secondaryfield], parang=True)


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')
    minbaselines = va(taskvals, 'crosscal', 'minbaselines', int, default=4)

    do_parallel_cal_apply(visname, fields, calfiles)
