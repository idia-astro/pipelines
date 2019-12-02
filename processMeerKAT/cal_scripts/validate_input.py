#Copyright (C) 2019 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

from __future__ import print_function

import sys
import os

import config_parser
from config_parser import validate_args as va
import processMeerKAT
from cal_scripts import get_fields, bookkeeping

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def validateinput():
    """
    Parse the input config file (command line argument) and validate that the
    parameters look okay
    """

    logger.info('This is version {0} of the pipeline'.format(processMeerKAT.__version__))

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool)
    refant = va(taskvals, 'crosscal', 'refant', str)
    nspw = va(taskvals, 'crosscal', 'nspw', int)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    # Check if the reference antenna exists, and complain and quit if it doesn't
    if not calcrefant:
        refant = va(taskvals, 'crosscal', 'refant', str)
        msmd.open(visname)
        get_fields.check_refant(MS=visname, refant=refant, config=config, warn=False)
        msmd.done()

    if not os.path.exists(visname):
        raise IOError("Path to MS %s not found" % (visname))


if __name__ == '__main__':
    validateinput()
