#Copyright (C) 2022 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os

from config_parser import validate_args as va
import processMeerKAT
import read_ms, bookkeeping

from casatools import msmetadata
from casatasks import casalog
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
msmd = msmetadata()

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def main(args,taskvals):
    """
    Parse the input config file (command line argument) and validate that the
    parameters look okay
    """

    cwd=os.getcwd()
    os.chdir(processMeerKAT.SCRIPT_DIR)
    commit=os.popen('git log --format="%H" -n 1').read()
    os.chdir(cwd)

    logger.info('This is version {0} of the pipeline - commit ID {1}'.format(processMeerKAT.__version__,commit))

    visname = va(taskvals, 'data', 'vis', str)
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool)
    refant = taskvals['crosscal']['refant']
    if type(refant) is str and 'm' in refant:
        refant = va(taskvals, 'crosscal', 'refant', str)
    else:
        refant = va(taskvals, 'crosscal', 'refant', int)
    nspw = va(taskvals, 'crosscal', 'nspw', int)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    # Check if the reference antenna exists, and complain and quit if it doesn't
    if not calcrefant:
        refant = va(taskvals, 'crosscal', 'refant', str)
        msmd.open(visname)
        read_ms.check_refant(MS=visname, refant=refant, config=args['config'], warn=False)
        msmd.done()

    if not os.path.exists(visname):
        raise IOError("Path to MS %s not found" % (visname))


if __name__ == '__main__':

    bookkeeping.run_script(main,logfile)
