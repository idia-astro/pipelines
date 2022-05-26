#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

#!/usr/bin/env python3
import os
import bookkeeping
import config_parser
from selfcal_scripts.selfcal_part2 import find_outliers
from casatasks import casalog
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))

if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    find_outliers(**params,step='sky')
    bookkeeping.rename_logs(logfile)
