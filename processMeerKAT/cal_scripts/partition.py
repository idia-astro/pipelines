
"""
Runs partition on the input MS
"""
import sys
import os

import config_parser
from config_parser import validate_args as va
from cal_scripts import get_fields

def do_partition(visname, spw):
    # Run partition
    mvis = os.path.split(visname.replace('.ms', '.mms'))[1]
    msmd.open(visname)
    nscan = msmd.nscans()
    msmd.close()
    msmd.done()

    partition(vis=visname, outputvis=mvis, spw=spw, createmms=True, datacolumn='DATA',
            numsubms=nscan, separationaxis='scan')

def main():

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool, default=False)
    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')
    spw = va(taskvals, 'crosscal', 'spw', str, default='')

    do_partition(visname, spw)

if __name__ == '__main__':
    main()

