"""
Validates the input parameters prior to running any of the scripts.
This can also be run at any stage of the pipeline.
"""

import sys
import os

import config_parser
from config_parser import validate_args as va
from cal_scripts import get_fields
import processMeerKAT


def validateinput():
    """
    Parse the input config file (command line argument) and validate that the
    parameters look okay
    """

    print('This is version {0}'.format(processMeerKAT.__version__))

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool, default=False)
    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')

    if not os.path.exists(visname):
        raise IOError("Path to MS %s not found" % (visname))

    # Check if the reference antenna exists, and complain and quit if it doesn't
    if not calcrefant:
        get_fields.check_refant(MS=visname, refant=refant, warn=False)



if __name__ == '__main__':
    validateinput()

