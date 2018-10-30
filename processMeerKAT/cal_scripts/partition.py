
"""
Runs partition on the input MS
"""
import sys
import os

#from processMeerKAT import config_parser
import config_parser
from config_parser import validate_args as va
from cal_scripts import get_fields


# Get the name of the config file
args = config_parser.parse_args()

# Parse config file
taskvals, config = config_parser.parse_config(args['config'])

visname = va(taskvals, 'data', 'vis', str)
calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool, default=False)
refant = va(taskvals, 'crosscal', 'refant', str, default='m005')

# Check if the reference antenna exists, and complain and quit if it doesn't
if not calcrefant:
    get_fields.check_refant(MS=visname, refant=refant, warn=False)

# Run partition
mvis = os.path.split(visname.replace('.ms', '.mms'))[1]
partition(vis=visname, outputvis=mvis, createmms=True, datacolumn='DATA')
