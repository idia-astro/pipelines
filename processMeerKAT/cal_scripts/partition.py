
"""
Runs partition on the input MS
"""
import sys
import os

#from processMeerKAT import config_parser
import config_parser
from config_parser import validate_args as va

# Get the name of the config file
args = config_parser.parse_args()
print(args)

# Parse config file
taskvals, config = config_parser.parse_config(args['config'])

# Partition
visname = va(taskvals, 'data', 'vis', str)
mvis = os.path.split(visname.replace('.ms', '.mms'))[1]
partition(vis=visname, outputvis=mvis, createmms=True, datacolumn='DATA')