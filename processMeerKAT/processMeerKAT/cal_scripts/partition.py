"""
Runs partition on the input MS
"""

from processMeerKAT import config_parser

# Get the name of the config file
args = config_parser.parse_args()

# Parse config file
taskvals, config = config_parser.parse_config(args['--config'])

# Partition
visname = taskvals['data']['vis']
mvis = visname.replace('.ms', '.mms')
partition(vis=visname, outputvis=mvis, createmms=True, datacolumn='DATA')
