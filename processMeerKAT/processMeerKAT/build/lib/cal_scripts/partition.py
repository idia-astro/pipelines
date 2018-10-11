"""
Runs partition on the input MS
"""

import sys, os
sys.path.append(os.getcwd())

from .. import config_parser

# Get the name of the config file
args = config_parser.parse_args()

taskvals, config_dict = config_parser.parse_config(args['--config'])
