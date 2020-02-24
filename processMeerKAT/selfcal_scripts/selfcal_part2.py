#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os

import config_parser
from config_parser import validate_args as va

if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])
    params = taskvals['selfcal']
    for arg in ['multiscale','nterms','calmode','atrous']:
        params[arg] = [params[arg]] * len(params['niter'])
    selfcal(**params)
