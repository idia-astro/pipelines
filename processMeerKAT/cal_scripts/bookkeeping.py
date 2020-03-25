#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys

import config_parser
from collections import namedtuple
import os

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def get_calfiles(visname, caldir):
        base = os.path.splitext(visname)[0]
        kcorrfile = os.path.join(caldir,base + '.kcal')
        bpassfile = os.path.join(caldir,base + '.bcal')
        gainfile =  os.path.join(caldir,base + '.gcal')
        dpolfile =  os.path.join(caldir,base + '.pcal')
        xpolfile =  os.path.join(caldir,base + '.xcal')
        xdelfile =  os.path.join(caldir,base + '.xdel')
        fluxfile =  os.path.join(caldir,base + '.fluxscale')

        calfiles = namedtuple('calfiles',
                ['kcorrfile', 'bpassfile', 'gainfile', 'dpolfile', 'xpolfile',
                    'xdelfile', 'fluxfile'])
        return calfiles(kcorrfile, bpassfile, gainfile, dpolfile, xpolfile,
                xdelfile, fluxfile)


def bookkeeping(visname):
    # Book keeping
    caldir = os.path.join(os.getcwd(), 'caltables')
    calfiles = get_calfiles(visname, caldir)

    return calfiles, caldir




def get_field_ids(fields):
    """
    Given an input list of source names, finds the associated field
    IDS from the MS and returns them as a list.
    """

    targetfield    = fields['targetfields']
    fluxfield      = fields['fluxfield']
    bpassfield     = fields['bpassfield']
    secondaryfield = fields['phasecalfield']
    kcorrfield     = fields['phasecalfield']
    xdelfield      = fields['phasecalfield']
    dpolfield      = fields['phasecalfield']
    xpolfield      = fields['phasecalfield']

    if fluxfield != secondaryfield:
        gainfields = \
                str(fluxfield) + ',' + str(secondaryfield)
    else:
        gainfields = str(fluxfield)

    FieldIDs = namedtuple('FieldIDs', ['targetfield', 'fluxfield',
                    'bpassfield', 'secondaryfield', 'kcorrfield', 'xdelfield',
                    'dpolfield', 'xpolfield', 'gainfields'])

    return FieldIDs(targetfield, fluxfield, bpassfield, secondaryfield,
            kcorrfield, xdelfield, dpolfield, xpolfield, gainfields)


def check_file(filepath):

    # Python2 only has IOError, so define FileNotFound
    try:
        FileNotFoundError
    except NameError:
        FileNotFoundError = IOError

    if not os.path.exists(filepath):
        logger.error('Calibration table "{0}" was not written. Please check the CASA output and whether a solution was found.'.format(filepath))
        raise FileNotFoundError
    else:
        logger.info('Calibration table "{0}" successfully written.'.format(filepath))

def get_selfcal_params():

    #Flag for input errors
    exit = False

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])
    params = taskvals['selfcal']

    check_params = params.keys()
    check_params.pop(check_params.index('nloops'))
    check_params.pop(check_params.index('restart_no'))

    params['vis'] = taskvals['data']['vis']
    if 'loop' not in params:
        params['loop'] = 0
    else:
        check_params.pop(check_params.index('loop'))

    for arg in check_params:

        # Multiscale needs to be a list of lists (if specifying multiple scales)
        # or a simple list (if specifying a single scale). So make sure these two
        # cases are covered. Likewise for imsize.

        if arg in ['multiscale','imsize']:
            # Not a list of lists, so turn it into one of right length
            if type(params[arg]) is list and (len(params[arg]) == 0 or type(params[arg][0]) is not list):
                params[arg] = [params[arg],] * (params['nloops'] + 1)
            # Not a list at all, so put it into a list
            elif type(params[arg]) is not list:
                params[arg] = [[params[arg],],] * (params['nloops'] + 1)
            # A list of lists of length 1, so put into list of lists of right length
            elif type(params[arg]) is list and type(params[arg][0]) is list and len(params[arg]) == 1:
                params[arg] = [params[arg][0],] * (params['nloops'] + 1)
            if len(params[arg]) != params['nloops'] + 1:
                logger.error("Parameter '{0}' in '{1}' is the wrong length. It is {2} but must be a single value or equal to 'nloops' + 1 ({3}).".format(arg,args['config'],len(params[arg]),params['nloops']+1))
                exit = True

        else:
            if type(params[arg]) is not list:
                params[arg] = [params[arg]] * (params['nloops'] + 1)

            if arg == 'solint' and len(params[arg]) != params['nloops']:
                logger.error("Parameter 'solint' in '{0}' is the wrong length. It is {1} but must be a single value (not a list) or equal 'nloops' ({2}).".format(args['config'],len(params[arg]),params['nloops']))
                exit = True
            elif arg != 'solint' and len(params[arg]) != params['nloops'] + 1:
                logger.error("Parameter '{0}' in '{1}' is the wrong length. It is {2} but must be a single value (not a list) equal to 'nloops' + 1 ({3}).".format(arg,args['config'],len(params[arg]),params['nloops']+1))
                exit = True

    if exit:
        sys.exit(1)

    return args,params
