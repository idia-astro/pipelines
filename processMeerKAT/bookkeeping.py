#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

#!/usr/bin/env python2.7

import sys
import traceback

import config_parser
from collections import namedtuple
import os

import logging
from time import gmtime
logging.Formatter.converter = gmtime
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
    extrafields    = fields['extrafields']
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
                    'dpolfield', 'xpolfield', 'gainfields', 'extrafields'])

    return FieldIDs(targetfield, fluxfield, bpassfield, secondaryfield,
            kcorrfield, xdelfield, dpolfield, xpolfield, gainfields, extrafields)

def polfield_name(visname):

    msmd.open(visname)
    fieldnames = msmd.fieldnames()
    msmd.done()

    polfield = ''
    if any([ff in ["3C286", "1328+307", "1331+305", "J1331+3030"] for ff in fieldnames]):
        polfield= list(set(["3C286", "1328+307", "1331+305", "J1331+3030"]).intersection(set(fieldnames)))[0]
    elif any([ff in ["3C138", "0518+165", "0521+166", "J0521+1638"] for ff in fieldnames]):
        polfield = list(set(["3C138", "0518+165", "0521+166", "J0521+1638"]).intersection(set(fieldnames)))[0]
    else:
        logger.warning("No valid polarization field found. Defaulting to use the phase calibrator to solve for XY phase.")
        logger.warning("The polarization solutions found will likely be wrong. Please check the results carefully.")

    return polfield

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
    params['refant'] = taskvals['crosscal']['refant']
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
                if arg in ['solint','calmode']:
                    params[arg] = [params[arg]] * (params['nloops'])
                else:
                    params[arg] = [params[arg]] * (params['nloops'] + 1)

            if arg in ['solint','calmode'] and len(params[arg]) != params['nloops']:
                logger.error("Parameter '{0}' in '{1}' is the wrong length. It is {2} long but must be 'nloops' ({3}) long or a single value (not a list).".format(arg,args['config'],len(params[arg]),params['nloops']))
                exit = True
            elif arg not in ['solint','calmode'] and len(params[arg]) != params['nloops'] + 1:
                logger.error("Parameter '{0}' in '{1}' is the wrong length. It is {2} long but must 'nloops' + 1 ({3}) long or a single value (not a list).".format(arg,args['config'],len(params[arg]),params['nloops']+1))
                exit = True

    if exit:
        sys.exit(1)

    return args,params

def run_script(func):

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    continue_run = config_parser.validate_args(taskvals, 'run', 'continue', bool, default=True)
    spw = config_parser.validate_args(taskvals, 'crosscal', 'spw', str)
    nspw = config_parser.validate_args(taskvals, 'crosscal', 'nspw', int)

    if continue_run:
        try:
            func(args,taskvals)
        except Exception as err:
            logger.error('Exception found in the pipeline of type {0}: {1}'.format(type(err),err))
            logger.error(traceback.format_exc())
            config_parser.overwrite_config(args['config'], conf_dict={'continue' : False}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
            if nspw > 1:
                for SPW in spw.split(','):
                    spw_config = '{0}/{1}'.format(SPW.replace('0:',''),args['config'])
                    config_parser.overwrite_config(spw_config, conf_dict={'continue' : False}, conf_sec='run', sec_comment='# Internal variables for pipeline execution')
            sys.exit(1)
    else:
        logger.error('Exception found in previous pipeline job, which set "continue=False" in [run] section of "{0}". Skipping "{1}".'.format(args['config'],os.path.split(sys.argv[2])[1]))
        #os.system('./killJobs.sh') # and cancelling remaining jobs (scanel not found since /opt overwritten)
        sys.exit(1)
