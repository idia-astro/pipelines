#Copyright (C) 2019 Inter-University Institute for Data Intensive Astronomy
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
    if not os.path.isdir(caldir):
        os.makedirs(caldir)

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

    if not os.path.exists(filepath):
        logger.error('Calibration table "{0}" was not written. Please check the CASA output and whether a solution was found.'.format(filepath))
        raise FileNotFoundError
    else:
        logger.info('Calibration table "{0}" successfully written.')