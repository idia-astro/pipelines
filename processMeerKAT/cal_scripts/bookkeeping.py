#Copyright (C) 2019 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys

import config_parser
from collections import namedtuple
import os

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

# Get access to the msmd module for get_fields.py
import casac
msmd = casac.casac.msmetadata()

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


def get_xy_field(visname):
    """
    From the input MS determine which field should
    be used for XY-phase calibration (if required).

    In the following order :
    3C286
    3C138
    secondaryfield
    """

    msmd.open(visname)
    fields = msmd.fieldnames()
    msmd.close()

    # Use 3C286 or 3C138 if present in the data
    calibrator_3C286 = set(["3C286", "1328+307", "1331+305", "J1331+3030"]).intersection(set(fields))
    calibrator_3C138 = set(["3C138", "0518+165", "0521+166", "J0521+1638"]).intersection(set(fields))

    if calibrator_3C286:
        xyfield = calibrator_3C286
    elif calibrator_3C138:
        xyfield = calibrator_3C138
    else:
        xyfield = fields.dpolfield

    return xyfield



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
