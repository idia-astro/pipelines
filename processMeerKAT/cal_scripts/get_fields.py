#Copyright (C) 2019 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

#!/usr/bin/env python2.7
from __future__ import print_function

import sys
import os

import processMeerKAT
import config_parser

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

# Get access to the msmd module for get_fields.py
import casac
msmd = casac.casac.msmetadata()

def get_fields(MS):

    """Extract field numbers from intent, including calibrators for bandpass, flux, phase & amplitude, and the target. Only the
    target allows for multiple field IDs, while all others extract the field with the most scans and put all other IDs as target fields.

    Arguments:
    ----------
    MS : str
        Input measurement set (relative or absolute path).

    Returns:
    --------
    fieldIDs : dict
        fluxfield : int
            Field for total flux calibration.
        bpassfield : int
            Field for bandpass calibration.
        phasecalfield : int
            Field for phase calibration.
        targetfields : int
            Target field."""

    fieldIDs = {}
    extra_fields = []

    #Set default for any missing intent as field for intent CALIBRATE_FLUX
    default = msmd.fieldsforintent('CALIBRATE_FLUX')[0]
    if default.size == 0:
        logger.error('You must have a field with intent "CALIBRATE_FLUX". I only found {0} in dataset "{1}".'.format(msmd.intents(),MS))
        return fieldIDs

    #Use 'CALIBRATE_PHASE' or if missing, 'CALIBRATE_AMPLI'
    phasecal_intent = 'CALIBRATE_PHASE'
    if phasecal_intent not in msmd.intents():
        phasecal_intent = 'CALIBRATE_AMPLI'

    fieldIDs['fluxfield'] = get_field(MS,'CALIBRATE_FLUX','fluxfield',extra_fields)
    fieldIDs['bpassfield'] = get_field(MS,'CALIBRATE_BANDPASS','bpassfield',extra_fields,default=default)
    fieldIDs['phasecalfield'] = get_field(MS,phasecal_intent,'phasecalfield',extra_fields,default=default)
    fieldIDs['targetfields'] = get_field(MS,'TARGET','targetfields',extra_fields,default=default,multiple=True)

    #Put any extra fields in target fields
    if len(extra_fields) > 0:
        fieldIDs['targetfields'] = "{0},{1}'".format(fieldIDs['targetfields'][:-1],','.join([str(extra_fields[i]) for i in range(len(extra_fields))]))

    return fieldIDs


def get_field(MS,intent,fieldname,extra_fields,default=0,multiple=False):

    """Extract field IDs based on intent. When multiple fields are present, if multiple is True, return a
    comma-seperated string, otherwise return a single field string corresponding to the field with the most scans.

    Arguments:
    ----------
    MS : str
        Input measurement set (relative or absolute path).
    intent : str
        Calibration intent.
    fieldname : str
        The name given by the pipeline to the field being extracted (for output).
    extra_fields : list
        List of extra fields (passed by reference).
    default : int, optional
        Default field to return if intent missing.
    multiple : bool, optional
        Allow multiple fields?

    Returns:
    --------
    fieldIDs : str
        Extracted field ID(s), comma-seperated for multiple fields."""

    fields = msmd.fieldsforintent(intent)

    if fields.size == 0:
        logger.warn('Intent "{0}" not found in dataset "{1}". Setting to "{2}"'.format(intent,MS,default))
        fieldIDs = "'{0}'".format(default)
    elif fields.size == 1:
        fieldIDs = "'{0}'".format(fields[0])
    else:
        logger.info('Multiple fields found with intent "{0}" in dataset "{1}" - {2}.'.format(intent,MS,fields))

        if multiple:
            logger.info('Will use all of them for "{0}".'.format(fieldname))
            fieldIDs = "'{0}'".format(','.join([str(fields[i]) for i in range(fields.size)]))
        else:
            maxfield, maxscan = 0, 0
            scans = [msmd.scansforfield(ff) for ff in fields]
            # scans is an array of arrays
            for ind, ss in enumerate(scans):
                if len(ss) > maxscan:
                    maxscan = len(ss)
                    maxfield = fields[ind]

            logger.warn('Only using field "{0}" for "{1}", which has the most scans ({2}).'.format(maxfield,fieldname,maxscan))
            fieldIDs = "'{0}'".format(maxfield)

            #Put any extra fields with intent CALIBRATE_BANDPASS in target field
            extras = list(set(fields) - set(extra_fields) - set([maxfield]))
            if len(extras) > 0:
               logger.warn('Putting extra fields with intent "{0}" in "targetfields" - {1}'.format(intent,extras))
               extra_fields += extras

    return fieldIDs

def check_refant(MS,refant,config,warn=True):

    """Check if reference antenna exists, otherwise throw an error or display a warning.

    Arguments:
    ----------
    MS : str
        Input measurement set (relative or absolute path).
    refant: str
        Input reference antenna.
    config : str
        Path to config file.
    warn : bool, optional
        Warn the user? If False, raise ValueError."""

    msmd.open(MS)
    if type(refant) is str:
        ants = msmd.antennanames()
    else:
        ants = msmd.antennaids()

    if refant not in ants:
        err = "Reference antenna '{0}' isn't present in input dataset '{1}'. Antennas present are: {2}. Try 'm052' or 'm005' if present.".format(refant,MS,ants)
        if warn:
            logger.warn(err)
        else:
            raise ValueError(err)
    else:
        logger.info("Using reference antenna '{0}'.".format(refant))
        if refant == 'm059':
            logger.info("This is usually a well-behaved (stable) antenna. Update 'refant' in [crosscal] section of '{0}' to change this.".format(config))

def check_scans(MS,nodes,tasks):

    """Check if the user has set the number of threads to a number larger than the number of scans.
    If so, display a warning and return the number of thread to be replaced in their config file.

    Arguments:
    ----------
    MS : str
        Input measurement set (relative or absolute path).
    nodes : int
        The number of nodes set by the user.
    tasks : int
        The number of tasks (per node) set by the user.

    Returns:
    --------
    threads : dict
        A dictionary with updated values for nodes and tasks per node to match the number of scans."""

    nscans = msmd.nscans()
    limit = int(1.1*(nscans/2 + 1))

    if abs(nodes * tasks - limit) > 0.1*limit:
        logger.warn('The number of threads ({0} node(s) x {1} task(s) = {2}) is not ideal compared to the number of scans ({3}) for "{4}".'.format(nodes,tasks,nodes*tasks,nscans,MS))

        #Start with eight tasks on one node, and increase count of nodes (and then tasks per node) until limit reached
        nodes = 1
        tasks = 8
        while nodes * tasks < limit:
            if nodes < processMeerKAT.TOTAL_NODES_LIMIT:
                nodes += 1
            elif tasks < processMeerKAT.NTASKS_PER_NODE_LIMIT:
                tasks += 1
            else:
                break

        logger.warn('Config file has been updated to use {0} node(s) and {1} task(s) per node.'.format(nodes,tasks))
        if nodes*tasks != limit:
            logger.info('For the best results, update your config file so that nodes x tasks per node = {0}.'.format(limit))

    threads = {'nodes' : nodes, 'ntasks_per_node' : tasks}
    return threads


def get_xy_field(visname, fields):
    """
    From the input MS determine which field should
    be used for XY-phase calibration (if required).

    In the following order :
    3C286
    3C138
    secondaryfield (nominally dpolfield)
    """

    msmd.open(visname)
    fieldnames = msmd.fieldnames()
    msmd.close()

    # Use 3C286 or 3C138 if present in the data
    calibrator_3C286 = set(["3C286", "1328+307", "1331+305", "J1331+3030"]).intersection(set(fieldnames))
    calibrator_3C138 = set(["3C138", "0518+165", "0521+166", "J0521+1638"]).intersection(set(fieldnames))

    if calibrator_3C286:
        xyfield = list(calibrator_3C286)[0]
    elif calibrator_3C138:
        xyfield = list(calibrator_3C138)[0]
    else:
        xyfield = fields.dpolfield

    return xyfield



def main():

    args = processMeerKAT.parse_args()
    msmd.open(args.MS)

    refant = config_parser.parse_config(args.config)[0]['crosscal']['refant']
    check_refant(args.MS, refant, args.config, warn=True)

    threads = check_scans(args.MS,args.nodes,args.ntasks_per_node)
    config_parser.overwrite_config(args.config, conf_dict=threads, conf_sec='slurm')

    fields = get_fields(args.MS)
    config_parser.overwrite_config(args.config, conf_dict=fields, conf_sec='fields')
    logger.info('[fields] section written to "{0}". Edit this section if you need to change field IDs (comma-seperated string for multiple IDs).'.format(args.config))
    msmd.done()

if __name__ == "__main__":
    main()
