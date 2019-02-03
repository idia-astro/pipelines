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

    """Extract field numbers from intent, including calibrators for bandpass, flux, phase & amplitude, and the target.
    All fields except the total flux calibrator allow for multiple field IDs.

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

    #Set default for any missing intent as field for intent CALIBRATE_FLUX
    default = msmd.fieldsforintent('CALIBRATE_FLUX')
    if default.size == 0:
        raise KeyError('You must have a field with intent "CALIBRATE_FLUX". I only found {0} in dataset "{1}".'.format(msmd.intents(),MS))

    #Use 'CALIBRATE_PHASE' or if missing, 'CALIBRATE_AMPLI'
    phasecal_intent = 'CALIBRATE_PHASE'
    if phasecal_intent not in msmd.intents():
        phasecal_intent = 'CALIBRATE_AMPLI'

    #Put any extra fields from 'CALIBRATE_FLUX' in phasecal fields
    fieldIDs['fluxfield'] = get_field(MS,'CALIBRATE_FLUX',multiple=False)
    #extra_fields = list(set(default[1:]) - set(msmd.fieldsforintent(phasecal_intent)))
    #if len(extra_fields) > 0:
    #    logger.warn('Putting extra fields with intent "CALIBRATE_FLUX" in "phasecalfield"')

    fieldIDs['bpassfield'] = get_field(MS,'CALIBRATE_BANDPASS',default=default)
    fieldIDs['phasecalfield'] = get_field(MS,phasecal_intent,default=default, multiple=False)
    fieldIDs['targetfields'] = get_field(MS,'TARGET',default=default)

    #Put any extra fields with intent CALIBRATE_BANDPASS in phasecal field
    #if len(extra_fields) > 0:
    #    fieldIDs['phasecalfield'] = "{0},{1}'".format(fieldIDs['phasecalfield'][:-1],','.join([str(extra_fields[i]) for i in range(len(extra_fields))]))

    return fieldIDs


def get_field(MS,intent,default=0,multiple=True):

    """Extract a field ID based on intent. If multiple is True, return comma-seperated string, otherwise single field string.

    Arguments:
    ----------
    MS : str
        Input measurement set (relative or absolute path).
    intent : str
        Calibration intent.
    default : int, optional
        Default field to return if intent missing.
    multiple : bool, optional
        Allow multiple fields?

    Returns:
    --------
    fields : str
        Extracted field ID(s)"""

    fields = msmd.fieldsforintent(intent)

    maxfield, maxscan = 0, 0
    if fields.size > 1 and not multiple:
        scans = [msmd.scansforfield(ff) for ff in fields]
        # scans is an array of arrays
        for ind, ss in enumerate(scans):
            if len(ss) > maxscan:
                maxscan = len(ss)
                maxfield = fields[ind]
    else:
        maxfield = fields[0]

    if fields.size == 0:
        logger.warn('Intent "{0}" not found in dataset "{1}". Setting to "{2}"'.format(intent,MS,default))
        fields = "'{0}'".format(default)
    elif fields.size > 1:
        if not multiple:
            logger.warn('Multiple fields found with intent "{0}" in dataset "{1}" - {2}. Only using field "{3}".'.format(intent,MS,fields,maxfield))
        else:
            logger.info('Multiple fields found with intent "{0}" in dataset "{1}" - {2}. Will use all of them.'.format(intent,MS,fields))

    if multiple:
        fields = "'{0}'".format(','.join([str(fields[i]) for i in range(fields.size)]))
    else:
        fields = "'{0}'".format(maxfield)

    return fields

def check_refant(MS,refant,warn=True):

    """Check if reference antenna exists, otherwise throw an error or display a warning.

    Arguments:
    ----------
    MS : str
        Input measurement set (relative or absolute path).
    refant: str
        Input reference antenna.
    warn : bool, optional
        Warn the user? If False, raise ValueError."""

    ants = msmd.antennanames()

    if refant not in ants:
        err = "Reference antenna '{0}' isn't present in input dataset '{1}'. Antennas present are: {2}".format(refant,MS,ants)
        if warn:
            logger.warn(err)
        else:
            raise ValueError(err)

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

        #Start with one node, and increase count of nodes (and then tasks per node) until limit reached
        nodes = 1
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


def main():

    args = processMeerKAT.parse_args()
    msmd.open(args.MS)

    refant = config_parser.parse_config(args.config)[0]['crosscal']['refant']
    check_refant(args.MS, refant, warn=True)

    fields = get_fields(args.MS)
    config_parser.overwrite_config(args.config, conf_dict=fields, conf_sec='fields')
    logger.info('Field IDs written to "{0}". Edit this file to change any field IDs (comma-seperated string for multiple IDs).'.format(args.config))

    threads = check_scans(args.MS,args.nodes,args.ntasks_per_node)
    config_parser.overwrite_config(args.config, conf_dict=threads, conf_sec='slurm')

    msmd.close()
    msmd.done()

if __name__ == "__main__":
    main()
