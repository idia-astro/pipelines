#!/usr/bin/python2.7
import sys
import os

import processMeerKAT
import config_parser

def get_fields(MS):

    """Extract field numbers from intent, including calibrators for bandpass, flux, phase & amplitude, and the target.
    All fields except the calibrator take the first field as the field ID.

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

    msmd.open(MS)

    fields = ['fluxfield','bpassfield','phasecalfield','targetfields']
    intents = ['CALIBRATE_FLUX','CALIBRATE_BANDPASS','CALIBRATE_PHASE','TARGET']
    fieldIDs = {}

    default_intent = msmd.fieldsforintent('CALIBRATE_FLUX')

    for i,intent in enumerate(intents):
        fieldID = msmd.fieldsforintent(intent)
        if fieldID.size > 0:
            fieldIDs[fields[i]] = "'{0}'".format(','.join([str(fieldID[j]) for j in range(fieldID.size)]))
        else:
            print 'Intent "{0}" not found. Setting {1}={2}'.format(intent,fields[i],default_intent)
            fieldIDs[fields[i]] = "'{0}'".format(default_intent)

    msmd.close()

    return fieldIDs

def check_refant(MS,refant,warn=True):

    """Check if reference antenna exists, otherwise throw an error or print a warning.

    Arguments:
    ----------
    MS : str
        Input measurement set (relative or absolute path).
    refant: str
        Input reference antenna.
    warn : bool, optional
        Warn the user? If False, raise ValueError."""

    msmd.open(MS)
    ants = msmd.antennanames()

    if refant not in ants:
        err = "Reference antenna '{0}' isn't present in input dataset '{1}'. Antennas present are: {2}".format(refant,MS,ants)
        if warn:
            print '### WARNING: {0}'.format(err)
        else:
            raise ValueError(err)

    msmd.close()

def main():

    args = processMeerKAT.parse_args()[0]
    refant = config_parser.parse_config(args.config)[0]['crosscal']['refant']
    check_refant(args.MS, refant, warn=True)

    fields = get_fields(args.MS)
    config_parser.overwrite_config(args.config, conf_dict=fields, conf_sec='fields')


if __name__ == "__main__":
    main()
