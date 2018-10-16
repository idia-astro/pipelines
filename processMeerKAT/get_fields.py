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
    my_intents : dict
        Dictionary of assumed intents for this observation. If any doesn't exist, the function will return None.

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

    return fieldIDs

def main():

    args = processMeerKAT.parse_args()[0]
    fields = get_fields(args.MS)
    config_parser.overwrite_config(args.config,additional_dict=fields,additional_sec='fields')


if __name__ == "__main__":
    main()
