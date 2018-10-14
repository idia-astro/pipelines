#!/usr/bin/python2.7
import sys
import os

sys.path.append('/data/users/krishna/pipeline/processMeerKAT/processMeerKAT')

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

    for i,intent in enumerate(intents):
        fieldIDs[fields[i]] = "'{0}'".format(msmd.fieldsforintent(intent)[0])

    return fieldIDs


if __name__ == "__main__":

    args = processMeerKAT.parse_args()[0]
    fields = get_fields(args.MS)
    config_parser.overwrite_config(args.config,additional_dict=fields,additional_sec='fields')
    config_parser.overwrite_config(args.config,additional_dict={'vis' : "'{0}'".format(args.MS)},additional_sec='data')

