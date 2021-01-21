#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

"""
Calculates the reference antenna
"""
import config_parser
from config_parser import validate_args as va
import bookkeeping

import os
import numpy as np

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
from casatools import msmetadata
msmd = msmetadata()

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def get_ref_ant(visname, fluxfield):

    msmd.open(visname)
    fluxscans = msmd.scansforfield(int(fluxfield))
    logger.info("Flux field scan no: %d" % fluxscans[0])
    antennas = msmd.antennasforscan(fluxscans[0])

    header = '{0: <3} {1: <4}'.format('ant', 'flags')
    logger.info("Antenna statistics on total flux calibrator")
    logger.info(header)

    tb.open(visname)

    fptr = open('ant_stats.txt', 'w')
    fptr.write(header)

    antflags = []
    for ant in antennas:
        antdat = tb.query('ANTENNA1==%d AND FIELD_ID==%d' % (ant, int(fluxfield))).getcol('FLAG')

        if antdat.size == 0:
            flags = 1
            fptr.write('{0: <3} {1:.4f}\n'.format(ant, np.nan))
            antflags.append(flags)
            continue

        flags = np.count_nonzero(antdat)/float(antdat.size)

        fptr.write('\n{0: <3} {1:.4f}'.format(ant, flags))
        logger.info('{0: <3} {1:.4f}'.format(ant, flags))
        antflags.append(flags)

    tb.close()
    tb.done()
    fptr.close()

    badants = []
    for idx, ant in enumerate(antflags):
        if ant > 0.8:
            badants.append(antennas[idx])

    refidx = np.argmin(antflags)

    logger.info('{0: <3} {1:.4f} (best antenna)'.format(antennas[refidx], antflags[refidx]))
    referenceant = msmd.antennastations(refidx)[0] # or msmd.antennanames(np.where(msmd.antennaids() == refidx)[0][0])[0]
    logger.info("setting reference antenna to: %s" % referenceant)

    logger.info("Bad antennas: {0}".format(badants))
    msmd.done()

    return referenceant, badants

def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)
    fields = bookkeeping.get_field_ids(taskvals['fields'])
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool)
    spw = va(taskvals, 'crosscal', 'spw', str)
    nspw = va(taskvals, 'crosscal', 'nspw', int)

    # Calculate reference antenna
    if calcrefant:
        if len(fields.fluxfield.split(',')) > 1:
            field = fields.fluxfield.split(',')[0]
        else:
            field = fields.fluxfield

        refant, badants = get_ref_ant(visname, field)
        # Overwrite config file with new refant
        config_parser.overwrite_config(args['config'], conf_sec='crosscal', conf_dict={'refant' : "'{0}'".format(refant)})
        config_parser.overwrite_config(args['config'], conf_sec='crosscal', conf_dict={'badants' : badants})

        #Replace reference antenna in each SPW config
        if nspw > 1:
            for SPW in spw.split(','):
                spw_config = '{0}/{1}'.format(SPW.replace('0:',''),args['config'])
                # Overwrite config file with new refant
                config_parser.overwrite_config(spw_config, conf_sec='crosscal', conf_dict={'refant' : "'{0}'".format(refant)})
                config_parser.overwrite_config(spw_config, conf_sec='crosscal', conf_dict={'badants' : badants})
                config_parser.overwrite_config(spw_config, conf_sec='crosscal', conf_dict={'calcrefant' : False})
    else:
        logger.info("Skipping calculation of reference antenna, as 'calcrefant=False' in '{0}'.".format(args['config']))

if __name__ == '__main__':

    bookkeeping.run_script(main,logfile)
