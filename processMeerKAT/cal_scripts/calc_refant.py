"""
Calculates the reference antenna
"""
import config_parser
from config_parser import validate_args as va
from cal_scripts import bookkeeping

import os
import numpy as np

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def get_ref_ant(visname, fluxfield):

    msmd.open(visname)
    fluxscans = msmd.scansforfield(int(fluxfield))
    logger.info("Flux field scan no: %d" % fluxscans[0])
    antennas = msmd.antennasforscan(fluxscans[0])
    msmd.done()

    header = '{0: <3} {1: <4} {2: <4}'.format('ant','median','rms')
    logger.info("Antenna statistics on total flux calibrator")
    logger.info("(flux in Jy averaged over scans & channels, and over all of each antenna's baselines)")
    logger.info(header)

    tb.open(visname)

    fptr = open('ant_stats.txt', 'w')
    fptr.write(header)

    antamp=[]; antrms = []
    for ant in antennas:
        antdat = tb.query('ANTENNA1==%d AND FIELD_ID==%d' % (ant, int(fluxfield))).getcol('DATA')
        antdat = np.abs(antdat)

        amp = np.median(antdat)
        rms = np.std(antdat)

        fptr.write('\n{0: <3} {1:.2f}   {2:.2f}'.format(ant, amp, rms))
        antamp.append(amp)
        antrms.append(rms)

    tb.close()
    tb.done()
    fptr.close()

    antamp = np.array(antamp)
    antrms = np.array(antrms)

    medamp = np.median(antamp)
    medrms = np.median(antrms)

    #iqramp = iqr(antamp)
    #iqrrms = iqr(antrms)
    lowamp = np.percentile(antamp, 5)
    highamp = np.percentile(antamp, 95)

    lowrms = np.percentile(antrms, 5)
    highrms = np.percentile(antrms, 95)

    logger.info('{0: <3} {1:.2f}  {2:.2f}'.format('All',medamp,medrms))

    goodrms=[]; goodamp=[]; goodant=[]
    badants = []
    for ii in range(len(antamp)):
        cond1 = antamp[ii] > lowamp
        cond1 = cond1 & (antamp[ii] < highamp)

        cond2 = antrms[ii] > lowrms
        cond2 = cond1 & (antrms[ii] < highrms)

        if cond1 and cond2:
            goodant.append(antennas[ii])
            goodamp.append(antamp[ii])
            goodrms.append(antrms[ii])
        else:
            badants.append(antennas[ii])

    goodrms = np.array(goodrms)
    jj = np.argmin(goodrms)

    logger.info('{0: <3} {1:.2f}  {2:.2f} (best antenna)'.format(goodant[jj], goodamp[jj], goodrms[jj]))
    logger.info('{0: <3} {1:.2f}  {2:.2f} (1st good antenna)'.format(goodant[0], goodamp[0], goodrms[0]))
    referenceant = str(goodant[jj])
    logger.info("setting reference antenna to: %s" % referenceant)

    logger.info("Bad antennas: {0}".format(badants))

    return referenceant, badants

def main():

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    visname = os.path.split(visname.replace('.ms', '.mms'))[1]

    fields = bookkeeping.get_field_ids(taskvals['fields'])
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool, default=False)

    # Calculate reference antenna
    if calcrefant:
        if len(fields.fluxfield.split(',')) > 1:
            field = fields.fluxfield.split(',')[0]
        else:
            field = fields.fluxfield

        refant, badants = get_ref_ant(visname, field)
        # Overwrite config file with new refant
        config_parser.overwrite_config(args['config'], conf_sec='crosscal', conf_dict={'refant':refant})
        config_parser.overwrite_config(args['config'], conf_sec='crosscal', conf_dict={'badants':badants})

if __name__ == '__main__':
    main()

