import sys
import os

import config_parser
from config_parser import validate_args as va
import processMeerKAT
from cal_scripts import get_fields, bookkeeping

from scipy.stats import iqr

def get_ref_ant(visname, fluxfield):

    msmd.open(visname)
    fluxscans = msmd.scansforfield(int(fluxfield))
    print "Flux field scan no: %d" % fluxscans[0]
    antennas = msmd.antennasforscan(fluxscans[0])
    msmd.done()
    print "\n Antenna statistics on flux field"
    print " ant    median    rms"

    tb.open(visname)

    fptr = open('ant_stats.txt', 'w')

    antamp=[]; antrms = []
    for ant in antennas:
        antdat = tb.query('ANTENNA1==%d AND FIELD_ID==%d' % (ant, int(fluxfield))).getcol('DATA')
        antdat = np.abs(antdat)

        amp = np.median(antdat)
        rms = np.std(antdat)

        fptr.write('% 02d % 8.3f % 8.3f\n' % (ant, amp, rms))
        antamp.append(amp)
        antrms.append(rms)

    tb.close()
    tb.done()

    antamp = np.array(antamp)
    antrms = np.array(antrms)

    medamp = np.median(antamp)
    medrms = np.median(antrms)

    iqramp = iqr(antamp)
    iqrrms = iqr(antrms)

    print "Median: %8.3f  %9.3f" % (medamp,medrms)

    goodrms=[]; goodamp=[]; goodant=[]
    badants = []
    for ii in range(len(antamp)):
        cond1 = antamp[ii] > medamp - iqramp
        cond1 = cond1 & (antamp[ii] < medamp + iqramp)

        cond2 = antrms[ii] > medrms - iqrrms
        cond2 = cond1 & (antrms[ii] < medrms + iqrrms)

        if cond1 and cond2:
            goodant.append(antennas[ii])
            goodamp.append(antamp[ii])
            goodrms.append(antrms[ii])
        else:
            badants.append(antennas[ii])

    goodrms = np.array(goodrms)
    jj = np.argmin(goodrms)

    print "best antenna: %2s  amp = %7.2f, rms = %7.2f" % \
                                (goodant[jj], goodamp[jj], goodrms[jj])
    print "1st good antenna: %2s  amp = %7.2f, rms = %7.2f" % \
                                (goodant[0], goodamp[0], goodrms[0])
    referenceant = str(goodant[jj])
    print "setting reference antenna to: %s" % referenceant

    return referenceant, badants


def validateinput():
    """
    Parse the input config file (command line argument) and validate that the
    parameters look okay
    """

    print('This is version {0} of the pipeline'.format(processMeerKAT.__version__))

    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool)
    refant = va(taskvals, 'crosscal', 'refant', str)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    # Check if the reference antenna exists, and complain and quit if it doesn't
    if calcrefant:
        if len(fields.fluxfield.split(',')) > 1:
            field = fields.fluxfield.split(',')[0]
        else:
            field = fields.fluxfield

        refant, badants = get_ref_ant(visname, field)
        # Overwrite config file with new refant
        config_parser.overwrite_config(args['config'], conf_sec='crosscal', conf_dict={'refant':refant})
        config_parser.overwrite_config(args['config'], conf_sec='crosscal', conf_dict={'badants':badants})
    else:
        refant = va(taskvals, 'crosscal', 'refant', str)
        msmd.open(visname)
        get_fields.check_refant(MS=visname, refant=refant, warn=False)
        msmd.close()

    if not os.path.exists(visname):
        raise IOError("Path to MS %s not found" % (visname))


if __name__ == '__main__':
    validateinput()
