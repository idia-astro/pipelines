import sys

import config_parser
from config_parser import validate_args as va

from cal_scripts import bookkeeping

from scipy.stats import iqr

def get_ref_ant(visname, fluxfield):

    msmd.open(visname)
    fluxscans = msmd.scansforfield(int(fluxfield))
    print "Flux field scan no: %d" % fluxscans[0]
    antennas = msmd.antennasforscan(fluxscans[0])
    msmd.done()
    print "\n Antenna statistics on flux field"
    print " ant    median    rms"

    antamp=[]; antrms = []
    for ant in antennas:
        ant = str(ant)
        t = visstat(vis=visname, field=fluxfield, antenna=ant,
                timeaverage=True, timebin='500min', timespan='state,scan',
                reportingaxes='field')

        item = str(t.keys()[0])
        amp = float(t[item]['median'])
        rms = float(t[item]['rms'])
        print "%3s  %8.3f %9.3f " % (ant, amp, rms)
        antamp.append(amp)
        antrms.append(rms)

    antamp = np.array(antamp)
    antrms = np.array(antrms)

    medamp = np.median(antamp)
    medrms = np.median(antrms)

    iqramp = iqr(antamp)
    iqrrms = iqr(antrms)

    print "Median: %8.3f  %9.3f" % (medamp,medrms)

    goodrms=[]; goodamp=[]; goodant=[]
    for ii in range(len(antamp)):
        cond1 = antamp[ii] > medamp - iqramp
        cond1 = cond1 & (antamp[ii] < medamp + iqramp)

        cond2 = antrms[ii] > medrms - iqrrms
        cond2 = cond1 & (antrms[ii] < medrms + iqrrms)

        if cond1 and cond2:
            goodant.append(antennas[ii])
            goodamp.append(antamp[ii])
            goodrms.append(antrms[ii])

    goodrms = np.array(goodrms)
    jj = np.argmin(goodrms)

    print "best antenna: %2s  amp = %7.2f, rms = %7.2f" % \
                                (goodant[jj], goodamp[jj], goodrms[jj])
    print "1st good antenna: %2s  amp = %7.2f, rms = %7.2f" % \
                                (goodant[0], goodamp[0], goodrms[0])
    referenceant = str(goodant[jj])
    print "setting reference antenna to: %s" % referenceant

    return referenceant

if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)
    visname = visname.replace('.ms', '.mms')

    fields = bookkeeping.get_field_ids(taskvals['fields'])

    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool, default=False)

    if calcrefant:
        if len(fields.fluxfield.split(',')) > 1:
            field = fields.fluxfield.split(',')[0]
        else:
            field = fields.fluxfield

        refant = get_ref_ant(visname, field)
        config_parser.overwrite_config(args['config'], {'refant':refant}, 'crosscal')
