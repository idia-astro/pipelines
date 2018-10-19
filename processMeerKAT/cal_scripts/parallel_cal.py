import sys

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va

from scipy.stats import iqr

def do_parallel_cal(visname, spw, fields, calfiles, referenceant,
        minbaselines, standard, do_clearcal=False):
    if do_clearcal:
        clearcal(vis=visname)

    print " starting setjy for flux calibrator"
    setjy(vis=visname, field = fields.fluxfield, spw = spw, scalebychan=True,
            standard=standard)

    print " starting antenna-based delay (kcorr)\n -> %s" % calfiles.kcorrfile
    gaincal(vis=visname, caltable = calfiles.kcorrfile, field
            = fields.kcorrfield, spw = spw, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'K',
            solint = 'inf', combine = '', parang = False, append = False)

    print " starting bandpass -> %s" % calfiles.bpassfile
    bandpass(vis=visname, caltable = calfiles.bpassfile,
            field = fields.bpassfield, spw = spw, refant = referenceant,
            minblperant = minbaselines, solnorm = True,  solint = 'inf',
            combine = 'scan', bandtype = 'B', fillgaps = 8,
            gaintable = calfiles.kcorrfile, gainfield = fields.kcorrfield,
            parang = False, append = False)

    print " starting gain calibration\n -> %s" % calfiles.gainfile
    gaincal(vis=visname, caltable = calfiles.gainfile,
            field = fields.gainfields, spw = spw, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'G',
            solint = 'inf', combine = '', calmode='ap',
            gaintable=[calfiles.kcorrfile, calfiles.bpassfile],
            gainfield=[fields.kcorrfield, fields.bpassfield],
            parang = False, append = False)

    fluxscale(vis=visname, caltable=calfiles.gainfile,
            reference=[fields.fluxfield], transfer='',
            fluxtable=calfiles.fluxfile, append=False)


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

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    calcrefant = va(taskvals, 'crosscal', 'calcrefant', bool, default=False)
    if calcrefant:
        if len(fields.fluxfield.split(',')) > 1:
            field = fields.fluxfield.split(',')[0]
        else:
            field = fields.fluxfield

        refant = get_ref_ant(visname, field)
        # Overwrite config file with new refant
        config_parser.overwrite_config(args['config'], {'refant':refant}, 'crosscal')
    else:
        refant = va(taskvals, 'crosscal', 'refant', str, default='m005')

    spw = va(taskvals, 'crosscal', 'spw', str, default='')
    minbaselines = va(taskvals, 'crosscal', 'minbaselines', int, default=4)
    standard = va(taskvals, 'crosscal', 'standard', str, default='Perley-Butler 2010')

    do_parallel_cal(visname, spw, fields, calfiles, refant,
            minbaselines, standard, do_clearcal=True)
