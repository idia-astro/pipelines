import numpy as np
import sys, os, shutil, glob
from recipes.almapolhelpers import *

from collections import namedtuple

sys.path.append(os.getcwd())

import config_parser

def get_field_ids(fields, visname):
    """
    Given an input list of source names, finds the associated field
    IDS from the MS and returns them as a list.
    """

    msmd.open(visname)

    fieldid = []
    for field in fields:
        idn = msmd.fieldsforname(field)

        if len(idn):
            fieldid.extend(idn)
        else:
            raise ValueError("Field %s not found in MS" % field)

    msmd.close()
    fieldid = [str(ff) for ff in fieldid]

    targetfield    = fieldid[3]         # Field ID of the target fields
    fluxfield      = fieldid[0]        # Field ID of the primary flux calibrator
    bpassfield     = fieldid[0]        # Field ID of the bandpass cal
    secondaryfield = fieldid[1]       # Field ID of the phase calibrator
    kcorrfield     = fieldid[1]       # Field ID of the antenna based delay cal
    xdelfield      = fieldid[2]      # Field ID of the cross-hand delay cal
    dpolfield      = fieldid[1]       # Field ID of the absolute pol angle cal
    xpolfield      = fieldid[2]      # Field ID of the pol leakage cal

    gainfields = \
            str(fluxfield) + ',' + str(secondaryfield)

    FieldIDs = namedtuple('FieldIDs', ['targetfield', 'fluxfield',
                    'bpassfield', 'secondaryfield', 'kcorrfield', 'xdelfield',
                    'dpolfield', 'xpolfield', 'gainfields'])

    return FieldIDs(targetfield, fluxfield, bpassfield, secondaryfield,
            kcorrfield, xdelfield, dpolfield, xpolfield, gainfields)



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
    print "Median: %8.3f  %9.3f" % (medamp,medrms)
    goodrms=[]; goodamp=[]; goodant=[]
    for i in range(len(antamp)):
        if (antamp[i] > medamp) and (antrms[i] < medrms):
            goodant.append(antennas[i])
            goodamp.append(antamp[i])
            goodrms.append(antrms[i])
    goodrms = np.array(goodrms)
    j = np.argmin(goodrms)

    print "best antenna: %2s  amp = %7.2f, rms = %7.2f" % \
                                (goodant[j], goodamp[j], goodrms[j])
    print "1st good antenna: %2s  amp = %7.2f, rms = %7.2f" % \
                                (goodant[0], goodamp[0], goodrms[0])
    referenceant = str(goodant[0])
    print "setting reference antenna to: %s" % referenceant

    return referenceant


def do_pre_flag(visname, spw, fields):
    clipfluxcal   = [0., 50.]
    clipphasecal  = [0., 50.]
    cliptarget    = [0., 20.]

    flagdata(vis=visname, mode='manual', autocorr=True, action='apply',
            flagbackup=True, savepars=False, writeflags=True)

    flagdata(vis=visname, mode="clip", spw=spw, field=fields.fluxfield,
            clipminmax=clipfluxcal, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip", spw=spw, field=fields.secondaryfield,
            clipminmax=clipphasecal, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='tfcrop', field=fields.gainfields, spw=spw,
            ntime='scan', timecutoff=5.0, freqcutoff=5.0, timefit='line',
            freqfit='line', extendflags=False, timedevscale=5., freqdevscale=5.,
            extendpols=True, growaround=False, action='apply', flagbackup=True,
            overwrite=True, writeflags=True, datacolumn='DATA')

    # Conservatively extend flags
    flagdata(vis=visname, mode='extend', spw=spw, field=fields.gainfields,
            datacolumn='data', clipzeros=True, ntime='scan', extendflags=False,
            extendpols=True, growtime=80., growfreq=80., growaround=False,
            flagneartime=False, flagnearfreq=False, action='apply',
            flagbackup=True, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip", spw=spw, field=fields.targetfield,
            clipminmax=cliptarget, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='tfcrop', field=fields.targetfield, spw=spw,
            ntime='scan', timecutoff=6.0, freqcutoff=6.0, timefit='poly',
            freqfit='poly', extendflags=False, timedevscale=5., freqdevscale=5.,
            extendpols=True, growaround=False, action='apply', flagbackup=True,
            overwrite=True, writeflags=True, datacolumn='DATA')

    flagdata(vis=visname, mode='extend', spw=spw, field=fields.targetfield,
            datacolumn='data', clipzeros=True, ntime='scan', extendflags=False,
            extendpols=True, growtime=80., growfreq=80., growaround=False,
            flagneartime=False, flagnearfreq=False, action='apply',
            flagbackup=True, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='summary', datacolumn='DATA',
            name=visname+'.flag.summary')


def do_pre_flag2(visname, spw, fields):
    clipfluxcal   = [0., 50.]
    clipphasecal  = [0., 50.]
    cliptarget    = [0., 20.]

    flagdata(vis=visname, mode="clip", spw = spw, field=fields.fluxfield,
            clipminmax=clipfluxcal, datacolumn="corrected", clipoutside=True,
            clipzeros=True, extendpols=False, action="apply", flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip", spw = spw,
            field=fields.secondaryfield, clipminmax=clipphasecal,
            datacolumn="corrected", clipoutside=True, clipzeros=True,
            extendpols=False, action="apply", flagbackup=True, savepars=False,
            overwrite=True, writeflags=True)

    # After clip, now flag using 'tfcrop' option for flux and phase cal tight
    # flagging
    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected",
            field=fields.gainfields, ntime="scan", timecutoff=6.0,
            freqcutoff=5.0, timefit="line", freqfit="line",
            flagdimension="freqtime", extendflags=False, timedevscale=5.0,
            freqdevscale=5.0, extendpols=False, growaround=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # now flag using 'rflag' option  for flux and phase cal tight flagging
    flagdata(vis=visname, mode="rflag", datacolumn="corrected",
            field=fields.gainfields, timecutoff=5.0, freqcutoff=5.0,
            timefit="poly", freqfit="line", flagdimension="freqtime",
            extendflags=False, timedevscale=4.0, freqdevscale=4.0,
            spectralmax=500.0, extendpols=False, growaround=False,
            flagneartime=False, flagnearfreq=False, action="apply",
            flagbackup=True, overwrite=True, writeflags=True)

    ## Now extend the flags (70% more means full flag, change if required)
    flagdata(vis=visname, mode="extend", spw = spw, field=fields.gainfields,
            datacolumn="corrected", clipzeros=True, ntime="scan",
            extendflags=False, extendpols=False, growtime=90.0, growfreq=90.0,
            growaround=False, flagneartime=False, flagnearfreq=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # Now flag for target - moderate flagging, more flagging in self-cal cycles
    flagdata(vis=visname, mode="clip", spw = spw, field=fields.targetfield,
            clipminmax=cliptarget, datacolumn="corrected", clipoutside=True,
            clipzeros=True, extendpols=False, action="apply", flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected",
            field=fields.targetfield, ntime="scan", timecutoff=6.0, freqcutoff=5.0,
            timefit="poly", freqfit="line", flagdimension="freqtime",
            extendflags=False, timedevscale=5.0, freqdevscale=5.0,
            extendpols=False, growaround=False, action="apply", flagbackup=True,
            overwrite=True, writeflags=True)

    # now flag using 'rflag' option
    flagdata(vis=visname, mode="rflag", datacolumn="corrected",
            field=fields.targetfield, timecutoff=5.0, freqcutoff=5.0, timefit="poly",
            freqfit="poly", flagdimension="freqtime", extendflags=False,
            timedevscale=5.0, freqdevscale=5.0, spectralmax=500.0,
            extendpols=False, growaround=False, flagneartime=False,
            flagnearfreq=False, action="apply", flagbackup=True, overwrite=True,
            writeflags=True)

    # Now summary
    flagdata(vis=visname, mode="summary", datacolumn="corrected",
            extendflags=True, name=visname + 'summary.split', action="apply",
            flagbackup=True, overwrite=True, writeflags=True)



def do_parallel_cal(visname, spw, fields, calfiles, referenceant,
        minbaselines, do_clearcal=False):
    if do_clearcal:
        clearcal(vis=visname)

    print " starting setjy for flux calibrator"
    setjy(vis=visname, field = fields.fluxfield, spw = spw, scalebychan=True,
            standard='Perley-Butler 2010')

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

    print " applying calibration -> primary calibrator"
    applycal(vis=visname, field=fields.fluxfield, spw = spw, selectdata=False,
            calwt=False, gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.fluxfile], gainfield=[fields.kcorrfield,
                    fields.bpassfield, fields.fluxfield], parang=True)

    print " applying calibration -> secondary calibrator"
    applycal(vis=visname, field=fields.secondaryfield, spw = spw,
            selectdata=False, calwt=False, gaintable=[calfiles.kcorrfile,
                calfiles.bpassfile, calfiles.fluxfile],
            gainfield=[fields.kcorrfield, fields.bpassfield,
                fields.secondaryfield], parang=True)

    print " applying calibration -> target calibrator"
    applycal(vis=visname, field=fields.targetfield, spw = spw, selectdata=False,
            calwt=False, gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.fluxfile], gainfield=[fields.kcorrfield,
                    fields.bpassfield, fields.secondaryfield], parang=True)



def do_cross_cal(visname, spw, fields, calfiles, referenceant, caldir,
        minbaselines, do_clearcal=False):

    if do_clearcal:
        clearcal(visname)

    print '\n\n ++++++ Linear Feed Polarization calibration ++++++'

    print " starting setjy for flux calibrator"
    setjy(vis=visname, field = fields.fluxfield, spw = spw, scalebychan=True,
            standard='Perley-Butler 2010')

    print " starting antenna-based delay (kcorr)\n -> %s" % calfiles.kcorrfile
    gaincal(vis=visname, caltable = calfiles.kcorrfile,
            field = fields.kcorrfield, spw = spw, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'K',
            solint = '10min', combine = '', parang = False, append = False)

    print " starting bandpass -> %s" % calfiles.bpassfile
    bandpass(vis=visname, caltable = calfiles.bpassfile,
            field = fields.bpassfield, spw = spw,
            refant = referenceant, minblperant = minbaselines, solnorm = True,
            solint = 'inf', combine = 'scan', bandtype = 'B', fillgaps = 8,
            gaintable = calfiles.kcorrfile, gainfield = fields.kcorrfield,
            parang = False, append = False)

    print " starting cross hand delay -> %s" % calfiles.xdelfile
    gaincal(vis=visname, caltable = calfiles.xdelfile, field = fields.xdelfield,
            spw = spw, refant = referenceant, smodel=[1., 0., 1., 0.],
            solint = 'inf', minblperant = minbaselines, gaintype = 'KCROSS',
            combine = 'scan',
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile],
            gainfield = [fields.kcorrfield, fields.bpassfield])

    base = visname.replace('.ms', '')
    gain1file   = os.path.join(caldir, base+'.g1cal')
    dtempfile   = os.path.join(caldir, base+'.dtempcal')
    xy0ambpfile = os.path.join(caldir, base+'.xyambcal')
    xy0pfile    = os.path.join(caldir, base+'.xycal')

    print " starting gaincal -> %s" % calfiles.gainfile
    gaincal(vis=visname, caltable=gain1file, field=fields.fluxfield, spw = spw,
            refant=referenceant, solint='1min', minblperant=minbaselines,
            solnorm=False, gaintype='G', gaintable=[calfiles.kcorrfile,
                calfiles.bpassfile, calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.xdelfield], append=False, parang=True)

    gaincal(vis=visname, caltable=gain1file, field=fields.secondaryfield,
            spw = spw, smodel=[1,0,0,0], refant=referenceant, solint='1min',
            minblperant=minbaselines, solnorm=False, gaintype='G',
            gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield,
                    fields.bpassfield, fields.xdelfield],
            append=True, parang=True)

    plotcal(caltable=gain1file, xaxis='time', yaxis='amp', poln='X',
            field=fields.secondaryfield, showgui=False,
            figfile = os.path.join(caldir,'initialgain.png'),
            markersize=3, plotsymbol='-', fontsize=8)

    # implied polarization from instrumental response
    print "\n Solve for Q, U from initial gain solution"
    GainQU = qufromgain(gain1file)
    print GainQU[int(fields.dpolfield)]

    print "\n Starting x-y phase calibration\n -> %s" % xy0ambpfile
    gaincal(vis=visname, caltable = xy0ambpfile, field = fields.dpolfield,
            spw = spw, refant = referenceant, solint = 'inf', combine = 'scan',
            gaintype = 'XYf+QU', minblperant = minbaselines,
            smodel = [1.,0.,1.,0.], preavg = 200.0,
            gaintable = [calfiles.kcorrfile,calfiles.bpassfile,
                gain1file, calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.secondaryfield, fields.xdelfield],
            append = False, parang = True)

    print "\n Check for x-y phase ambiguity."
    xyamb(xytab=xy0ambpfile, qu=GainQU[int(fields.dpolfield)], xyout = xy0pfile)

    S = [1.0, GainQU[int(fields.dpolfield)][0],
            GainQU[int(fields.dpolfield)][1], 0.0]

    p = np.sqrt(S[1]**2 + S[2]**2)
    print "Model for polarization calibrator S =", S
    print "Fractional polarization =", p

    gaincal(vis=visname, caltable = calfiles.gainfile, field = fields.fluxfield,
            spw = spw, refant = referenceant, solint = '5min', solnorm = False,
            gaintype = 'G', minblperant = minbaselines, combine = '',
            minsnr = 3, calmode = 'ap',
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield,fields.bpassfield,fields.xdelfield],
            parang = True, append = False)

    print "\n solution for secondary with parang = true"
    gaincal(vis=visname, caltable = calfiles.gainfile,
            field = fields.secondaryfield, spw = spw, refant = referenceant,
            solint = '5min', solnorm = False,  gaintype = 'G',
            minblperant = minbaselines,combine = '', smodel = S, minsnr = 3,
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield,fields.bpassfield,fields.xdelfield],
            append = True, parang = True)

    print "\n now re-solve for Q,U from the new gainfile\n -> %s" \
                                                        % calfiles.gainfile
    Gain2QU = qufromgain(calfiles.gainfile)
    print GainQU[int(fields.dpolfield)]

    print "starting \'Dflls\' polcal -> %s"  % calfiles.dpolfile
    polcal(vis=visname, caltable = dtempfile, field = fields.dpolfield,
            spw = spw, refant = '', solint = 'inf', combine = 'scan',
            poltype = 'Dflls', smodel = S, preavg= 200.0,
            gaintable = [calfiles.kcorrfile,calfiles.bpassfile,
                calfiles.gainfile, calfiles.xdelfile, xy0pfile],
           gainfield = [fields.kcorrfield, fields.bpassfield,
               fields.secondaryfield, fields.xdelfield, fields.dpolfield],
           append = False)

    Dgen(dtab=dtempfile, dout=calfiles.dpolfile)

    print " starting fluxscale -> %s", calfiles.fluxfile
    fluxscale(vis=visname, caltable = calfiles.gainfile,
            reference = fields.fluxfield, transfer = '',
            fluxtable = calfiles.fluxfile,
            listfile = os.path.join(caldir,'fluxscale.txt'), append = False)

    #---------------------------------------------------
    marktime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print "Calibration solutions complete: %s" % marktime
    print "Applying calibrations..."
    # ---------------------------------------------------

    calfiles = calfiles._replace(xpolfile=xy0pfile)
    fields = fields._replace(xpolfield=fields.dpolfield)

    print " applying calibrations: primary calibrator"
    applycal(vis=visname, field = fields.fluxfield, spw = spw,
            selectdata = False, calwt = True, gaintable = [calfiles.kcorrfile,
                calfiles.bpassfile, calfiles.fluxfile, calfiles.dpolfile,
                calfiles.xdelfile, calfiles.xpolfile],
        gainfield = [fields.kcorrfield,fields.bpassfield, fields.fluxfield,
            fields.dpolfield,fields.xdelfield, fields.xpolfield],
        parang = True)

    print " applying calibrations: polarization calibrator"
    applycal(vis=visname, field = fields.dpolfield, spw = spw,
            selectdata = False, calwt = True, gaintable = [calfiles.kcorrfile,
                calfiles.bpassfile, calfiles.fluxfile, calfiles.dpolfile,
                calfiles.xdelfile, calfiles.xpolfile],
        gainfield = [fields.kcorrfield,fields.bpassfield,fields.secondaryfield,
            fields.dpolfield,fields.xdelfield,fields.xpolfield],
        parang= True)

    print " applying calibrations: secondary calibrators"
    applycal(vis=visname, field = fields.secondaryfield, spw = spw,
            selectdata = False, calwt = True,
        gaintable = [calfiles.kcorrfile, calfiles.bpassfile, calfiles.fluxfile,
            calfiles.dpolfile, calfiles.xdelfile, calfiles.xpolfile],
        gainfield = [fields.kcorrfield, fields.bpassfield,
            fields.secondaryfield, fields.dpolfield, fields.xdelfield,
            fields.xpolfield],
        parang= True)

    print " applying calibrations: target fields"
    applycal(vis=visname, field = fields.targetfield, spw = spw,
            selectdata = False, calwt = True, gaintable = [calfiles.kcorrfile,
                calfiles.bpassfile, calfiles.fluxfile, calfiles.dpolfile,
                calfiles.xdelfile, calfiles.xpolfile],
        gainfield = [fields.kcorrfield, fields.bpassfield,
            fields.secondaryfield, fields.dpolfield, fields.xdelfield,
            fields.xpolfield],
        parang= True)

    marktime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print "Apply calibrations complete: %s" % marktime


def split_vis(visname, fields, specave, timeave):
    outputbase = visname.strip('.ms')
    split(vis=visname, outputvis = outputbase+'.'+fields.targetfield+'.ms',
            datacolumn='corrected', field = fields.targetfield, spw = spw,
            keepflags=True, width = specave, timebin = timeave)

    split(vis=visname, outputvis = outputbase+'.'+fields.secondaryfield+'.ms',
            datacolumn='corrected', field = fields.secondaryfield, spw = spw,
            keepflags=True, width = specave, timebin = timeave)


def get_calfiles(visname, caldir):
    base = visname.replace('.ms', '')
    kcorrfile = os.path.join(caldir,base + '.kcal')
    bpassfile = os.path.join(caldir,base + '.bcal')
    gainfile =  os.path.join(caldir,base + '.gcal')
    dpolfile =  os.path.join(caldir,base + '.pcal')
    xpolfile =  os.path.join(caldir,base + '.xcal')
    xdelfile =  os.path.join(caldir,base + '.xdel')
    fluxfile =  os.path.join(caldir,base + '.fluxscale')

    calfiles = namedtuple('calfiles',
            ['kcorrfile', 'bpassfile', 'gainfile', 'dpolfile', 'xpolfile',
                'xdelfile', 'fluxfile'])
    return calfiles(kcorrfile, bpassfile, gainfile, dpolfile, xpolfile,
            xdelfile, fluxfile)



def crosscal(visname, sources, **kwargs):
    """
    Run the cross-calibration routines for MeerKAT.

    Possible kwargs:
        minbaselines    int, Min. no. of baselines for calibration
        preflag         bool, flag before calibration
        specave         int, no of chans to average after split
        timeave         str with units, time in units to average after split

        selspw          str, spw to use for flagging + calibration
        calcrefant      bool, use internal logic to find refant
    """

    visname = visname

    minbaselines = kwargs.pop('minbaselines', 4)

    fluxcal = sources['fluxcal']
    phasecal = sources['phasecal']
    polangcal = sources['polangcal']
    target = sources['target']

    fields = get_field_ids([fluxcal, phasecal, polangcal, target], visname)

    preflag = kwargs.pop('preflag', True)

    if type(preflag) is not bool:
        raise ValueError('preflag must be a boolean value')

    # Averaging Parameters for split
    specave = kwargs.pop('specave', 2)
    timeave = kwargs.pop('timeave', '8s')

    spw             = kwargs.pop('spw', '0:860~1700MHz')
    badfreqranges   = \
            ['944~947MHz', '1160~1310MHz', '1476~1611MHz', '1670~1700MHz']
    calcrefant      = kwargs.pop('calcrefant', True)

    if calcrefant:
        referenceant = get_ref_ant(visname, fields.fluxfield)
    else:
        referenceant = kwargs.pop('referenceant', 'm005')

    # Book keeping
    workdir = os.path.join(os.getcwd(), 'pipeline')
    if not os.path.isdir(workdir):
        os.makedirs(workdir)

    procdir = os.path.join(workdir, 'processing/')
    if not os.path.isdir(procdir):
        os.makedirs(procdir)

    logdir = os.path.join(procdir, 'logs/')
    if not os.path.isdir(logdir):
        os.makedirs(logdir)

    caldir = os.path.join(procdir, 'calib_out/')
    calfiles = get_calfiles(visname, caldir)

    timestamp = time.strftime("%d%b%Y_%H%M%S", time.localtime())
    logfile =  logdir+'calib_'+timestamp+'.log'
    casalog.setlogfile(logfile)
    process_start = time.time()

    #----- remove any previous calibration output directories etc.
    try:
        if os.path.isdir(caldir):
            shutil.rmtree(caldir)
            print " deleting existing output directory: %s" % caldir
    except OSError:
        prin("output directory does not exist. Continuing")

    print " creating new output directory: %s\n" % caldir
    os.makedirs(caldir)

    mvis = visname.replace('.ms', '.mms')
    partition(vis=visname, outputvis=mvis, createmms=True, datacolumn='DATA')
    visname = mvis

    # Backup the original flags
    flagmanager(vis=visname, mode='save', versionname='orig',
            comment='original flags', merge='replace')

    # Flag out the contaminated frequencies
    if len(badfreqranges):
        for badfreq in badfreqranges:
            badspw = '0:' + badfreq
            flagdata(vis=visname, mode='manual', spw=badspw)

    clearcal(visname)

    #if preflag:
    #    do_pre_flag(visname, spw, fields)

    #do_parallel_cal(visname, spw, fields, calfiles, referenceant, minbaselines)

    #if preflag:
    #    do_pre_flag2(visname, spw, fields)

    do_cross_cal(visname, spw, fields, calfiles, referenceant, caldir,
            minbaselines, do_clearcal=True)

    split_vis(visname, fields, specave, timeave)

    marktime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime() )
    print "pipeline processing of field %s completed" % ms
    process_end = time.time()
    duration = (process_end - process_start)/3600.0
    print "Run time: %7.2f hours" % duration

    marktime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
    print "\n Pipeline processing completed at %s" % marktime

    process_end = time.time()
    duration = (process_end - process_start)/3600.0
    print " Total run time: %6.2f hours" % duration

    print "\n Cleaning up."
    for filename in os.listdir(workdir):
        if (filename.endswith('.last') or filename.endswith('.log')):
            os.remove(filename)

if __name__ == '__main__':
    sources = {}
    sources['fluxcal'] = 'J1939-6342'
    sources['phasecal'] = 'J0240-2309'
    sources['polangcal'] = 'J0240-2309'
    sources['target'] = 'CDFS16'

    taskvals, conf = config_parser.parse_config('default_config.txt')

    crosscal('cdfs16_raw_tiny.ms', sources, **taskvals['crosscal'])
