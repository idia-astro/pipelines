import os, time, sys, string, shutil
import numpy as np
from collections import namedtuple
from recipes.almapolhelpers import *

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

    FieldIDs = namedtuple('FieldIDs', ['flux', 'phase', 'pol', 'target'])
    return FieldIDs(fieldid[0], fieldid[1], fieldid[2], fieldid[3])



visname = 'CDFS16_1_avg.ms'
base = 'cdfs16_1_avg'
dopolcal = 1  # Do polarisation calibration or no
doplotcal = 0
telescope = 'meerkat'
polnbasis = 'linear'
minbaselines = 4

fluxcal = 'J1939-6342'
phasecal = 'J0240-2309'
polangcal = 'J0240-2309'
target = 'CDFS16'

fields = get_field_ids([fluxcal, phasecal, polangcal, target], visname)

targetfield    = fields.target  # Field ID of the target fields
fluxfield      = fields.flux    # Field ID of the primary flux calibrator
bpassfield     = fields.flux    # Field ID of the source used for bandpass cal
secondaryfield = fields.phase   # Field ID of the phase calibrator
kcorrfield     = fields.phase   # Field ID of the antenna based delay calibrator
xdelfield      = fields.pol     # Field ID of the cross-hand delay calibrator
dpolfield      = fields.phase   # Field ID of the absolute pol angle calibrator
xpolfield      = fields.pol     # Field ID of the pol leakage calibrator

gainfields = str(fluxfield) + ',' + str(secondaryfield) + ',' + str(dpolfield)
#bpassfield = gainfields

preflag = True                # flag the data before calibration
postflag = True               # flag the data again after calibration

# Averaging Parameters for split
specave = 2                        # number of frequency channels to average when splitting
timeave = '8s'                     # time averaging interval in split

# Min/max levels in Jy for a blanket clip over the MS
clipfluxcal   = [0., 50.]
clipphasecal  = [0., 50.]
cliptarget    = [0., 20.]
clipresid     = [0., 10.]

# According to Perley et al 2013 integrated polarization of 3C138 is 5.6% a 1050 MHz angle -14 degrees.
# The angle does not appear to vary with frequency, so RM = 0.  Fractional polarization is decreasing
# with frequency from 9% at 1.95 GHz, 8.4% at 1.64 GHz and 5.6% at 1.05 GHz. So at 0.6 GHz extrapolation
# would suggest something like 3%.  Flux density at 0.6 GHz should be about 10 Jy.  So

if telescope.lower() == 'meerkat':
    gainchannels    = '50~1300'
    splitchannels   = '50~1300'
    flagchannels    = '50~1300'
    badfreqranges   =['944~947MHz', '1160~1310MHz', '1476~1611MHz', '1670~1700MHz']
    referenceant    ='m005'

elif telescope.lower() == 'gmrt':
    gainchannels    = '20~200'       # channel range to use for time-dependent gain calibration
    splitchannels   = '20~200'      # channels to split for imaging
    flagchannels    = '20~200'
    badfreqranges   = []
    PrimaryPolModel = {'3C286':[21.069,0.210,0.471,0.0],
                        '3C138':[10.0,0.2782,-0.1124,0.0]}
    referenceant    ='C00'
else:
    gainchannels    = ''
    splitchannels   = ''
    flagchannels    = ''
    badfreqranges   = []
    PrimaryPolModel = {'3C286':[21.069,0.210,0.471,0.0],
                        '3C138':[10.0,0.2782,-0.1124,0.0]}
    referenceant    ='0'

splitspw    = '0:' + splitchannels
gainspw     = '0:' + gainchannels
flagspw     = '0:' + flagchannels

mvis = visname.replace('.ms', '.mms')
partition(vis=visname, outputvis=mvis, createmms=True, datacolumn='DATA')
visname = mvis

clearcal(visname)

# Backup the original flags
flagmanager(vis=visname, mode='save', versionname='orig', comment='original flags',
        merge='replace')

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
    t = visstat2(vis=visname, field=fluxfield, antenna=ant, timeaverage=True, timebin='500min',
                 timespan='state,scan',reportingaxes='field')
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

print "best antenna: %2s  amp = %7.2f, rms = %7.2f" % (goodant[j], goodamp[j], goodrms[j])
print "1st good antenna: %2s  amp = %7.2f, rms = %7.2f" % (goodant[0], goodamp[0], goodrms[0])
referenceant = str(goodant[0])
print "setting reference antenna to: %s" % referenceant


# Flag out the contaminated frequencies
if len(badfreqranges):
    for badfreq in badfreqranges:
        badspw = '0:' + badfreq
        flagdata(vis=visname, mode='manual', spw=badspw)

flagdata(vis=visname, mode='manual',
            antenna = '20,49', action='apply', flagbackup=True,
            savepars=False, writeflags=True)

if preflag:
    flagdata(vis=visname, mode='manual', autocorr=True, action='apply',
            flagbackup=True, savepars=False, writeflags=True)

    flagdata(vis=visname, mode="clip", spw=flagspw, field=fluxfield,
            clipminmax=clipfluxcal, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip", spw=flagspw, field=secondaryfield,
            clipminmax=clipphasecal, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='tfcrop', field=gainfields, spw=flagspw, ntime='scan',
            timecutoff=5.0, freqcutoff=5.0, timefit='line', freqfit='line',
            extendflags=False, timedevscale=5., freqdevscale=5., extendpols=True,
            growaround=False, action='apply', flagbackup=True, overwrite=True,
            writeflags=True, datacolumn='DATA')

    # Conservatively extend flags
    flagdata(vis=visname, mode='extend', spw=flagspw, field=gainfields,
            datacolumn='data', clipzeros=True, ntime='scan', extendflags=False,
            extendpols=True, growtime=80., growfreq=80., growaround=False,
            flagneartime=False, flagnearfreq=False, action='apply', flagbackup=True,
            overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip", spw=flagspw, field=targetfield,
            clipminmax=cliptarget, datacolumn="DATA",clipoutside=True,
            clipzeros=True, extendpols=True, action="apply",flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='tfcrop', field=targetfield, spw=flagspw, ntime='scan',
            timecutoff=6.0, freqcutoff=6.0, timefit='poly', freqfit='poly',
            extendflags=False, timedevscale=5., freqdevscale=5., extendpols=True,
            growaround=False, action='apply', flagbackup=True, overwrite=True,
            writeflags=True, datacolumn='DATA')

    flagdata(vis=visname, mode='extend', spw=flagspw, field=targetfield,
            datacolumn='data', clipzeros=True, ntime='scan', extendflags=False,
            extendpols=True, growtime=80., growfreq=80., growaround=False,
            flagneartime=False, flagnearfreq=False, action='apply', flagbackup=True,
            overwrite=True, writeflags=True)

    flagdata(vis=visname, mode='summary', datacolumn='DATA', name=visname+'.flag.summary')


clearcal(visname)
workdir = os.path.join(os.getcwd(), base)
workdir = os.path.join(workdir, 'pipeline')
if not os.path.isdir(workdir):
    os.makedirs(workdir)

procdir = os.path.join(workdir, 'processing/')
if not os.path.isdir(procdir):
    os.makedirs(procdir)

logdir = os.path.join(procdir, 'logs/')
if not os.path.isdir(logdir):
    os.makedirs(logdir)

timestamp = time.strftime("%d%b%Y_%H%M%S", time.localtime())
logfile =  logdir+'calib_'+timestamp+'.log'
casalog.setlogfile(logfile)
process_start = time.time()

caldir = os.path.join(procdir, 'calib_out/')

#----- remove any previous calibration output directories and split measurements sets.
try:
    shutil.rmtree(caldir)
    print " deleting existing output directory: %s" % caldir
except: print " output directory does not exist"

print " creating new output directory: %s\n" % caldir
os.makedirs(caldir)

kcorrfile = caldir+base + '.kcal'
bpassfile = caldir+base + '.bcal'
gainfile =  caldir+base + '.gcal'
dpolfile =  caldir+base + '.pcal'
xpolfile =  caldir+base + '.xcal'
xdelfile =  caldir+base + '.xdel'
fluxfile =  caldir+base + '.fluxscale'

# --- Initial calibration - no polarisation. Only to identify outliers

print " starting setjy for flux calibrator"
setjy(vis=visname, field = fluxfield, spw = gainspw, scalebychan=True, standard='Perley-Butler 2010')

print " starting antenna-based delay (kcorr)\n -> %s" % kcorrfile
gaincal(vis=visname, caltable = kcorrfile, field = kcorrfield, spw = gainspw,
        refant = referenceant,  minblperant = minbaselines, solnorm = False,  gaintype = 'K',
        solint = 'inf', combine = '', parang = False, append = False)

print " starting bandpass -> %s" % bpassfile
bandpass(vis=visname, caltable = bpassfile, field = bpassfield, spw = gainspw,
        refant = referenceant, minblperant = minbaselines, solnorm = True,  solint = 'inf',
        combine = 'scan', bandtype = 'B', fillgaps = 8, gaintable = kcorrfile,
        gainfield = kcorrfield, parang = False, append = False)

print " starting gain calibration\n -> %s" % kcorrfile
gaincal(vis=visname, caltable = gainfile, field = gainfields, spw = gainspw,
        refant = referenceant,  minblperant = minbaselines, solnorm = False,  gaintype = 'G',
        solint = 'inf', combine = '', calmode='ap',
        gaintable=[kcorrfile, bpassfile], gainfield=[kcorrfield, bpassfield],
        parang = False, append = False)

fluxscale(vis=visname, caltable=gainfile, reference=[fluxfield], transfer='',
        fluxtable=fluxfile, append=False)

print " applying calibration -> primary calibrator"
applycal(vis=visname, field=fluxfield, spw = gainspw, selectdata=False, calwt=False,
        gaintable=[kcorrfile, bpassfile, fluxfile], gainfield=[kcorrfield,
            bpassfield, fluxfield], parang=True)

print " applying calibration -> secondary calibrator"
applycal(vis=visname, field=secondaryfield, spw = gainspw, selectdata=False, calwt=False,
        gaintable=[kcorrfile, bpassfile, fluxfile], gainfield=[kcorrfield,
            bpassfield, secondaryfield], parang=True)

print " applying calibration -> target calibrator"
applycal(vis=visname, field=target, spw = gainspw, selectdata=False, calwt=False,
        gaintable=[kcorrfile, bpassfile, fluxfile], gainfield=[kcorrfield,
            bpassfield, secondaryfield], parang=True)

if preflag:
    default(flagdata)

    flagdata(vis=visname, mode="clip", spw = gainspw, field=fluxfield,
            clipminmax=clipfluxcal, datacolumn="corrected", clipoutside=True,
            clipzeros=True, extendpols=False, action="apply", flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="clip", spw = gainspw, field=secondaryfield,
            clipminmax=clipphasecal, datacolumn="corrected", clipoutside=True,
            clipzeros=True, extendpols=False, action="apply", flagbackup=True,
            savepars=False, overwrite=True, writeflags=True)

    # After clip, now flag using 'tfcrop' option for flux and phase cal tight
    # flagging
    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected", field=gainfields,
            ntime="scan", timecutoff=6.0, freqcutoff=5.0, timefit="line",
            freqfit="line", flagdimension="freqtime", extendflags=False,
            timedevscale=5.0, freqdevscale=5.0, extendpols=False, growaround=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # now flag using 'rflag' option  for flux and phase cal tight flagging
    flagdata(vis=visname, mode="rflag", datacolumn="corrected", field=gainfields,
            timecutoff=5.0, freqcutoff=5.0, timefit="poly", freqfit="line",
            flagdimension="freqtime", extendflags=False, timedevscale=4.0,
            freqdevscale=4.0, spectralmax=500.0, extendpols=False, growaround=False,
            flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,
            overwrite=True, writeflags=True)

    ## Now extend the flags (70% more means full flag, change if required)
    flagdata(vis=visname, mode="extend", spw = gainspw, field=gainfields,
            datacolumn="corrected", clipzeros=True, ntime="scan", extendflags=False,
            extendpols=False, growtime=90.0, growfreq=90.0, growaround=False,
            flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,
            overwrite=True, writeflags=True)

    # Now flag for target - moderate flagging, more flagging in self-cal cycles
    flagdata(vis=visname, mode="clip", spw = gainspw, field=target, clipminmax=cliptarget,
            datacolumn="corrected", clipoutside=True, clipzeros=True,
            extendpols=False, action="apply", flagbackup=True, savepars=False,
            overwrite=True, writeflags=True)

    flagdata(vis=visname, mode="tfcrop", datacolumn="corrected", field=target,
            ntime="scan", timecutoff=6.0, freqcutoff=5.0, timefit="poly",
            freqfit="line", flagdimension="freqtime", extendflags=False,
            timedevscale=5.0, freqdevscale=5.0, extendpols=False, growaround=False,
            action="apply", flagbackup=True, overwrite=True, writeflags=True)

    # now flag using 'rflag' option
    flagdata(vis=visname, mode="rflag", datacolumn="corrected", field=target,
            timecutoff=5.0, freqcutoff=5.0, timefit="poly", freqfit="poly",
            flagdimension="freqtime", extendflags=False, timedevscale=5.0,
            freqdevscale=5.0, spectralmax=500.0, extendpols=False, growaround=False,
            flagneartime=False, flagnearfreq=False, action="apply", flagbackup=True,
            overwrite=True, writeflags=True)

    # Now summary
    flagdata(vis=visname, mode="summary", datacolumn="corrected", extendflags=True,
            name=visname + 'summary.split', action="apply", flagbackup=True,
            overwrite=True, writeflags=True)

    clearcal(visname)

# --- Actual calibration procedures

print " starting setjy for flux calibrator"
setjy(vis=visname, field = fluxfield, spw = splitspw, scalebychan=True, standard='Perley-Butler 2010')

print " starting antenna-based delay (kcorr)\n -> %s" % kcorrfile
gaincal(vis=visname, caltable = kcorrfile, field = kcorrfield, spw = splitspw,
        refant = referenceant,  minblperant = minbaselines, solnorm = False,  gaintype = 'K',
        solint = '10min', combine = '', parang = False, append = False)

print " starting bandpass -> %s" % bpassfile
bandpass(vis=visname, caltable = bpassfile, field = bpassfield, spw = splitspw,
        refant = referenceant, minblperant = minbaselines, solnorm = True,  solint = 'inf',
        combine = 'scan', bandtype = 'B', fillgaps = 8, gaintable = kcorrfile,
        gainfield = kcorrfield, parang = False, append = False)

print " starting cross hand delay -> %s" % xdelfile
gaincal(vis=visname, caltable = xdelfile, field = xdelfield, spw = gainspw,
        refant = referenceant, smodel=[1., 0., 1., 0.], solint = 'inf',
        minblperant = minbaselines, gaintype = 'KCROSS',
        combine = 'scan', gaintable = [kcorrfile, bpassfile],
        gainfield = [kcorrfield, bpassfield])

if dopolcal:
    if polnbasis.lower() == 'linear':
        print '\n\n ++++++ Linear Feed Polarization calibration ++++++'
        gain1file   = caldir+base+'.g1cal'
        dtempfile   = caldir+base+'.dtempcal'
        xy0ambpfile = caldir+base+'.xyambcal'
        xy0pfile    = caldir+base+'.xycal'

        print " starting gaincal -> %s" % gainfile
        gaincal(vis=visname, caltable=gain1file, field=fluxfield, spw = gainspw,
                refant=referenceant, solint='1min', minblperant=minbaselines, solnorm=False,
                gaintype='G', gaintable=[kcorrfile, bpassfile, xdelfile],
                gainfield = [kcorrfield, bpassfield, xdelfield],
                append=False, parang=True)

        gaincal(vis=visname, caltable=gain1file, field=secondaryfield, spw = gainspw,
                smodel=[1,0,0,0], refant=referenceant, solint='1min',
                minblperant=minbaselines, solnorm=False, gaintype='G',
                gaintable=[kcorrfile, bpassfile, xdelfile],
                gainfield = [kcorrfield, bpassfield, xdelfield],
                append=True, parang=True)

        plotcal(caltable=gain1file, xaxis='time', yaxis='amp', poln='X',
                field=secondaryfield, showgui=False, figfile = outdir+'initialgain.png',
                markersize=3, plotsymbol='-', fontsize=8)

        print "\n Solve for Q, U from initial gain solution"
        GainQU = qufromgain(gain1file)  # implied polarization from instrumental response
        print GainQU[int(dpolfield)]

        print "\n Starting x-y phase calibration\n -> %s" % xy0ambpfile
        gaincal(vis=visname, caltable = xy0ambpfile, field = dpolfield, spw = splitspw,
                refant = referenceant, solint = 'inf', combine = 'scan', gaintype = 'XYf+QU',
                minblperant = minbaselines,smodel = [1.,0.,1.,0.], preavg = 200.0,
                gaintable = [kcorrfile,bpassfile, gain1file, xdelfile],
                gainfield = [kcorrfield, bpassfield, secondaryfield, xdelfield],
                append = False, parang = True)

        print "\n Check for x-y phase ambiguity."
        xyamb(xytab=xy0ambpfile, qu=GainQU[int(dpolfield)], xyout = xy0pfile)

        S = [1.0,GainQU[int(dpolfield)][0],GainQU[int(dpolfield)][1],0.0]
        p = np.sqrt(S[1]**2 + S[2]**2)
        print "Model for polarization calibrator S =", S
        print "Fractional polarization =", p

        print "\n redoing gain calibration with new calibrator source polarization -> %s" % gainfile

        gaincal(vis=visname, caltable = gainfile, field = fluxfield, spw = splitspw,
                refant = referenceant, solint = '5min', solnorm = False,  gaintype = 'G',
                minblperant = minbaselines, combine = '',  minsnr = 3, calmode = 'ap',
                gaintable = [kcorrfile,bpassfile,xdelfile],
                gainfield = [kcorrfield,bpassfield,xdelfield],
                parang = True, append = False)

        print "\n solution for secondary with parang = true"
        gaincal(vis=visname, caltable = gainfile, field = secondaryfield, spw = splitspw,
                refant = referenceant, solint = '5min', solnorm = False,  gaintype = 'G',
                minblperant = minbaselines,combine = '', smodel = S, minsnr = 3,
                gaintable = [kcorrfile,bpassfile,xdelfile],
                gainfield = [kcorrfield,bpassfield,xdelfield],
                append = True, parang = True)

        print "\n now re-solve for Q,U from the new gainfile\n -> %s" % gainfile
        Gain2QU = qufromgain(gainfile)
        print GainQU[int(dpolfield)]

        print "starting \'Dflls\' polcal -> %s"  % dpolfile
        polcal(vis=visname, caltable = dtempfile, field = dpolfield, spw = gainspw, refant = '',
               solint = 'inf', combine = 'scan', poltype = 'Dflls', smodel = S, preavg= 200.0,
               gaintable = [kcorrfile,bpassfile, gainfile, xdelfile, xy0pfile],
               gainfield = [kcorrfield, bpassfield, secondaryfield, xdelfield, dpolfield],
               append = False)

        Dgen(dtab=dtempfile, dout=dpolfile)

    else:      # circular feed polarization calibration
        print '\n\n ++++++ Circular Feed Polarization calibration ++++++'

        gaincal(vis=visname, caltable=gainfile, field=gainfields, spw = gainspw,
                refant=referenceant, solint='inf', minblperant=minbaselines, solnorm=False,
                gaintype='G', combine='scan', calmode='ap', gaintable=[kcorrfile,
                    bpassfile, xdelfile], gainfield=[kcorrfield, bpassfield, xdelfield],
                append=False, parang=True)

        print " starting Df polcal -> %s"  % dpolfile
        polcal(vis=visname, caltable = dpolfile, field = dpolfield, spw = gainspw,
            refant = referenceant, solint = 'inf', minblperant = minbaselines, combine = 'scan',
            poltype = 'Df+QU',
            gaintable = [kcorrfile, bpassfile, gainfile, xdelfile],
            gainfield = [kcorrfield, bpassfield, secondaryfield, xdelfield],
            append = False)

        print " starting Xf polcal -> %s"  % xpolfile
        polcal(vis=visname, caltable = xpolfile, field = xpolfield, spw = gainspw,
            refant = referenceant, solint = 'inf', minblperant = minbaselines, combine = 'scan',
            poltype = 'Xf', smodel = PrimaryPolModel,
            gaintable = [kcorrfile, bpassfile, gainfile, xdelfile, dpolfile],
            gainfield = [kcorrfield, bpassfield, secondaryfield, xdelfield, dpolfield],
            append = False)

print " starting fluxscale -> %s", fluxfile
fluxscale(vis=visname, caltable = gainfile, reference = fluxfield,
          transfer = '', fluxtable = fluxfile,
          listfile = caldir+'fluxscale.txt', append = False)

if doplotcal:
    #-------------------------------------- Plot cal solutions
    print " plotting cal solutions"
    os.chdir(procdir)
    # antenna-based delay
    print " antenna-based delay"
    plotcal(caltable = kcorrfile, xaxis = 'time', yaxis = 'delay', poln = 'X',
            field = kcorrfield, showgui = False,
            figfile = caldir+'spw'+gainchannels+'X_delay.png',
            markersize=3.0, plotsymbol= '-.', fontsize=8.0)
    plotcal(caltable = kcorrfile, xaxis = 'time', yaxis = 'delay', poln = 'Y',
            field = kcorrfield, showgui = False,
            figfile = caldir+'spw'+gainchannels+'Y_delay.png',
            markersize=3.0, plotsymbol= '-.', fontsize=8.0)

    # bandpass
    print " plotting bandpass solutions"
    plotcal(caltable = bpassfile, xaxis = 'chan', yaxis = 'amp',
            field = bpassfield, iteration = 'antenna',
            subplot = 931, showgui = False,
            figfile = caldir+'spw'+splitchannels+'_bpass.amp.png',
            markersize=3.0, plotsymbol= '.', plotrange=[-2,250,0.2,1.5], fontsize=8.0,
            plotcolor = 'blue')
    plotcal(caltable = bpassfile, xaxis = 'chan', yaxis = 'phase',
            field = bpassfield, antenna = '', iteration = 'antenna',
            subplot = 931, showgui = False,
            figfile = caldir+'spw'+splitchannels+'_bpass.phase.png',
            markersize=3.0, plotsymbol= '.', fontsize=8.0, plotcolor = 'blue')

    # gain
    print " plotting gain solutions"
    plotcal(caltable = fluxfile, xaxis = 'time', yaxis = 'amp',
            field = '', antenna = '',  iteration = 'antenna',
            subplot = 931, showgui = False, figfile = caldir+'spw'+gainchannels+'_gain.amp.png',
            markersize=3.0, plotsymbol= '.', fontsize=8.0, plotcolor = 'blue')
    plotcal(caltable = fluxfile, xaxis = 'time', yaxis = 'phase',
            field = '', antenna = '', iteration = 'antenna',
            subplot = 931, showgui = False, figfile = caldir+'spw'+gainchannels+'_gain.phase.png',
            markersize=3.0, plotsymbol= '.', fontsize=8.0, plotcolor = 'blue')

    # polarization D-terms versus channel
    print " plotting D-terms"
    plotcal(caltable = dpolfile, xaxis = 'chan', yaxis = 'amp', field = dpolfield,
            spw = '', iteration = 'antenna', subplot = 931, showgui = False,
            figfile = caldir+'spw'+splitchannels+'_dpol.amp.png',
            markersize=3.0, plotsymbol= '.', plotrange=[-2,250,0.0,0.5], fontsize=8.0,
            plotcolor = 'blue')
    plotcal(caltable = dpolfile, xaxis = 'chan', yaxis = 'phase', field = dpolfield,
            spw = '', iteration = 'antenna', subplot = 931, showgui = False,
            figfile = caldir+'spw'+splitchannels+'_dpol.phase.png',
            markersize=3.0, plotsymbol= '.', fontsize=8.0, plotcolor = 'blue')
    if(polnbasis == 'linear'):
        print " plotting x-y phase solution"
        plotcal(caltable = xy0pfile, xaxis = 'chan', yaxis = 'phase',
                field = dpolfield, antenna='', spw = splitspw, iteration = 'antenna',
                subplot = 821, showgui = False, figfile = caldir+'xy0.png',
                markersize=1.0, plotsymbol= '.', fontsize=8.0)
    else:
        # polarization position angle correction versus channel
        print " plotting position angle solution"
        plotcal(caltable = xpolfile, xaxis = 'chan', yaxis = 'phase', field = xpolfield,
                spw = '', showgui = False, figfile = caldir+'spw'+splitchannels+'_xpol.png',
                markersize=5.0, plotsymbol= '.', fontsize=8.0)

#os.chdir(workdir)
#---------------------------------------------------
marktime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
print "Calibration solutions complete: %s" % marktime
print "Applying calibrations..."
# ---------------------------------------------------

if polnbasis.lower() == 'linear':
    xpolfile = xy0pfile
    xpolfield = dpolfield

print " applying calibrations: primary calibrator"
applycal(vis=visname, field = fluxfield, spw = splitspw, selectdata = False, calwt = False,
    gaintable = [kcorrfile,bpassfile, fluxfile, dpolfile, xdelfile, xpolfile],
    gainfield = [kcorrfield,bpassfield,fluxfield,dpolfield,xdelfield, xpolfield],
    parang = True)

print " applying calibrations: polarization calibrator"
applycal(vis=visname, field = dpolfield, spw = splitspw, selectdata = False, calwt = False,
    gaintable = [kcorrfile,bpassfile, fluxfile, dpolfile, xdelfile, xpolfile],
    gainfield = [kcorrfield,bpassfield,secondaryfield,dpolfield,xdelfield,xpolfield],
    parang= True)

print " applying calibrations: secondary calibrators"
applycal(vis=visname, field = secondaryfield, spw = splitspw, selectdata = False, calwt = False,
    gaintable = [kcorrfile, bpassfile, fluxfile, dpolfile, xdelfile, xpolfile],
    gainfield = [kcorrfield, bpassfield, secondaryfield, dpolfield, xdelfield, xpolfield],
    parang= True)

print " applying calibrations: target fields"
applycal(vis=visname, field = targetfield, spw = splitspw, selectdata = False, calwt = False,
    gaintable = [kcorrfile, bpassfile, fluxfile, dpolfile, xdelfile, xpolfile],
    gainfield = [kcorrfield, bpassfield, secondaryfield, dpolfield, xdelfield, xpolfield],
    parang= True)

marktime = time.strftime("%a, %d %b %Y %H:%M:%S", time.localtime())
print "Apply calibrations complete: %s" % marktime

split(vis=visname, outputvis = base+'.' + target + '.ms', datacolumn='corrected',
         field = fields.target, spw = splitspw, keepflags=True, width = specave,
         timebin = timeave)

split(vis=visname, outputvis = base+'.' + phasecal + '.ms', datacolumn='corrected',
         field = fields.phase, spw = splitspw, keepflags=True, width = specave,
         timebin = timeave)

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
    if ( filename.endswith('.last') or filename.endswith('.log') ):
        os.remove(filename)
