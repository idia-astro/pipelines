#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os
import shutil

import config_parser
import bookkeeping
from config_parser import validate_args as va
from recipes.almapolhelpers import *
from recipes import tec_maps

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def qu_polfield(polfield, visname):
    """
    Given the pol source name and the reference frequency, returns the fractional Q and U
    calculated from Perley & Butler 2013
    """

    msmd.open(visname)
    meanfreq = msmd.meanfreq(0, unit='GHz')
    msmd.done()

    if polfield in ["3C286", "1328+307", "1331+305", "J1331+3030"]:
        #f_coeff=[1.2515,-0.4605,-0.1715,0.0336]    # coefficients for model Stokes I spectrum from Perley and Butler 2013
        perley_frac = np.array([0.086,0.095,0.099])
        perley_f = np.array([1050,1450,1640])
        pa_polcal = np.array([33.0,33.0,33.0])
    elif polfield in ["3C138", "0518+165", "0521+166", "J0521+1638"]:
        #f_coeff=[1.0332,-0.5608,-0.1197,0.041]    # coefficients for model Stokes I spectrum from Perley and Butler 2013
        perley_frac = np.array([0.056,0.075,0.084])
        perley_f = np.array([1050,1450,1640])
        pa_polcal = np.array([-14.0,-11.0,-10.0])
    else:
        # This should never happen.
        raise ValueError("Invalid polarization field. Exiting.")

    p = np.polyfit(perley_f, perley_frac, deg=2)
    p = np.poly1d(p)

    pa = np.polyfit(perley_f, pa_polcal, deg=2)
    pa = np.poly1d(pa)

    #polpoly = np.poly1d(polcoeffs)
    #polval = polpoly(meanfreq)

    # BEWARE: Stokes I coeffs are in log-log space, so care must be taken while converting to linear.
    # They are in np.log10 space, not np.log space!
    # Coefficients are from Perley-Butler 2013 (An Accurate Flux Density Scale from 1 to 50 GHz)
    #stokesIpoly = np.poly1d(stokesIcoeffs)
    #stokesIval = stokesIpoly(np.log10(meanfreq))
    ## in Jy
    #stokesIval = np.power(10, stokesIval)

    q = p(meanfreq) * np.cos(2*np.deg2rad(pa(meanfreq)))
    u = p(meanfreq) * np.sin(2*np.deg2rad(pa(meanfreq)))

    return q, u

def do_cross_cal(visname, fields, calfiles, referenceant, caldir,
        minbaselines, standard):

    polfield = bookkeeping.polfield_name(visname)
    if polfield == '':
        polfield = fields.secondaryfield
    else:
        polqu = qu_polfield(polfield, visname)

    if not os.path.isdir(caldir):
        os.makedirs(caldir)
    elif not os.path.isdir(caldir+'_round1'):
        os.rename(caldir,caldir+'_round1')
        os.makedirs(caldir)

    base = visname.replace('.ms', '')
    dtempfile   = os.path.join(caldir, base+'.dtempcal')
    xy0ambpfile = os.path.join(caldir, base+'.xyambcal')
    xy0pfile    = os.path.join(caldir, base+'.xycal')
    xpfile      = os.path.join(caldir, base+'.xfcal')


    logger.info(" starting bandpass -> %s" % calfiles.bpassfile)
    bandpass(vis=visname, caltable = calfiles.bpassfile,
            field = fields.bpassfield, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  solint = '10min',
            combine = 'scan', bandtype = 'B', fillgaps = 8,
            parang = False, append = False)
    bookkeeping.check_file(calfiles.bpassfile)

    logger.info("starting \'Dflls\' polcal -> %s"  % calfiles.dpolfile)
    polcal(vis=visname, caltable = calfiles.dpolfile, field = fields.bpassfield,
            refant = '', solint = 'inf', combine = 'scan',
            poltype = 'Dflls', preavg= 200.0,
            gaintable = [calfiles.bpassfile],
            gainfield = [fields.bpassfield],
            append = False)
    bookkeeping.check_file(calfiles.dpolfile)

    logger.info(" starting gain calibration\n -> %s" % calfiles.gainfile)
    gaincal(vis=visname, caltable = calfiles.gainfile,
            field = fields.gainfields, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'T',
            solint = 'inf', combine = '', calmode='ap',
            gaintable=[calfiles.bpassfile,calfiles.dpolfile],
            gainfield=[fields.bpassfield,fields.bpassfield],
            parang = False, append = False)
    bookkeeping.check_file(calfiles.gainfile)

    if polfield != fields.secondaryfield:
        logger.info(" starting pol calibrator gain calibration\n -> %s" % calfiles.gainfile)
        gaincal(vis=visname, caltable = calfiles.gainfile,
                field = polfield, refant = referenceant,
                minblperant = minbaselines, solnorm = False,  gaintype = 'T',
                solint = 'inf', combine = '', calmode='ap',
                gaintable=[calfiles.bpassfile,calfiles.dpolfile],
                gainfield=[fields.bpassfield,fields.bpassfield],
                parang = False, append = True)
        bookkeeping.check_file(calfiles.gainfile)

    # Only run fluxscale if bootstrapping
    if len(fields.gainfields) > 1:
        logger.info(" starting fluxscale -> %s", calfiles.fluxfile)
        fluxscale(vis=visname, caltable=calfiles.gainfile,
                reference=[fields.fluxfield], transfer='',
                fluxtable=calfiles.fluxfile, append=False, display=False,
                listfile = os.path.join(caldir,'fluxscale_xy_yx.txt'))
        bookkeeping.check_file(calfiles.fluxfile)

    if polfield == fields.secondaryfield:
        # Cannot resolve XY ambiguity so write into final file directly
        xyfile = xy0pfile
    else:
        xyfile = xy0ambpfile

    logger.info("\n Starting x-y phase calibration\n -> %s" % xy0ambpfile)
    gaincal(vis=visname, caltable = xyfile, field = polfield,
            refant = referenceant, solint = 'inf', combine = 'scan,2.5MHz',
            gaintype = 'XYf+QU', minblperant = minbaselines,
            preavg = 200.0,
            gaintable = [calfiles.bpassfile, calfiles.dpolfile, calfiles.gainfile],
            gainfield = [fields.bpassfield, fields.bpassfield, polfield],
            append = False)
    bookkeeping.check_file(xyfile)

    if polfield != fields.secondaryfield:
        logger.info("\n Check for x-y phase ambiguity.")
        #logger.info("Polarization qu is ", polqu)
        S = xyamb(xytab=xy0ambpfile, qu=polqu, xyout = xy0pfile)
        #logger.info("smodel = ", S)


def main(args,taskvals):

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')
    minbaselines = va(taskvals, 'crosscal', 'minbaselines', int, default=4)
    standard = va(taskvals, 'crosscal', 'standard', str, default='Perley-Butler 2010')

    do_cross_cal(visname, fields, calfiles, refant, caldir, minbaselines, standard)

if __name__ == '__main__':

    bookkeeping.run_script(main)
