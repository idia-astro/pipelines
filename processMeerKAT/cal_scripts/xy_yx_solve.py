from __future__ import print_function

import sys
import os
import shutil

import config_parser
from cal_scripts import bookkeeping
from config_parser import validate_args as va
from recipes.almapolhelpers import *

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def do_cross_cal(visname, fields, calfiles, referenceant, caldir,
        minbaselines, standard, do_clearcal=False):

    logger.info(" starting antenna-based delay (kcorr)\n -> %s" % calfiles.kcorrfile)
    gaincal(vis=visname, caltable = calfiles.kcorrfile,
            field = fields.kcorrfield, refant = referenceant,
            minblperant = minbaselines, solnorm = False,  gaintype = 'K',
            solint = '10min', combine = 'scan', parang = False, append = False)

    logger.info(" starting bandpass -> %s" % calfiles.bpassfile)
    bandpass(vis=visname, caltable = calfiles.bpassfile,
            field = fields.bpassfield,
            refant = referenceant, minblperant = minbaselines, solnorm = False,
            solint = 'inf', combine = 'scan', bandtype = 'B', fillgaps = 8,
            gaintable = calfiles.kcorrfile, gainfield = fields.kcorrfield,
            parang = False, append = False)

    logger.info(" starting cross hand delay -> %s" % calfiles.xdelfile)
    gaincal(vis=visname, caltable = calfiles.xdelfile, field = fields.xdelfield,
            refant = referenceant, smodel=[1., 0., 1., 0.],
            solint = 'inf', minblperant = minbaselines, gaintype = 'KCROSS',
            combine = 'scan',
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile],
            gainfield = [fields.kcorrfield, fields.bpassfield])

    base = visname.replace('.ms', '')
    gain1file   = os.path.join(caldir, base+'.g1cal')
    dtempfile   = os.path.join(caldir, base+'.dtempcal')
    xy0ambpfile = os.path.join(caldir, base+'.xyambcal')
    xy0pfile    = os.path.join(caldir, base+'.xycal')

    # Delete output from any previous calibration run
    if os.path.exists(gain1file):
        shutil.rmtree(gain1file)

    if os.path.exists(dtempfile):
        shutil.rmtree(dtempfile)

    if os.path.exists(xy0ambpfile):
        shutil.rmtree(xy0ambpfile)

    if os.path.exists(xy0pfile):
        shutil.rmtree(xy0pfile)

    # TODO: Look into deleting this first call
    logger.info(" starting gaincal -> %s" % gain1file)
    gaincal(vis=visname, caltable=gain1file, field=fields.fluxfield,
            refant=referenceant, solint='10min', minblperant=minbaselines,
            solnorm=False, gaintype='G',
            gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.xdelfield], append=False, parang=True)

    gaincal(vis=visname, caltable=gain1file, field=fields.secondaryfield,
            smodel=[1,0,0,0], refant=referenceant, solint='10min',
            minblperant=minbaselines, solnorm=False, gaintype='G',
            gaintable=[calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.xdelfield],
            append=True, parang=True)

    # implied polarization from instrumental response
    logger.info("\n Solve for Q, U from initial gain solution")
    GainQU = qufromgain(gain1file)
    logger.info(GainQU[int(fields.dpolfield)])

    logger.info("\n Starting x-y phase calibration\n -> %s" % xy0ambpfile)
    gaincal(vis=visname, caltable = xy0ambpfile, field = fields.dpolfield,
            refant = referenceant, solint = 'inf', combine = 'scan',
            gaintype = 'XYf+QU', minblperant = minbaselines,
            smodel = [1.,0.,1.,0.], preavg = 200.0,
            gaintable = [calfiles.kcorrfile,calfiles.bpassfile,
                gain1file, calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.secondaryfield, fields.xdelfield],
            append = False)

    logger.info("\n Check for x-y phase ambiguity.")
    xyamb(xytab=xy0ambpfile, qu=GainQU[int(fields.dpolfield)], xyout = xy0pfile)

    S = [1.0, GainQU[int(fields.dpolfield)][0],
            GainQU[int(fields.dpolfield)][1], 0.0]

    p = np.sqrt(S[1]**2 + S[2]**2)
    logger.info("Model for polarization calibrator S = {0:.4}".format(S))
    logger.info("Fractional polarization = {0:.4}".format(p))

    gaincal(vis=visname, caltable = calfiles.gainfile, field = fields.fluxfield,
            refant = referenceant, solint = '10min', solnorm = False,
            gaintype = 'G', minblperant = minbaselines, combine = '',
            minsnr = 3, calmode = 'ap',
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield,fields.bpassfield,fields.xdelfield],
            parang = True, append = False)

    logger.info("\n solution for secondary with parang = true")
    gaincal(vis=visname, caltable = calfiles.gainfile,
            field = fields.secondaryfield, refant = referenceant,
            solint = '10min', solnorm = False,
            gaintype = 'G', minblperant = minbaselines,
            combine = '', smodel = S, minsnr = 3,
            gaintable = [calfiles.kcorrfile, calfiles.bpassfile,
                calfiles.xdelfile],
            gainfield = [fields.kcorrfield, fields.bpassfield,
                fields.xdelfield],
            parang = True, append = True)

    logger.info("\n now re-solve for Q,U from the new gainfile\n -> %s" \
                                                        % calfiles.gainfile)
    Gain2QU = qufromgain(calfiles.gainfile)
    logger.info(Gain2QU[int(fields.dpolfield)])

    logger.info("starting \'Dflls\' polcal -> %s"  % calfiles.dpolfile)
    polcal(vis=visname, caltable = dtempfile, field = fields.dpolfield,
            refant = '', solint = 'inf', combine = 'scan',
            poltype = 'Dflls', smodel = S, preavg= 200.0,
            gaintable = [calfiles.kcorrfile,calfiles.bpassfile,
                calfiles.gainfile, calfiles.xdelfile, xy0pfile],
           gainfield = [fields.kcorrfield, fields.bpassfield,
               fields.secondaryfield, fields.xdelfield, fields.dpolfield],
           append = False)

    Dgen(dtab=dtempfile, dout=calfiles.dpolfile)

    # Only run fluxscale if bootstrapping
    if len(fields.gainfields) > 1:
        logger.info(" starting fluxscale -> %s", calfiles.fluxfile)
        fluxscale(vis=visname, caltable = calfiles.gainfile,
                reference = fields.fluxfield, transfer = '',
                fluxtable = calfiles.fluxfile,
                listfile = os.path.join(caldir,'fluxscale_xy_yx.txt'),
                append = False, display=False)


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])

    visname = va(taskvals, 'data', 'vis', str)

    calfiles, caldir = bookkeeping.bookkeeping(visname)
    fields = bookkeeping.get_field_ids(taskvals['fields'])

    refant = va(taskvals, 'crosscal', 'refant', str, default='m005')
    minbaselines = va(taskvals, 'crosscal', 'minbaselines', int, default=4)
    standard = va(taskvals, 'crosscal', 'standard', str, default='Perley-Butler 2010')

    do_cross_cal(visname, fields, calfiles, refant, caldir,
            minbaselines, standard, do_clearcal=True)
