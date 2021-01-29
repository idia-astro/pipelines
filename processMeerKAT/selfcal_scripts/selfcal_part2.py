#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import shutil
import os

import config_parser
from config_parser import validate_args as va
import bookkeeping

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def predict_model(vis, imagename, imsize, cell, gridder, wprojplanes, deconvolver,
                  robust, niter, threshold, nterms, regionfile, loop):

    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
            wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop],
            weighting='briggs', robust = robust[loop], niter=0,
            threshold=threshold[loop], nterms=nterms[loop], pblimit=-1, mask=regionfile,
            savemodel='modelcolumn', restart=True, restoration=False, calcpsf=False, calcres=False, parallel = False)

def selfcal_part2(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter,
                  threshold, uvrange, nterms, gridder, deconvolver, solint, calmode, flag):

    visbase = os.path.split(vis.rstrip('/ '))[1] # Get only vis name, not entire path
    basename = visbase.replace('.mms', '') + '_im_%d' # Images will be produced in $CWD
    imagename = basename % loop
    pixmask = basename % loop + ".pixmask"
    rmsfile = basename % loop + ".rms"
    caltable = visbase.replace('.mms', '') + '.gcal%d' % loop
    prev_caltables = sorted(glob.glob('*.gcal?'))

    if loop == 0 and not os.path.exists(pixmask):
        imagename += '_nomask'
        do_gaincal = False
    else:
        do_gaincal = True

    if nterms[loop] > 1 and deconvolver[loop] == 'mtmfs':
        bdsfname = imagename + ".image.tt0"
    else:
        bdsfname = imagename + ".image"

    fitsname = imagename + '.fits'
    exportfits(imagename = bdsfname, fitsimage=fitsname)

    if not (type(threshold[loop]) is str and 'Jy' in threshold[loop]) and threshold[loop] > 1 and os.path.exists(rmsfile):
        stats = imstat(imagename=rmsfile)
        threshold[loop] *= stats['min'][0]

    if not os.path.exists(bdsfname):
        logger.error("Image {0} doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(bdsfname,loop))
        return loop+1
    else:
        if do_gaincal:
            if os.path.exists(caltable):
                loop += 1
                logger.info('Caltable {} exists. Not overwriting, continuing to next loop.'.format(caltable))
                return loop

            predict_model(vis, imagename, imsize, cell, gridder, wprojplanes,
                      deconvolver, robust, niter, threshold, nterms, pixmask,loop)

            solnorm = 'a' in calmode[loop]
            normtype='median' #if solnorm else 'mean'
            gaintype = 'T' if dopol else 'G'

            gaincal(vis=vis, caltable=caltable, selectdata=True, refant = refant, solint=solint[loop], solnorm=solnorm,
                    normtype=normtype,
                    gaintype=gaintype,
                    uvrange=uvrange[loop],
                    gaintable=prev_caltables,
                    calmode=calmode[loop], append=False, parang=False)

            loop += 1

        return loop


if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    loop = selfcal_part2(**params)
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')
    bookkeeping.rename_logs(logfile)
