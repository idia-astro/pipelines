#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import os

import config_parser
from config_parser import validate_args as va
import bookkeeping

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def symlink_psf(imagename,prefix):

    for product in ['psf','sumwt']:
        products = glob.glob('{0}.{1}*'.format(prefix,product))
        for fname in products:
            name, ext = os.path.splitext(fname)
            os.symlink(fname,'{0}.{1}{2}'.format(imagename,product,ext))

def selfcal_part1(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter,
                  threshold, uvrange, nterms, gridder, deconvolver, solint, calmode, flag):

    visbase = os.path.split(vis.rstrip('/ '))[1] # Get only vis name, not entire path
    basename = visbase.replace('.mms', '') + '_im_%d' # Images will be produced in $CWD
    imagename = basename % loop
    pixmask = basename % loop + ".pixmask"
    rmsfile = basename % loop + ".rms"
    caltable = visbase.replace('.mms', '') + '.gcal%d' % (loop - 1)
    all_caltables = sorted(glob.glob('*.gcal?'))
    calcpsf = True
    symlink = False

    if loop == 0 and not os.path.exists(pixmask):
        clearcal(vis=vis, addmodel=True) #Add model column with MPI rather than in selfcal_part2 without MPI

    if not (type(threshold[loop]) is str and 'Jy' in threshold[loop]) and threshold[loop] > 1 and os.path.exists(rmsfile):
        stats = imstat(imagename=rmsfile)
        threshold[loop] *= stats['min'][0]

    if loop > 0 and not os.path.exists(caltable):
        logger.error("Calibration table {0} doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(caltable,loop))
        sys.exit(1)
    else:
        if loop == 0:
            if not os.path.exists(pixmask):
                pixmask = ''
                imagename += '_nomask'
            else:
                symlink_psf(imagename,imagename + '_nomask')
                calcpsf = False

        elif 0 < loop <= (nloops):
                applycal(vis=vis, selectdata=False, gaintable=all_caltables, parang=False, interp='linear,linearflag')

                if flag[loop]:
                    flagdata(vis=vis, mode='rflag', datacolumn='RESIDUAL', field='', timecutoff=5.0,
                            freqcutoff=5.0, timefit='line', freqfit='line', flagdimension='freqtime',
                            extendflags=False, timedevscale=3.0, freqdevscale=3.0, spectralmax=500,
                            extendpols=False, growaround=False, flagneartime=False, flagnearfreq=False,
                            action='apply', flagbackup=True, overwrite=True, writeflags=True)

                #TODO: check with of these are necessary
                elif gridder[loop] == gridder[loop-1] and robust[loop] == robust[loop-1] and nterms[loop] == nterms[loop-1] and imsize[loop] == imsize[loop-1] and cell[loop] == cell[loop-1] and wprojplanes[loop] == wprojplanes[loop-1]:
                    symlink_psf(imagename,basename % (loop-1))
                    calcpsf = False

        tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
            wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop], restoration=True,
            weighting='briggs', robust = robust[loop], niter=niter[loop],
            threshold=threshold[loop], nterms=nterms[loop], calcpsf=calcpsf,
            pblimit=-1, mask=pixmask, parallel = True)


if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    selfcal_part1(**params)
