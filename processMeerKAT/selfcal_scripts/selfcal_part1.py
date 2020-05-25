#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import os

import config_parser
from config_parser import validate_args as va
from cal_scripts import bookkeeping

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def selfcal_part1(vis, nloops, restart_no, cell, robust, imsize, wprojplanes, niter, threshold,
        multiscale, nterms, gridder, deconvolver, solint, calmode, atrous, loop, refant):

    visbase = os.path.split(vis)[1] # Get only vis name, not entire path
    basename = visbase.replace('.ms', '') + '_im_%d' # Images will be produced in $CWD
    imagename = basename % (loop + restart_no)
    #regionfile = basename % (loop + restart_no) + ".casabox"
    pixmask = basename % (loop + restart_no) + ".pixmask"
    caltable = vis.replace('.ms', '') + '.gcal%d' % (loop + restart_no - 1)
    all_caltables = sorted(glob.glob('*.gcal?'))

    if loop > 0 and not os.path.exists(caltable):
        logger.error("Calibration table {0} doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(caltable,loop))
        sys.exit(1)
    else:
        if loop == 0 and not os.path.exists(pixmask):
            pixmask = ''
            imagename += '_nomask'
        elif 0 < loop <= (nloops):
                applycal(vis=vis, selectdata=False, gaintable=all_caltables, parang=True)

                flagdata(vis=vis, mode='rflag', datacolumn='RESIDUAL', field='', timecutoff=5.0,
                    freqcutoff=5.0, timefit='line', freqfit='line', flagdimension='freqtime',
                    extendflags=False, timedevscale=3.0, freqdevscale=3.0, spectralmax=500,
                    extendpols=False, growaround=False, flagneartime=False, flagnearfreq=False,
                    action='apply', flagbackup=True, overwrite=True, writeflags=True)

        tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
            wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop], restoration=True,
            weighting='briggs', robust = robust[loop], niter=niter[loop], scales=multiscale[loop],
            threshold=threshold[loop], nterms=nterms[loop],
            savemodel='none', pblimit=-1, mask=pixmask, parallel = True)


if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    selfcal_part1(**params)

