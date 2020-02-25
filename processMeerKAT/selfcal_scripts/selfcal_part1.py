#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import os

import config_parser
from config_parser import validate_args as va

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def selfcal_part1(vis, nloops, restart_no, cell, robust, imsize, wprojplanes, niter, threshold,
		multiscale, nterms, gridder, deconvolver, solint, calmode, atrous, loop):

    imagename = vis.replace('.ms', '') + '_im_%d' % (loop + restart_no)
    regionfile = imagename + ".casabox"
    caltable = vis.replace('.ms', '') + '.gcal%d' % (loop + restart_no)

    if loop > 0 and not os.path.exists(caltable):
        logger.error("Calibration table {0} doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(caltable,loop))
        sys.exit(1)
    else:
        if loop == 0 and not os.path.exists(regionfile):
            regionfile = ''
            imagename += '_nomask'
        elif 0 < loop <= (nloops):
                applycal(vis=vis, selectdata=False, gaintable=caltable, parang=True)

                flagdata(vis=vis, mode='rflag', datacolumn='RESIDUAL', field='', timecutoff=5.0,
                    freqcutoff=5.0, timefit='line', freqfit='line', flagdimension='freqtime',
                    extendflags=False, timedevscale=3.0, freqdevscale=3.0, spectralmax=500,
                    extendpols=False, growaround=False, flagneartime=False, flagnearfreq=False,
		    action='apply', flagbackup=True, overwrite=True, writeflags=True)

        tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize, cell=cell, stokes='I', gridder=gridder,
            wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=True,
            weighting='briggs', robust = robust, niter=niter[loop], scales=multiscale[loop],
            threshold=threshold[loop], nterms=nterms[loop],
            savemodel='none', pblimit=-1, mask=regionfile, parallel = True)


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])
    params = taskvals['selfcal']

    for arg in ['multiscale','nterms','calmode','atrous']:
        if type(params[arg]) is not list:
            params[arg] = [params[arg]] * len(params['niter'])

    if 'loop' not in params:
        params['loop'] = 0

    selfcal_part1(**params)

