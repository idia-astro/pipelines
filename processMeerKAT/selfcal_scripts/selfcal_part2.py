#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import shutil
import os

import config_parser
from config_parser import validate_args as va

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

# So that CASA can find pyBDSF
os.putenv('PYTHONPATH', '/usr/lib/python2.7/dist-packages/')

def sortByTT(fname):
    return int(fname[-1])

def predict_model(vis, imagename, imsize, cell, gridder, wprojplanes,
                      deconvolver, robust, niter, multiscale, threshold, nterms,
                      regionfile,loop):

    # Rename image products for predict
    models=glob.glob(imagename + '.model*')
    images=glob.glob(imagename + '.image*')

    for fname in images+models:
        name, ext = os.path.splitext(fname)
        os.rename(fname,name+'.round1'+ext)

    startmodel = glob.glob(imagename + '.model*')
    if nterms[loop] > 1:
        startmodel.sort(key=sortByTT)

    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize, cell=cell, stokes='I', gridder=gridder,
            wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=False,
            weighting='briggs', robust = robust, niter=0, scales=multiscale[loop],
            threshold=threshold[loop], nterms=nterms[loop], calcpsf=False, calcres=False,
            startmodel = startmodel, savemodel='modelcolumn', pblimit=-1,
            mask='', parallel = False)

def selfcal_part2(vis, nloops, restart_no, cell, robust, imsize, wprojplanes, niter, threshold,
                multiscale, nterms, gridder, deconvolver, solint, calmode, atrous, loop):

    basename = vis.replace('.ms', '') + '_im_%d'
    imagename = basename % (loop + restart_no)
    regionfile = basename % (loop + restart_no) + ".casabox"
    caltable = vis.replace('.ms', '') + '.gcal%d' % (loop + restart_no)

    if loop == 0 and not os.path.exists(regionfile):
        imagename += '_nomask'
        do_gaincal = False
    else:
        do_gaincal = True

    if nterms[loop] > 1:
        bdsmname = imagename + ".image.tt0"
    else:
        bdsmname = imagename + ".image"

    if not os.path.exists(bdsmname):
        logger.error("Image {0} doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(bdsmname,loop))
        sys.exit(1)
    else:
        if do_gaincal:
            predict_model(vis, imagename, imsize, cell, gridder, wprojplanes,
                      deconvolver, robust, niter, multiscale, threshold, nterms,
                      regionfile,loop)

            gaincal(vis=vis, caltable=caltable, selectdata=False, solint=solint[loop],
                calmode=calmode[loop], append=False, parang=True)

            loop += 1

        regionfile = basename % (loop + restart_no) + ".casabox"

        # If it's the first round of selfcal and regionfile is blank, only run
        # BDSF and quit.
        if atrous[loop]:
            atrous_str = '--atrous-do'
        else:
            atrous_str = ''

        os.system('/usr/bin/python bdsm_model.py {} {} --thresh-isl 20 '
        '--thresh-pix 10 {} --clobber --adaptive-rms-box '
        '--rms-map'.format(bdsmname, regionfile, atrous_str))

        return loop


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])
    params = taskvals['selfcal']

    if 'loop' not in params:
        params['loop'] = 0

    for arg in ['multiscale','nterms','calmode','atrous']:

        # Multiscale needs to be a list of lists (if specifying multiple scales)
        # or a simple list (if specifying a single scale). So make sure these two
        # cases are covered.

        # multiscale is not a list of lists, so turn it into one
        if arg == 'multiscale' and type(params[arg]) is list and len(params[arg]) == 0:
            params[arg] = [params[arg],] * len(params['niter'])
        # Not a list at all, so put it into a list
        if arg == 'multiscale' and type(params[arg]) is not list:
            params[arg] = [[params[arg],],] * len(params['niter'])

        if type(params[arg]) is not list:
            params[arg] = [params[arg]] * len(params['niter'])

    loop = selfcal_part2(**params)
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')
