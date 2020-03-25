#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import shutil
import os

import config_parser
from config_parser import validate_args as va
from cal_scripts import bookkeeping

import logging
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

# So that CASA can find pyBDSF
os.putenv('PYTHONPATH', '/usr/lib/python2.7/dist-packages/')

def predict_model(vis, imagename, imsize, cell, gridder, wprojplanes,
                      deconvolver, robust, niter, multiscale, threshold, nterms,
                      regionfile,loop):

    # Rename image products for predict
    models=glob.glob(imagename + '.model*')
    images=glob.glob(imagename + '.image*')

    for fname in images+models:
        name, ext = os.path.splitext(fname)
        os.rename(fname,name+'.round1'+ext)

    startmodel = sorted(glob.glob(imagename + '.model*'))

    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
            wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop], restoration=False,
            weighting='briggs', robust = robust[loop], niter=0, scales=multiscale[loop],
            threshold=threshold[loop], nterms=nterms[loop], calcpsf=False, calcres=False,
            startmodel = startmodel, savemodel='modelcolumn', pblimit=-1,
            mask='', parallel = False)

    #Rename image output to previous name
    for fname in images:
        name, ext = os.path.splitext(fname)
        os.rename(name+'.round1'+ext,fname)

def selfcal_part2(vis, nloops, restart_no, cell, robust, imsize, wprojplanes, niter, threshold,
                multiscale, nterms, gridder, deconvolver, solint, calmode, atrous, loop):

    basename = vis.replace('.ms', '') + '_im_%d'
    imagename = basename % (loop + restart_no)
    regionfile = basename % (loop + restart_no) + ".casabox"
    caltable = vis.replace('.ms', '') + '.gcal%d' % (loop + restart_no)
    prev_caltables = sorted(glob.glob('*.gcal?'))

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
                    gaintable=prev_caltables, calmode=calmode[loop], append=False, parang=True)

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

    args,params = bookkeeping.get_selfcal_params()
    loop = selfcal_part2(**params)
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')
