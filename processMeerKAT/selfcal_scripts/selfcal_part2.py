#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import shutil
import os

import config_parser
from config_parser import validate_args as va

# So that CASA can find pyBDSF
os.putenv('PYTHONPATH', '/usr/lib/python2.7/dist-packages/')

def sortByTT(fname):
    return int(fname[-1])

def predict_model(vis, imagename, imsize, cell, gridder, wprojplanes,
                      deconvolver, robust, niter, multiscale, threshold, nterms,
                      regionfile,loop):

    ll = loop

    # Rename image products for predict
    models=glob.glob(imagename + '.model*')
    images=glob.glob(imagename + '.image*')

    for fname in images+models:
        name, ext = os.path.splitext(fname)
        os.rename(fname,name+'.round1'+ext)

    startmodel = glob.glob(imagename + '.model*')
    if nterms > 1:
        models.sort(key=sortByTT)

    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize, cell=cell, stokes='I', gridder=gridder,
            wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=False,
            weighting='briggs', robust = robust, niter=0, scales=multiscale[ll],
            threshold=threshold[ll], nterms=nterms[ll], calcpsf=False, calcres=False,
            startmodel = startmodel, savemodel='modelcolumn', pblimit=-1,
            mask=regionfile, parallel = False)

def selfcal_part2(vis, nloop, restart_no, cell, robust, imsize, wprojplanes, niter, threshold,
                multiscale, nterms, gridder, deconvolver, solint, calmode, atrous, loop):

    ll = loop
    imagename = vis.replace('.ms', '') + '_im_%d' % (ll + restart_no)
    regionfile = imagename + ".casabox"
    caltable = vis.replace('.ms', '') + '.gcal%d' % (ll + restart_no + 1)

    if ll == 0 and not os.path.exists(regionfile):
        imagename += '_nomask'
        do_gaincal = False
    else:
        do_gaincal = True

    if nterms[ll] > 1:
        bdsmname = imagename + ".image.tt0"
    else:
        bdsmname = imagename + ".image"

    if not os.path.exists(bdsmname):
        logger.error("Image {0} doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(bdsmname,ll))
        return True
    else:
        # If it's the first round of selfcal and regionfile is blank, only run
        # BDSF and quit.
        if atrous[ll]:
            atrous_str = '--atrous-do'
        else:
            atrous_str = ''

        os.system('/usr/bin/python bdsm_model.py {} {} --thresh-isl 20 '
        '--thresh-pix 10 {} --clobber --adaptive-rms-box '
        '--rms-map'.format(bdsmname, regionfile, atrous_str))

        if do_gaincal:
            predict_model(vis, imagename, imsize, cell, gridder, wprojplanes,
                      deconvolver, robust, niter, multiscale, threshold, nterms,
                      regionfile,loop)

            gaincal(vis=vis, caltable=caltable, selectdata=False, solint=solint[ll],
                calmode=calmode[ll], append=False, parang=True)

        return do_gaincal


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])
    params = taskvals['selfcal']

    if 'loop' not in params:
        params['loop'] = 0

    for arg in ['multiscale','nterms','calmode','atrous']:
        params[arg] = [params[arg]] * len(params['niter'])

    ran_gaincal = selfcal_part2(**params)

    if ran_gaincal:
        config_parser.overwrite_config(args['config'], conf_dict={'loop' : params['loop']+1},  conf_sec='selfcal')
