#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import shutil
import os

import config_parser
from config_parser import validate_args as va

def predict_model(vis, imagename, imsize, cell, gridder, wprojplanes,
                      deconvolver, robust, niter, multiscale, threshold, nterms,
                      regionfile):

    # Rename image products for predict
    for im in glob.glob(imagename + ".*"):
        ext = im.replace(imagename, '')
        #name, ext = os.path.splitext(im)
        out = 'tmpim.im' + ext
        shutil.copytree(im, out)

    # Remove image and model image
    shutil.rmtree(glob.glob('tmpim.im.image*'))
    shutil.rmtree(glob.glob('tmpim.im.model*'))

    startmodel = glob.glob(imagename + '.model*')

    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename='tmpim.im',
            imsize=imsize, cell=cell, stokes='I', gridder=gridder,
            wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=False,
            weighting='briggs', robust = robust, niter=0, multiscale=multiscale[ll],
            threshold=threshold[ll], nterms=nterms[ll], calcpsf=False, calcres=False,
            startmodel = startmodel, savemodel='modelcolumn', pblimit=-1,
            mask=regionfile, parallel = False)


    shutil.rmtree("tmpim.im*")

def selfcal_part2(vis, imagename, imsize, cell, gridder, wprojplanes,
                   deconvolver, robust, niter, multiscale, threshold,
                   nterms, regionfile, restart_no, nloop, solint, calmode,
                   atrous, loop):

    ll = params['loop']

    # If it's the first round of selfcal and regionfile is blank, only run
    # BDSF and quit.
    if len(regionfile) == 0:
        do_gaincal = False
    else:
        do_gaincal = True

    if atrous[ll]:
        atrous_str = '--atrous-do'
    else:
        atrous_str = ''

    if nterms[ll] > 1:
        bdsmname = imagename + ".image.tt0"
    else:
        bdsmname = imagename + ".image"

    regionfile = imagename + ".casabox"
    os.system('/usr/bin/python bdsm_model.py {} {} --thresh-isl 20 '
        '--thresh-pix 10 {} --clobber --adaptive-rms-box '
        '--rms-map'.format(bdsmname, regionfile, atrous_str))

    if do_gaincal:
        predict_model(vis, imagename, imsize, cell, gridder, wprojplanes,
                      deconvolver, robust, niter, multiscale, threshold, nterms,
                      regionfile)

        gaincal(vis=vis, caltable=caltable, selectdata=False, solint=solint[ll],
                calmode=calmode[ll], append=False, parang=True)

    return do_gaincal


if __name__ == '__main__':
    # Get the name of the config file
    args = config_parser.parse_args()

    # Parse config file
    taskvals, config = config_parser.parse_config(args['config'])
    params = taskvals['selfcal']

    for arg in ['multiscale','nterms','calmode','atrous']:
        params[arg] = [params[arg]] * len(params['niter'])

    ran_gaincal = selfcal_part2(**params)

    if ran_gaincal:
        config_parser.overwrite_config(args['config'], conf_dict={'loop' : params['loop']+1})
