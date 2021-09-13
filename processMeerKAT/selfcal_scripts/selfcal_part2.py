#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import shutil
import os

import config_parser
from config_parser import validate_args as va
import bookkeeping
import processMeerKAT

from casatasks import *
from casatools import *
from casatools import image,quanta
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
ia=image()
qa=quanta()

import bdsf
import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def predict_model(vis, imagename, imsize, cell, gridder, wprojplanes, deconvolver,
                  robust, niter, threshold, nterms, regionfile, loop, cfcache):

    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
            wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop],
            weighting='briggs', robust = robust[loop], threshold=threshold[loop],
            nterms=nterms[loop], pblimit=-1, mask=regionfile,# cfcache=cfcache,
            niter=0, savemodel='modelcolumn', restart=True,
            restoration=False, calcpsf=False, calcres=False, parallel = False)

def selfcal_part2(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter,
                  threshold, uvrange, nterms, gridder, deconvolver, solint, calmode, discard_loop0, gaintype, flag):

    visbase = os.path.split(vis.rstrip('/ '))[1] # Get only vis name, not entire path
    basename = visbase.replace('.mms', '') + '_im_%d' # Images will be produced in $CWD
    imagename = basename % loop
    pixmask = basename % loop + ".pixmask"
    rmsfile = basename % loop + ".rms"
    caltable = visbase.replace('.mms', '') + '.gcal%d' % loop
    prev_caltables = sorted(glob.glob('*.gcal?'))
    caltable0 = visbase.replace('.mms', '') + '.gcal0'
    cfcache = visbase.replace('.mms', '') + '.cf'

    if discard_loop0 and caltable0 in prev_caltables:
        prev_caltables.pop(prev_caltables.index(caltable0))

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
    if not os.path.exists(fitsname):
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
                      deconvolver, robust, niter, threshold, nterms, pixmask,loop,cfcache)

            solnorm = 'a' in calmode[loop]
            normtype='median' #if solnorm else 'mean'

            gaincal(vis=vis, caltable=caltable, selectdata=True, refant = refant, solint=solint[loop], solnorm=solnorm,
                    normtype=normtype,
                    gaintype=gaintype[loop],
                    uvrange=uvrange[loop],
                    gaintable=prev_caltables,
                    calmode=calmode[loop], append=False, parang=False)

            loop += 1

        return loop

def run_bdsf(vis, nloops, nterms, loop, threshold, imsize):

    logger.info('Beginning {0}.'.format(sys.argv[0]))
    visbase = os.path.split(vis.rstrip('/ '))[1] # Get only vis name, not entire path
    basename = visbase.replace('.mms', '') + '_im_%d' # Images will be produced in $CWD
    regionfile = basename % loop + ".casabox"
    ascii = basename % loop + ".ascii"
    maskfile = basename % loop + ".islmask"
    rmsfile = basename % loop + ".rms"

    if loop == 0:
        imagename = basename % loop
    else:
        # Using the previous image to make curent mask
        imagename = basename % (loop - 1)

    if loop == 0 and not os.path.exists(maskfile):
        imagename += '_nomask'

    bdsfname = imagename + '.fits'

    if not (type(threshold[loop]) is str and 'Jy' in threshold[loop]) and threshold[loop] > 1:
        thresh = threshold[loop]
    elif len(threshold) > loop+1 and not (type(threshold[loop+1]) is str and 'Jy' in threshold[loop+1]) and threshold[loop+1] > 1:
        thresh = threshold[loop+1]
    else:
        thresh = 10

    # Identify bright sources
    img = bdsf.process_image(bdsfname, adaptive_rms_box=True,
        rms_box_bright=(40,5), advanced_opts=True, fittedimage_clip=3.0,
        group_tol=0.5, group_by_isl=False, mean_map='map',
        rms_box=(100,30), rms_map=True, thresh='hard', thresh_isl=thresh, thresh_pix=thresh,
        blank_limit=1e-10)

    # Write out catalog
    img.write_catalog(outfile=regionfile, format='casabox', clobber=True, catalog_type='srl')
    img.write_catalog(outfile=ascii, format='ascii', clobber=True, catalog_type='srl')
    img.export_image(outfile=maskfile, img_type='island_mask', img_format='casa', clobber=True)

    # Write out model image
    modelname = imagename + '.bdsf_model.fits'
    img.export_image(outfile=modelname, clobber=True, img_format='fits', img_type='gaus_model')

    # Write out RMS image
    img.export_image(outfile=rmsfile, img_type='rms', img_format='casa', clobber=True)

    logger.info('Completed {0}.'.format(sys.argv[0]))
    return loop,rmsfile

def mask_image(vis, nloops, nterms, loop):

    visbase = os.path.split(vis.rstrip('/ '))[1] # Get only vis name, not entire path
    basename = visbase.replace('.mms', '') + '_im_%d' # Images will be produced in $CWD
    regionfile = basename % loop + ".casabox"
    maskfile = basename % loop + ".islmask"
    pixmask = basename % loop + ".pixmask"

    if loop == 0:
        imagename = basename % loop
    else:
        # Using the previous image to make curent mask
        imagename = basename % (loop - 1)

    # At this point the mask will always exist, since pybdsf is always run before this script.
    # So need to check that if none of .image.tt0 or .image exists, check for _nomask.image or _nomask.image.tt0
    if loop == 0 and not any([os.path.exists(ff) for ff in [imagename + '.image.tt0', imagename + '.image']]):
        imagename += '_nomask'

    # Note: bdsfname refers to a FITS file in run_bdsf.py, but to the CASA image here.
    # because bdsf sometimes barfs on casa images with non-standard header keywords, but
    # can process the corresponding FITS file.
    #
    # However in this script we are doing some header manipulation and so all the images need
    # to be in the CASA format.
    if nterms[loop] > 1:
        bdsfname = imagename + ".image.tt0"
    else:
        bdsfname = imagename + ".image"

    #bdsfname = imagename + '.fits'

    if not os.path.exists(bdsfname):
        raise IOError("Image %s does not exist" % bdsfname)

    # Make the pixel mask, copy it over to an image to get the right coords, export mask to it's
    # own image. Adapted from Brad's bdsf masking script.

    # Using a complicated name clashes with makemask internal logic, so copy over into a temp
    # name
    tmpisl = 'tmp.isl'
    tmpim = 'tmp.im'
    tmpmask = 'tmp.mask'
    for im in [tmpisl, tmpim, tmpmask]:
        if os.path.exists(im):
            shutil.rmtree(im)

    shutil.copytree(maskfile, tmpisl)
    shutil.copytree(bdsfname, tmpim)

    ia.open(tmpisl)
    mask_expression = '%s > 1e-10' % (tmpisl)
    ia.calcmask(mask_expression, name=tmpmask)
    ia.done()

    # Copy mask to an image with right coords
    makemask(mode='copy', inpimage=tmpisl, inpmask='%s:%s' % (tmpisl, tmpmask),
                output = '%s:%s' % (tmpim, tmpmask))
    # Copy mask out to it's own image
    makemask(mode='copy', inpimage=bdsfname, inpmask='%s:%s' % (tmpim, tmpmask),
                output = pixmask, overwrite=True)
    exportfits(imagename=pixmask, fitsimage=pixmask+'.fits', overwrite=True)

    # clean up
    for im in [tmpisl, tmpim, tmpmask]:
        if os.path.exists(im):
            shutil.rmtree(im)

    return loop,pixmask

if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    loop = selfcal_part2(**params)
    loop,rmsmap = run_bdsf(params['vis'], params['nloops'], params['nterms'], loop, params['threshold'], params['imsize'])
    loop,pixmask = mask_image(params['vis'], params['nloops'], params['nterms'], loop)

    if config_parser.has_section(args['config'], 'image'):
        config_parser.overwrite_config(args['config'], conf_dict={'mask' : "'{0}'".format(pixmask)}, conf_sec='image')
        config_parser.overwrite_config(args['config'], conf_dict={'rmsmap' : "'{0}'".format(rmsmap)}, conf_sec='image')
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')

    bookkeeping.rename_logs(logfile)
