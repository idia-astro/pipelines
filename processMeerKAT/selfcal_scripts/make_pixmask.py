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

import logging

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


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)
if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    loop,pixmask = mask_image(params['vis'], params['nloops'], params['nterms'], params['loop'])
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')

    if config_parser.has_section(args['config'], 'image'):
        config_parser.overwrite_config(args['config'], conf_dict={'mask' : "'{0}'".format(pixmask)}, conf_sec='image')
