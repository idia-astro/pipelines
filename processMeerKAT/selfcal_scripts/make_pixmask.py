#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import shutil
import os

import config_parser
from config_parser import validate_args as va
from cal_scripts import bookkeeping
import processMeerKAT

import logging

#@bookkeeping.run_script
def mask_image(vis, nloops, atrous, nterms, restart_no, loop):
    basename = vis.replace('.ms', '') + '_im_%d'
    regionfile = basename % (loop + restart_no) + ".casabox"
    maskfile = basename % (loop + restart_no) + ".islmask"
    pixmask = basename % (loop + restart_no) + ".pixmask"

    if loop == 0:
        imagename = basename % (loop + restart_no)
    else:
        # Using the previous image to make curent mask
        imagename = basename % (loop - 1 + restart_no)

    # At this point the mask will always exist, since pybdsf is always run before this script.
    # So need to check that if none of .image.tt0 or .image exists, check for _nomask.image or _nomask.image.tt0
    if loop == 0 and not any([os.path.exists(ff) for ff in [imagename + '.image.tt0', imagename + '.image']]):
        imagename += '_nomask'

    if nterms[loop] > 1:
        bdsfname = imagename + ".image.tt0"
    else:
        bdsfname = imagename + ".image"

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

    return loop


logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)
if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    loop = mask_image(params['vis'], params['nloops'], params['atrous'], params['nterms'], params['restart_no'], params['loop'])
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')
