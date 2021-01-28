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
import bdsf

logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)


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


if __name__ == '__main__':
    args,params = bookkeeping.get_selfcal_params()
    loop,rmsmap = run_bdsf(params['vis'], params['nloops'], params['nterms'], params['loop'], params['threshold'], params['imsize'])
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')

    if config_parser.has_section(args['config'], 'image'):
        config_parser.overwrite_config(args['config'], conf_dict={'rmsmap' : "'{0}'".format(rmsmap)}, conf_sec='image')
