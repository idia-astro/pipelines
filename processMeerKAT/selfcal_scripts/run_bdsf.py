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


def run_bdsf(vis, nloops, atrous, nterms, restart_no, loop):
    basename = vis.replace('.ms', '') + '_im_%d'
    regionfile = basename % (loop + restart_no) + ".casabox"
    maskfile = basename % (loop + restart_no) + ".islmask"

    if loop == 0:
        imagename = basename % (loop + restart_no)
    else:
        # Using the previous image to make curent mask
        imagename = basename % (loop - 1 + restart_no)

    if loop == 0 and not os.path.exists(maskfile):
        imagename += '_nomask'

    #if nterms[loop] > 1:
    #    bdsfname = imagename + ".image.tt0"
    #else:
    #    bdsfname = imagename + ".image"

    bdsfname = imagename + '.fits'

    # Identify bright sources
    img = bdsf.process_image(bdsfname, adaptive_rms_box=True,
        rms_box_bright=(40,5), advanced_opts=True, fittedimage_clip=3.0,
        group_tol=0.5, group_by_isl=False, atrous_do=atrous[loop], mean_map='map',
        rms_box=(100,30), rms_map=True, thresh='hard', thresh_isl=10, thresh_pix=10,
        blank_limit=1e-10)

    # Write out catalog
    img.write_catalog(outfile=regionfile, format='casabox', clobber=True,
        catalog_type='srl')
    img.export_image(outfile=maskfile, img_type='island_mask', img_format='casa', clobber=True)

    # Write out model image
    modelname = imagename + '.bdsf_model.fits'
    img.export_image(outfile=modelname, clobber=True, img_format='fits',
        img_type='gaus_model')

    #if 'fits' in imagename.lower():
    #    img.export_image(outfile=modelname, clobber=True, img_format='fits',
    #        img_type='gaus_model')

    #if '.image' in imagename.lower():
    #    img.export_image(outfile=modelname, clobber=True, img_format='casa',
    #        img_type='gaus_model')


    return loop



if __name__ == '__main__':
    args,params = bookkeeping.get_selfcal_params()
    loop = run_bdsf(params['vis'], params['nloops'], params['atrous'], params['nterms'], params['restart_no'], params['loop'])
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')
