#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

#! /usr/bin/python

"""
Script called from selfcal_part2.py to box for cleaning and to write out the
PyBDSF model.  Parameters need to be changed in this script every run.
"""

import bdsf
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('imagename', help='Name of the input CASA or FITS image')
parser.add_argument('outfile', help='Name of the output catalog file')
parser.add_argument('--adaptive-rms-box', action='store_true')
parser.add_argument('--adaptive-thresh', type=int, default=20)
parser.add_argument('--rms-box-bright', nargs=2, default=(40,5))
parser.add_argument('--atrous-do', action='store_true')
parser.add_argument('--mean-map', default='map')
parser.add_argument('--rms-box', nargs=2, default=(100,30))
parser.add_argument('--rms-map', action='store_true')
parser.add_argument('--thresh', default='hard')
parser.add_argument('--thresh-isl', type=int, default=3)
parser.add_argument('--thresh-pix', type=int, default=7)
parser.add_argument('--format', dest='fmt', default='casabox')
parser.add_argument('--catalog-type', default='srl')
parser.add_argument('--clobber', action='store_true')
parser.add_argument('--residim', action='store_true')
parser.add_argument('--blank-limit', type=float, default=None)

args = parser.parse_args()

# Identify bright sources
img = bdsf.process_image(args.imagename, adaptive_rms_box=args.adaptive_rms_box,
    adaptive_thresh=args.adaptive_thresh, rms_box_bright=args.rms_box_bright,
    advanced_opts=True, fittedimage_clip=3.0, group_tol=0.5,
    group_by_isl=False, atrous_do=args.atrous_do, mean_map=args.mean_map,
    rms_box=args.rms_box, rms_map=args.rms_map, thresh=args.thresh,
    thresh_isl=args.thresh_isl, thresh_pix=args.thresh_pix,
    blank_limit=args.blank_limit)

# Write out catalog
img.write_catalog(outfile=args.outfile, format=args.fmt, clobber=args.clobber,
    catalog_type=args.catalog_type)

#modelname, ext = args.imagename.partition('.')[:2]

# Write out model image
#modelname += '_bdsf_model' + ext
modelname = args.imagename + '.bdsf_model'
if 'FITS' in args.imagename.upper():
    img.export_image(outfile=modelname, img_format='fits',
        img_type='gaus_model')

if '.image' in args.imagename.lower():
    img.export_image(outfile=modelname, img_format='casa',
        img_type='gaus_model')
