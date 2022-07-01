#Copyright (C) 2022 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import os
import numpy as np

import config_parser
import bookkeeping

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
import casampi

import shutil
from katbeam import JimBeam
from casatools import image
ia = image()


def do_pb_corr(inpimage, pbthreshold=0):
    """
    Given the input CASA image, outputs a katbeam corrected image, optionally
    cutoff at a specified threshold.

    Inputs:
    inpimage    Input CASA image name, str
    threshold   Cutoff threshold to mask the PB, float

    Outputs:
    None
    """

    pbcorimage = inpimage.replace('.image', '.katbeam_pbcor.image')
    pbimage = inpimage.replace('.image', '.katbeam.pb')

    LBandBeam = JimBeam('MKAT-AA-L-JIM-2020')

    ia.open(inpimage)
    csys = ia.coordsys().torecord()
    imgdata = ia.getchunk()
    shape = ia.shape()
    ia.close()

    cx, cy = shape[0]//2, shape[1]//2

    # Size of each pixel
    cdelt = np.abs(csys['direction0']['cdelt'][0])
    unit = csys['direction0']['units'][0]

    if unit == 'rad':
        cdelt = np.rad2deg(cdelt)
    elif unit == "'": #arcmin
        cdelt /= 60.

    # Frequency of image, convert from Hz to MHz
    try:
        freq = csys['spectral1']['wcs']['crval']/1e6
    except KeyError:
        freq = csys['spectral2']['wcs']['crval']/1e6


    x = np.linspace(-cx, cx+1, shape[0])
    y = np.linspace(-cy, cy+1, shape[1])

    xx, yy = np.meshgrid(x, y)

    # Convert pixels into separation in degrees
    xx *= cdelt
    yy *= cdelt

    # Generate the 2D PB image
    beam_I = LBandBeam.I(xx, yy, freq)

    # Match shape with image data for PB correction
    if len(shape) == 4:
        beam_I = beam_I[:, :, None, None]

    pbcor_imgdata = imgdata/beam_I

    # Mask below the threshold
    if pbthreshold > 0:
        pbcor_imgdata[beam_I < pbthreshold] = np.nan
        beam_I[beam_I < pbthreshold] = np.nan

    shutil.copytree(inpimage, pbimage)
    ia.open(pbimage)
    ia.putchunk(beam_I)
    ia.close()

    shutil.copytree(inpimage, pbcorimage)
    ia.open(pbcorimage)
    ia.putchunk(pbcor_imgdata)
    ia.close()


def science_image(vis, cell, robust, imsize, wprojplanes, niter, threshold, multiscale, nterms, gridder, deconvolver, restoringbeam, specmode, stokes, mask, rmsmap, outlierfile, keepmms, pbthreshold):

    visbase = os.path.split(vis.rstrip('/ '))[1] # Get only vis name, not entire path
    extn = '.ms' if keepmms==False else '.mms'
    imagename = visbase.replace(extn, '.science_image') # Images will be produced in $CWD

    if not (type(threshold) is str and 'Jy' in threshold) and threshold > 1 and os.path.exists(rmsmap):
        stats = imstat(imagename=rmsmap)
        threshold *= stats['min'][0]

    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
        imsize=imsize, cell=cell, stokes=stokes, gridder=gridder, specmode=specmode,
        wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=True,
        weighting='briggs', robust = robust, niter=niter, scales=multiscale,
        threshold=threshold, nterms=nterms, calcpsf=True, mask=mask, outlierfile=outlierfile,
        pblimit=-1, restoringbeam=restoringbeam, parallel = True)

    if nterms > 1:
        imname = imagename + '.image.tt0'
    elif nterms == 1:
        imname = imagename + '.image'

    do_pb_corr(imname, pbthreshold)

if __name__ == '__main__':

    args,params = bookkeeping.get_imaging_params()
    science_image(**params)
    bookkeeping.rename_logs(logfile)
