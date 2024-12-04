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

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s")

import shutil
from katbeam import JimBeam
from casatools import image
ia = image()


def do_pb_corr(inpimage, pbthreshold=0, pbband='LBand'):
    """
    Given the input CASA image, outputs a katbeam corrected image, optionally
    cutoff at a specified threshold.

    Inputs:
    inpimage        Input CASA image name, str
    pbthreshold     Cutoff threshold to mask the PB, float
    pbband          Band at which to generate the PB

    Outputs:
    None
    """

    pbcorimage = inpimage.replace('.image', '.katbeam_pbcor.image')
    pbimage = inpimage.replace('.image', '.katbeam.pb')

    ia.open(inpimage)
    csys = ia.coordsys().torecord()
    imgdata = ia.getchunk()
    shape = ia.shape()

    cx, cy = shape[0]//2, shape[1]//2

    # Size of each pixel
    cdelt = np.abs(csys['direction0']['cdelt'][0])
    unit = csys['direction0']['units'][0]

    if unit == 'rad':
        cdelt = np.rad2deg(cdelt)
    elif unit == "'": #arcmin
        cdelt /= 60.

    if pbband == 'LBand':
        PBeam = JimBeam('MKAT-AA-L-JIM-2020')
    elif pbband == 'SBand':
        PBeam = JimBeam('MKAT-AA-S-JIM-2020')
    elif pbband == 'UHF':
        PBeam = JimBeam('MKAT-AA-UHF-JIM-2020')
    else:
        logger.error('Input pbband not recognized. Must be one of LBand, SBand or UHF. Defaulting to LBand.')
        PBeam = JimBeam('MKAT-AA-L-JIM-2020')

    x = np.linspace(-cx, cx+1, shape[0])
    y = np.linspace(-cy, cy+1, shape[1])

    xx, yy = np.meshgrid(x, y)

    # Convert pixels into separation in degrees
    xx *= cdelt
    yy *= cdelt

    # Generate the PB image/cube
    beam_I = np.empty(shape)
    for i in range(shape[-1]):
        beam_I[:,:,0,i] = PBeam.I(xx, yy, (ia.toworld([0,0,0,i])['numeric'][-1])/1e6)

    ia.close()

    pbcor_imgdata = imgdata/beam_I

    # Mask below the threshold
    if pbthreshold > 0:
        pbcor_imgdata[beam_I < pbthreshold] = np.nan
        #beam_I[beam_I < pbthreshold] = np.nan

    shutil.copytree(inpimage, pbimage)
    ia.open(pbimage)
    ia.putchunk(beam_I)
    ia.close()

    shutil.copytree(inpimage, pbcorimage)
    ia.open(pbcorimage)
    ia.putchunk(pbcor_imgdata)
    ia.close()


def science_image(vis, spw, cell, robust, imsize, wprojplanes, niter, threshold, multiscale, nterms, gridder, deconvolver, specmode, uvtaper, restfreq, restoringbeam, stokes, mask, rmsmap, outlierfile, keepmms, pbthreshold, pbband, fitspw):

    if ',' in vis:           #currently setting visbase to the CWD in the case of combining tracks
        vis = vis.split(',')
        visbase = os.getcwd().split('/')[-1]
    else:
        visbase = os.path.split(vis.rstrip('/ '))[1] # Get only vis name, not entire path
    extn = '.ms' if keepmms==False else '.mms'
    imagename = visbase.replace(extn, '.science_image') # Images will be produced in $CWD

    if os.path.exists(outlierfile) and open(outlierfile).read() == '':
        outlierfile = ''

    if not (type(threshold) is str and 'Jy' in threshold) and threshold > 1 and os.path.exists(rmsmap):
        stats = imstat(imagename=rmsmap)
        threshold *= stats['min'][0]

    if deconvolver == 'mtmfs':
        imname = imagename + '.image.tt0'
    else:
        imname = imagename + '.image'

    #Disable parallel processing if cube imaging being done (CASA bug)
    parallel = True
    if specmode == 'cube':
        parallel = False
        if fitspw != '':
            spw = fitspw
            if isinstance(vis,list): #in the case of combining tracks, need a list equal to the number of MSs. Why? I don't know, but it didn't work with a single value
                spw = [spw]*len(vis)
                
    if not os.path.exists(imname):

        tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize, cell=cell, stokes=stokes, gridder=gridder, specmode=specmode,
            wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=True,
            weighting='briggs', robust = robust, niter=niter, scales=multiscale,
            restfreq=restfreq, uvtaper = uvtaper, spw=spw,
            threshold=threshold, nterms=nterms, calcpsf=True, mask=mask, outlierfile=outlierfile,
            pbcor=False, pblimit=-1, restoringbeam=restoringbeam, parallel = parallel)

    else:
        logger.warning('Output image "{0}" already exists. Skipping tclean step and applying pb correction.'.format(imname))

    if len(stokes) > 1 and 'I' in stokes.upper():
        logger.warning('Output image "{0}" includes multiple Stokes, but katbeam only applicable to Stokes I. Selecting Stokes I and applying PB correction.'.format(imname))
        stokesI = imname + '.StokesI'
        if not os.path.exists(stokesI):
            imsubimage(imagename=imname, outfile=stokesI, stokes='I')
        imname = stokesI

    if 'I' in stokes.upper():
        do_pb_corr(imname, pbthreshold, pbband)

if __name__ == '__main__':

    args,params = bookkeeping.get_imaging_params()
    params['fitspw'] = config_parser.get_key(args['config'], "image", "fitspw")
    science_image(**params)
    bookkeeping.rename_logs(logfile)
