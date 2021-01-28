#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import os

import config_parser
import bookkeeping

def science_image(vis, cell, robust, imsize, wprojplanes, niter, threshold, multiscale, nterms, gridder, deconvolver, restoringbeam, specmode, stokes, mask, rmsmap):

    visbase = os.path.split(vis.rstrip('/ '))[1] # Get only vis name, not entire path
    imagename = visbase.replace('.mms', '.science_image') # Images will be produced in $CWD

    if not (type(threshold) is str and 'Jy' in threshold) and threshold > 1 and os.path.exists(rmsmap):
        stats = imstat(imagename=rmsmap)
        threshold *= stats['min'][0]

    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
        imsize=imsize, cell=cell, stokes=stokes, gridder=gridder, specmode=specmode,
        wprojplanes = wprojplanes, deconvolver = deconvolver, restoration=True,
        weighting='briggs', robust = robust, niter=niter, scales=multiscale,
        threshold=threshold, nterms=nterms, calcpsf=True, mask=mask,
        pblimit=-1, restoringbeam=restoringbeam, parallel = True)

if __name__ == '__main__':

    args,params = bookkeeping.get_imaging_params()
    science_image(**params)
