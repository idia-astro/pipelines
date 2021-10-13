#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import shutil
import os
import re

import config_parser
from config_parser import validate_args as va
import bookkeeping
import processMeerKAT

from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.wcs import WCS

from casatasks import *
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

def selfcal_part2(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter, threshold, uvrange,
                  nterms, gridder, deconvolver, solint, calmode, discard_nloops, gaintype, outlier_threshold, flag):

    imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,_,_ = bookkeeping.get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,threshold,step='predict')

    if calmode[loop] != '':
        if os.path.exists(caltable):
            logger.info('Caltable {0} exists. Not overwriting, continuing to next loop.'.format(caltable))
        else:
            tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
                    imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
                    wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop],
                    weighting='briggs', robust = robust[loop], threshold=threshold[loop],
                    nterms=nterms[loop], pblimit=-1, mask=pixmask, outlierfile=outlierfile,
                    niter=0, savemodel='modelcolumn', restart=True, # cfcache=cfcache,
                    restoration=False, calcpsf=False, calcres=False, parallel = False)

            solnorm = 'a' in calmode[loop]
            normtype='median' #if solnorm else 'mean'

            gaincal(vis=vis, caltable=caltable, selectdata=True, refant = refant, solint=solint[loop], solnorm=solnorm,
                    normtype=normtype,
                    gaintype=gaintype[loop],
                    uvrange=uvrange[loop],
                    gaintable=prev_caltables,
                    calmode=calmode[loop], append=False, parang=False)
    else:
        logger.warning("Skipping selfcal loop {0} since calmode == ''.".format(loop))

def pybdsf(imbase,rmsfile,imagename,outimage,thresh,maskfile,cat,trim_box=None,write_all=True):

    fitsname = outimage

    #Force source finding to be on FITS file for stability (e.g. avoiding CASA non-standard header keywords)
    if write_all:
        fitsname = imagename + '.fits'
        if not os.path.exists(fitsname):
            exportfits(imagename = outimage, fitsimage=fitsname)

    img = bdsf.process_image(fitsname, adaptive_rms_box=True,
        rms_box_bright=(40,5), advanced_opts=True, fittedimage_clip=3.0,
        group_tol=0.5, group_by_isl=False, mean_map='map',
        rms_box=(100,30), rms_map=True, thresh='hard', thresh_isl=thresh, thresh_pix=thresh,
        blank_limit=1e-10, trim_box=trim_box)

    # Write out island mask and FITS catalog
    img.export_image(outfile=maskfile, img_type='island_mask', img_format='casa', clobber=True)
    img.write_catalog(outfile=cat, format='fits', clobber=True, catalog_type='srl')

    if write_all:
        regionfile = imbase % loop + ".casabox"
        ascii = imbase % loop + ".ascii"

        #Write out catalogs
        img.write_catalog(outfile=regionfile, format='casabox', clobber=True, catalog_type='srl')
        img.write_catalog(outfile=ascii, format='ascii', clobber=True, catalog_type='srl')

        # Write out RMS image
        img.export_image(outfile=rmsfile, img_type='rms', img_format='casa', clobber=True)

def find_outliers(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter, threshold, uvrange,
                  nterms, gridder, deconvolver, solint, calmode, discard_nloops, gaintype, outlier_threshold, flag):

    local = locals()
    imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,thresh,maskfile = bookkeeping.get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,threshold,step='bdsf')
    cat = imagename + ".catalog.fits"
    outlierfile_all = 'outliers.txt'
    fitsname = imagename + '.fits'
    outlier_imsize = 128
    outlier_snr = 50

    pybdsf(imbase,rmsfile,imagename,outimage,thresh,maskfile,cat)

    if outlier_threshold != '' and outlier_threshold != 0:
        # Write initial outlier file (over entire initial image) if it doesn't already exist
        if loop == 0 and not os.path.exists(outlierfile_all):
            tab=fits.open(cat)
            data = tab[1].data
            tab.close()

            if outlier_threshold > 1.0:
                metric = data['Total_flux']/data['E_Total_flux']
            else:
                metric = data['Total_flux']

            outliers=data[metric > outlier_threshold]
            out = open(outlierfile_all,'w')
            mask = 'mask={0}'.format(pixmask) if pixmask != '' else ''

            for i,pos in enumerate(outliers):
                SkyPos = SkyCoord(ra=pos['RA'],dec=pos['Dec'],unit='deg,deg')
                phasecenter = 'J2000 {0}'.format(SkyPos.to_string('hmsdms'))
                out.write("""
                imagename={0}_outlier{1}
                imsize=[{2},{2}]
                cell=[1.0arcsec,1.0arcsec]
                phasecenter={3}
                nterms=3
                gridder=standard
                {4}\n""".format(imbase%(loop+1),i,outlier_imsize,phasecenter,mask))

            out.close()

        if os.path.exists(outlierfile_all):
            logger.info("All 'outliers' within field written to '{0}'.".format(outlierfile_all))
        else:
            logger.error("Outlier file '{0}' doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(outlierfile_all,loop))
            sys.exit(1)

        #Write outlier file specific to this loop
        outliers=open(outlierfile_all).read()
        out = open(outlierfile,'w')

        img = fits.open(fitsname)
        head = img[0].header
        img.close()

        #Update header to reflect next loop, and pop degenerate axes
        imsize=params['imsize'][loop+1]
        cell=params['cell'][loop+1]
        if type(imsize) is not list:
            imsize = [imsize, imsize]
        if type(cell) is not list:
            cell = [cell, cell]

        head['NAXIS1']=imsize[0]
        head['NAXIS2']=imsize[1]
        head['CRPIX1'] = int(imsize[0]/2 + 1)
        head['CRPIX2'] = int(imsize[1]/2 + 1)

        xdelt = qa.convert(cell[0],'deg')['value']
        ydelt = qa.convert(cell[1],'deg')['value']
        if head['CDELT1'] < 0:
            head['CDELT1'] = -xdelt
        else:
            head['CDELT1'] = xdelt
        if head['CDELT2'] < 0:
            head['CDELT2'] = -ydelt
        else:
            head['CDELT2'] = ydelt

        head['NAXIS']=2
        for axis in ['NAXIS3','NAXIS4']:
            if axis in head.keys():
                head.pop(head.index(axis))

        w = WCS(head)
        r=re.compile(r'phasecenter=J2000 (?P<ra>.*?) (?P<dec>.*?)\n')
        positions=[m.groupdict() for m in r.finditer(outliers)]
        outlier_bases = re.findall(r'imagename=(.*)\n',outliers)

        #Only write positions for this loop outside imaging area
        for i,position in enumerate(positions):
            pos=SkyCoord(**position)
            if not w.footprint_contains(pos):
                mask= ''
                phasecenter = 'J2000 {0}'.format(pos.to_string('hmsdms'))
                
                if loop > 0:
                    base = outlier_bases[i]
                    im = base + '.image'
                    outlier_cat = base + ".catalog.fits"
                    outlier_mask = '{0}.islmask'.format(base)

                    if nterms[loop] > 1 and deconvolver[loop] == 'mtmfs':
                        im += '.tt0'

                    if os.path.exists(im):
                        #Run PyBDSF on outlier and update mask
                        pybdsf(imbase,rmsfile,base,im,outlier_snr,outlier_mask,outlier_cat,write_all=False)
                        outlier_pixmask = mask_image(**local,outlier_base=base,outlier_image=im)
                    else:
                        #Use main image, run PyBDSF on box around outlier, and update mask
                        ia.open(outimage)
                        pix = ia.topixel(pos.to_string('hmsdms'))['numeric']
                        x,y = pix[0],pix[1]
                        delta = outlier_imsize/2
                        trim_box = (x-delta,x+delta,y-delta,y+delta)
                        ia.close()

                        pybdsf(imbase,rmsfile,imagename,outimage,outlier_snr,outlier_mask,outlier_cat,trim_box=trim_box,write_all=False)
                        outlier_pixmask = mask_image(**local,outlier_base=base)

                    mask = 'mask={0}'.format(outlier_pixmask)

                    #If catalog written, take new PyBDSF position closest to previous position
                    if os.path.exists(outlier_cat):
                        tab=fits.open(outlier_cat)
                        data = tab[1].data
                        tab.close()
                        cat_positions = SkyCoord(ra=data['RA'],dec=data['Dec'],unit='deg,deg')
                        row,_,_ = pos.match_to_catalog_sky(cat_positions)
                        phasecenter = 'J2000 {0}'.format(cat_positions[row].to_string('hmsdms'))
                    else:
                        logger.warning("PyBDSF catalogue '{0}' not created. Using old position and mask.".format(outlier_cat))
                        mask = 'mask={0}'.format(pixmask)

                elif pixmask != '':
                    #Use original mask
                    mask = 'mask={0}'.format(pixmask)

                out.write("""
                imagename={0}_outlier{1}
                imsize=[{2},{2}]
                cell=[1.0arcsec,1.0arcsec]
                phasecenter={3}
                nterms=3
                gridder=standard
                {4}\n""".format(imbase%(loop+1),i,outlier_imsize,phasecenter,mask))

        out.close()

        if os.path.exists(outlierfile):
            logger.info("Outlier file '{0}' written.".format(outlierfile))
        else:
            logger.error("Outlier file '{0}' doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(outlierfile,loop))
            sys.exit(1)

    return rmsfile,outlierfile

def mask_image(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter, threshold, uvrange, nterms, gridder,
                  deconvolver, solint, calmode, discard_nloops, gaintype, outlier_threshold, flag, outlier_base='', outlier_image=''):

    imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,thresh,maskfile = bookkeeping.get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,threshold,step='mask')

    if outlier_base != '':
        maskfile = outlier_base + '.islmask'
        pixmask = outlier_base + '.pixmask'
    if outlier_image != '':
        outimage = outlier_image

    if pixmask != '':
        # Make the pixel mask, copy it over to an image to get the right coords,
        # export mask to its own image. Adapted from Brad's bdsf masking script.

        # Using a complicated name clashes with makemask internal logic, so copy over into a temp name
        tmpisl = 'tmp.isl'
        tmpim = 'tmp.im'
        tmpmask = 'tmp.mask'
        for im in [tmpisl, tmpim, tmpmask]:
            if os.path.exists(im):
                shutil.rmtree(im)

        shutil.copytree(maskfile, tmpisl)
        shutil.copytree(outimage, tmpim)

        ia.open(tmpisl)
        mask_expression = '%s > 1e-10' % (tmpisl)
        ia.calcmask(mask_expression, name=tmpmask)
        ia.done()

        # Copy mask to an image with right coords
        makemask(mode='copy', inpimage=tmpisl, inpmask='%s:%s' % (tmpisl, tmpmask),
                    output = '%s:%s' % (tmpim, tmpmask), overwrite=True)
        # Copy mask out to its own image
        makemask(mode='copy', inpimage=outimage, inpmask='%s:%s' % (tmpim, tmpmask),
                    output = pixmask, overwrite=True)

        # clean up
        for im in [tmpisl, tmpim, tmpmask]:
            if os.path.exists(im):
                shutil.rmtree(im)

    return pixmask

if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    loop = params['loop']

    selfcal_part2(**params)
    rmsmap,outlierfile = find_outliers(**params)
    pixmask = mask_image(**params)

    loop += 1

    if config_parser.has_section(args['config'], 'image'):
        config_parser.overwrite_config(args['config'], conf_dict={'mask' : "'{0}'".format(pixmask)}, conf_sec='image')
        config_parser.overwrite_config(args['config'], conf_dict={'rmsmap' : "'{0}'".format(rmsmap)}, conf_sec='image')
        config_parser.overwrite_config(args['config'], conf_dict={'outlierfile' : "'{0}'".format(outlierfile)}, conf_sec='image')
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')

    bookkeeping.rename_logs(logfile)
