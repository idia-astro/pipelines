#Copyright (C) 2022 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import shutil
import os
import re
import numpy as np

import config_parser
from config_parser import validate_args as va
import bookkeeping
import processMeerKAT

from astropy.coordinates import SkyCoord
from astropy.io import fits
from astropy.wcs import WCS
from astropy.units import Quantity

from casatasks import *
from casatools import image,quanta,msmetadata
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
ia=image()
qa=quanta()
msmd=msmetadata()

import bdsf
import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def selfcal_part2(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter, threshold, uvrange,
                  nterms, gridder, deconvolver, solint, calmode, discard_nloops, gaintype, outlier_threshold, outlier_radius, flag):

    imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,_,_,_,_ = bookkeeping.get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,outlier_radius,threshold,step='predict')
    if os.path.exists(outlierfile) and open(outlierfile).read() == '':
        outlierfile = ''

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

def pybdsf(imbase,rmsfile,imagename,outimage,thresh,maskfile,cat,loop,trim_box=None,write_all=True):

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
    img.write_catalog(outfile=cat, format='fits', clobber=True, catalog_type='gaul')

    if write_all:
        regionfile = imbase % loop + ".casabox"
        ascii = imbase % loop + ".ascii"

        #Write out catalogs
        img.write_catalog(outfile=regionfile, format='casabox', clobber=True, catalog_type='srl')
        img.write_catalog(outfile=ascii, format='ascii', clobber=True, catalog_type='gaul')

        # Write out RMS image
        img.export_image(outfile=rmsfile, img_type='rms', img_format='casa', clobber=True)

def find_outliers(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter, threshold, uvrange, nterms,
                  gridder, deconvolver, solint, calmode, discard_nloops, gaintype, outlier_threshold, outlier_radius, flag, step):

    local = locals()
    local.pop('step')
    imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,thresh,maskfile,targetfield,sky_model_radius = bookkeeping.get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,outlier_radius,threshold,step)
    cat = imagename + ".catalog.fits"
    outlierfile_all = 'outliers.txt'
    fitsname = imagename + '.fits'
    outlier_imsize = 128
    outlier_snr = 5
    outlier_min_flux = 1e-3 # 1 mJy

    if step != 'sky':
        pybdsf(imbase,rmsfile,imagename,outimage,thresh,maskfile,cat,loop)

    #Store before potentially updating to mJy
    orig_threshold = outlier_threshold

    if outlier_threshold != '' and outlier_threshold != 0:

        #Take sky positions from RACS
        if step == 'sky':
            #from astroquery.utils.tap.core import TapPlus
            #from astropy.table import vstack

            #Open MS and extract first target centre; use first sub-MS for speed if MMS
            if os.path.exists('{0}/SUBMSS'.format(vis)):
                tmpvis = glob.glob('{0}/SUBMSS/*'.format(vis))[0]
            else:
                tmpvis = vis

            msmd.open(tmpvis)
            dir=msmd.sourcedirs()[str(targetfield)]
            ra=qa.convert(dir['m0'],'deg')['value']
            dec=qa.convert(dir['m1'],'deg')['value']
            if ra < 0:
                ra += 360
            phasecenter=SkyCoord(ra=ra,dec=dec,unit='deg,deg')
            msmd.done()

            cat = 'RACS_local.fits'
            fluxcol = 'total_flux_source'
            efluxcol = 'e_total_flux_source'
            racol = 'ra'
            deccol = 'dec'
            outlier_threshold *= 1e3 #convert from Jy to mJy

            #Extract RACS positions within sky_model_radius
            # try:
            #     casdatap = TapPlus(url="https://casda.csiro.au/casda_vo_tools/tap")
            #     query = "SELECT * FROM  AS110.racs_dr1_sources_galacticcut_v2021_08_v01 where 1=CONTAINS(POINT('ICRS', ra, dec),CIRCLE('ICRS',{0},{1},{2}))".format(ra,dec,sky_model_radius)
            #     job = casdatap.launch_job_async(query)
            #     galcut = job.get_results()
            #     job = casdatap.launch_job_async(query.replace('cut','region'))
            #     galregion = job.get_results()
            #     RACS = vstack([galcut,galregion],join_type='exact')
            #     RACS.write(cat,overwrite=True)
            # except:
            RACS = '{0}/RACS.fits.gz'.format(processMeerKAT.SCRIPT_DIR)
            tmp = fits.open(RACS)
            all_positions = SkyCoord(ra=tmp[1].data[racol],dec=tmp[1].data[deccol],unit='deg,deg')
            tmp[1].data = tmp[1].data[phasecenter.separation(all_positions) < Quantity(sky_model_radius,'deg')]
            tmp.writeto(cat,overwrite=True)
            tmp.close()
            del all_positions #Don't store millions of positions beyond here

            index = loop

        else:
            fluxcol = 'Total_flux'
            efluxcol = 'E_Total_flux'
            racol = 'RA'
            deccol = 'Dec'
            index = loop+1

        # Write initial outlier file
        if step == 'sky':
            tab=fits.open(cat)
            data = tab[1].data
            tab.close()

            if orig_threshold > 1.0:
                metric = data[fluxcol]/data[efluxcol]
            else:
                metric = data[fluxcol]

            outliers=data[metric > outlier_threshold]
            out = open(outlierfile_all,'w')
            mask = 'mask={0}'.format(pixmask)# if pixmask != '' else ''

            for i,row in enumerate(outliers):
                SkyPos = SkyCoord(ra=row[racol],dec=row[deccol],unit='deg,deg')
                position = 'J2000 {0}'.format(SkyPos.to_string('hmsdms'))
                out.write("""
                imagename={0}_outlier{1}
                imsize=[{2},{2}]
                cell=[1.0arcsec,1.0arcsec]
                phasecenter={3}
                nterms={4}
                gridder=standard
                {5}\n""".format(imbase%(index),i,outlier_imsize,position,nterms[loop],mask))

            out.close()

            if os.path.exists(outlierfile_all):
                logger.info("All 'outliers' within field written to '{0}' based on catalog '{1}'.".format(outlierfile_all,os.path.split(cat)[1]))
            else:
                logger.error("Outlier file '{0}' doesn't exist, so sky model build went wrong. Will terminate process.".format(outlierfile_all))
                sys.exit(1)

        outliers=open(outlierfile_all).read()
        out = open(outlierfile,'w')

        if os.path.exists(fitsname):
            img = fits.open(fitsname)
            head = img[0].header
            img.close()
            index = loop+1
        elif step == 'sky':
            img=fits.PrimaryHDU()
            head = img.header
            head['CUNIT1']='deg'
            head['CUNIT2']='deg'
            head['CTYPE1']='RA---SIN'
            head['CTYPE2']='DEC--SIN'
            head['CRVAL1']=ra
            head['CRVAL2']=dec
            index = loop

        #Update header to reflect image, and pop degenerate axes
        imsize=imsize[index]
        cell=cell[index]
        if type(imsize) is not list:
            imsize = [imsize, imsize]
        if type(cell) is not list:
            cell = [cell, cell]

        head['NAXIS1'] = imsize[0]
        head['NAXIS2'] = imsize[1]
        head['CRPIX1'] = int(imsize[0]/2 + 1)
        head['CRPIX2'] = int(imsize[1]/2 + 1)

        xdelt = qa.convert(cell[0],'deg')['value']
        ydelt = qa.convert(cell[1],'deg')['value']
        if 'CDELT1' in head.keys() and head['CDELT1'] > 0:
            head['CDELT1'] = xdelt
        else:
            head['CDELT1'] = -xdelt
        if 'CDELT2' in head.keys() and head['CDELT2'] < 0:
            head['CDELT2'] = -ydelt
        else:
            head['CDELT2'] = ydelt

        head['NAXIS']=2
        for axis in ['NAXIS3','NAXIS4']:
            if axis in head.keys():
                head.pop(head.index(axis))

        #Shrink image by 1% to include outliers on edge of main images
        head['NAXIS1'] = int(head['NAXIS1']*0.99)
        head['NAXIS2'] = int(head['NAXIS2']*0.99)

        w = WCS(head)
        r=re.compile(r'phasecenter=J2000 (?P<ra>.*?) (?P<dec>.*?)\n')
        positions=[m.groupdict() for m in r.finditer(outliers)]
        outlier_bases = re.findall(r'imagename=(.*)\n',outliers)
        num_outliers = 0

        #Only write positions for this loop outside imaging area
        for i,position in enumerate(positions):
            pos=SkyCoord(**position)
            if not w.footprint_contains(pos):
                num_outliers += 1
                mask = 'mask={0}'.format(pixmask)
                phasecenter = 'J2000 {0}'.format(pos.to_string('hmsdms'))

                if step == 'bdsf':
                    base = outlier_bases[i].replace('im_0','im_{0}'.format(index-1))
                    im = base + '.image'
                    outlier_cat = base + ".catalog.fits"
                    outlier_mask = '{0}.islmask'.format(base)

                    if deconvolver[loop] == 'mtmfs':
                        im += '.tt0'

                    if os.path.exists(im):
                        #Run PyBDSF on outlier and update mask
                        pybdsf(imbase,rmsfile,base,im,outlier_snr,outlier_mask,outlier_cat,loop,write_all=False)
                        outlier_pixmask = mask_image(**local,outlier_base=base,outlier_image=im)
                    else:
                        #Use main image, run PyBDSF on box around outlier, and update mask
                        ia.open(outimage)
                        pix = ia.topixel(pos.to_string('hmsdms'))['numeric']
                        x,y = pix[0],pix[1]
                        delta = outlier_imsize/2
                        trim_box = (x-delta,x+delta,y-delta,y+delta)
                        ia.close()

                        if x < 0 or x > imsize[0] or y < 0 or y > imsize[1]:
                            logger.warning("Image '{0}' doesn't exist. Position is outside main image so assuming outlier was dropped and skipping again.".format(im))
                            continue
                        else:
                            logger.warning("Image '{0}' doesn't exist. Assuming source exists inside main image, so running PyBDSF on '{1}' using {2}x{2} pixel trim box.".format(im,outimage,outlier_imsize))

                        pybdsf(imbase,rmsfile,imagename,outimage,outlier_snr,outlier_mask,outlier_cat,loop,trim_box=trim_box,write_all=False)
                        outlier_pixmask = mask_image(**local,outlier_base=base)

                    mask = 'mask={0}'.format(outlier_pixmask)

                    #If catalog written, take new PyBDSF position as brightest Gaussian component, or otherwise, closest to previous position
                    brightest = True
                    if os.path.exists(outlier_cat):
                        tab=fits.open(outlier_cat)
                        data = tab[1].data
                        tab.close()
                        if np.sum(data['Total_flux']) < outlier_min_flux:
                            logger.warning('Total flux in catalogue for "{0}_outlier{1}" < {2} Jy. Excluding outlier.'.format(imbase%(index),i,outlier_min_flux))
                            continue
                        cat_positions = SkyCoord(ra=data['RA'],dec=data['Dec'],unit='deg,deg')
                        if brightest:
                            row = np.where(data['Total_flux'] == np.max(data['Total_flux']))[0][0]
                        else:
                            row,_,_ = pos.match_to_catalog_sky(cat_positions)
                        phasecenter = 'J2000 {0}'.format(cat_positions[row].to_string('hmsdms'))
                    else:
                        logger.warning("PyBDSF catalogue '{0}' not created. Excluding outlier.".format(outlier_cat))
                        continue
                        #Using old position and mask.".format(outlier_cat))
                        #mask = 'mask={0}'.format(pixmask)

                out.write("""
                imagename={0}_outlier{1}
                imsize=[{2},{2}]
                cell=[1.0arcsec,1.0arcsec]
                phasecenter={3}
                nterms={4}
                gridder=standard
                {5}\n""".format(imbase%(index),i,outlier_imsize,phasecenter,nterms[loop],mask))

            else:
                logger.info('Excluding "{0}", as it lies within the image footprint.'.format(outlier_bases[i]))

        out.close()

        if os.path.exists(outlierfile):
            logger.info("Outlier file '{0}' written.".format(outlierfile))
        else:
            logger.error("Outlier file '{0}' doesn't exist, so self-calibration loop {1} failed. Will terminate selfcal process.".format(outlierfile,loop))
            sys.exit(1)

        if num_outliers > 10:
            logger.warning('The number of outliers written to "{0}" is > 10. As this will take some time to image, you may want to set a higher outlier_threshold than {1}, and/or increase your imsize beyond {2}.'.format(outlierfile,orig_threshold,imsize))
        elif num_outliers == 0:
            logger.warning("No outliers identified. Setting outlierfile=''.")
            outlierfile = ''

    return rmsfile,outlierfile

def mask_image(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter, threshold, uvrange, nterms, gridder,
                  deconvolver, solint, calmode, discard_nloops, gaintype, outlier_threshold, outlier_radius, flag, outlier_base='', outlier_image=''):

    imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,thresh,maskfile,_,_ = bookkeeping.get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,outlier_radius,threshold,step='mask')

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
    rmsmap,outlierfile = find_outliers(**params,step='bdsf')
    pixmask = mask_image(**params)

    loop += 1

    if config_parser.has_section(args['config'], 'image'):
        if config_parser.get_key(args['config'], 'image', 'specmode') != 'cube':
            # Don't copy over continuum mask for cube-mode science imagin; allow for 3D mask (e.g. SoFiA)
            config_parser.overwrite_config(args['config'], conf_dict={'mask' : "'{0}'".format(pixmask)}, conf_sec='image')
        config_parser.overwrite_config(args['config'], conf_dict={'rmsmap' : "'{0}'".format(rmsmap)}, conf_sec='image')
        config_parser.overwrite_config(args['config'], conf_dict={'outlierfile' : "'{0}'".format(outlierfile)}, conf_sec='image')
    config_parser.overwrite_config(args['config'], conf_dict={'loop' : loop},  conf_sec='selfcal')

    bookkeeping.rename_logs(logfile)
