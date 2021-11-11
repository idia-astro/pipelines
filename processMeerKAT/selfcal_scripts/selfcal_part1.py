#Copyright (C) 2020 Inter-University Institute for Data Intensive Astronomy
#See processMeerKAT.py for license details.

import sys
import glob
import os
import re

# Adapt PYTHONPATH to include processMeerKAT
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

import config_parser
from config_parser import validate_args as va
import bookkeeping

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
import casampi

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)

def symlink_psf(imagenames,loop):

    for imagename in imagenames:
        prefix = imagename.replace('im_{0}'.format(loop),'im_{0}'.format(loop-1))
        for product in ['psf','sumwt']:
            products = glob.glob('{0}.{1}*'.format(prefix,product))
            #If outlier's PSF missing, abandon symlinking attempt and return calcpsf=True
            if len(products) == 0:
                return True
            for fname in products:
                name, ext = os.path.splitext(fname)
                # Will not have e.g. .tt0 if nterms < 2
                if ext == product:
                    ext = ''
                symlink = '{0}.{1}{2}'.format(imagename,product,ext)
                if not os.path.exists(symlink):
                    os.symlink(fname,symlink)

    return False

def selfcal_part1(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter, threshold,
                  uvrange, nterms, gridder, deconvolver, solint, calmode, discard_nloops, gaintype, outlier_threshold, flag):

    imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,_,_ = bookkeeping.get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,threshold,step='tclean')
    calcpsf = True

    #Add model column with MPI rather than in selfcal_part2 without MPI.
    #Assumes you've split out your corrected data from crosscal
    if loop == 0:
        clearcal(vis=vis, addmodel=True)

    if 1 <= loop <= nloops:
        if len(prev_caltables) > 0 and calmode[loop-1] != '':
            applycal(vis=vis, selectdata=False, gaintable=prev_caltables, parang=False, interp='linear,linearflag')

            if flag[loop-1]:
                flagdata(vis=vis, mode='rflag', datacolumn='RESIDUAL', field='', timecutoff=5.0,
                        freqcutoff=5.0, timefit='line', freqfit='line', flagdimension='freqtime',
                        extendflags=False, timedevscale=3.0, freqdevscale=3.0, spectralmax=500,
                        extendpols=False, growaround=False, flagneartime=False, flagnearfreq=False,
                        action='apply', flagbackup=True, overwrite=True, writeflags=True)

        if (not flag[loop-1] or len(prev_caltables) == 0) and gridder[loop] == gridder[loop-1] and robust[loop] == robust[loop-1] and nterms[loop] == nterms[loop-1] and imsize[loop] == imsize[loop-1] and cell[loop] == cell[loop-1]:
            # Assumes it's safe to re-use previous PSF for outliers if position has slightly changed
            imagenames = [imagename]
            if outlierfile != '':
                imagenames += re.findall(r'imagename=(.*)\n',open(outlierfile).read())
            calcpsf = symlink_psf(imagenames,loop)

    if os.path.exists(outimage):
        logger.info('Image "{0}" exists. Not overwriting, continuing to next loop.'.format(outimage))
    else:
        tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
            wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop], restoration=True,
            weighting='briggs', robust = robust[loop], niter=niter[loop], outlierfile=outlierfile,
            threshold=threshold[loop], nterms=nterms[loop], calcpsf=calcpsf, # cfcache = cfcache,
            pblimit=-1, mask=pixmask, parallel = True)


if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    selfcal_part1(**params)
    bookkeeping.rename_logs(logfile)
