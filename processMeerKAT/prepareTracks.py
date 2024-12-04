import os
import sys
import glob
from shutil import copytree

import config_parser
from config_parser import validate_args as va
import bookkeeping

from casatasks import *
logfile=casalog.logfile()
casalog.setlogfile('logs/{SLURM_JOB_NAME}-{SLURM_JOB_ID}.casa'.format(**os.environ))
import casampi

from casatools import msmetadata,image
msmd = msmetadata()
ia = image()

import logging
from time import gmtime
logging.Formatter.converter = gmtime
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s", level=logging.INFO)


from aux_scripts.concat import check_output, get_infiles
from selfcal_scripts.selfcal_part2 import find_outliers, mask_image


def deconvolveMSs(vis, refant, dopol, nloops, loop, cell, robust, imsize, wprojplanes, niter, threshold, uvrange, nterms,
                  gridder, deconvolver, solint, calmode, discard_nloops, gaintype, outlier_threshold, outlier_radius, flag):
    
    imbase,imagename,outimage,pixmask,rmsfile,caltable,prev_caltables,threshold,outlierfile,cfcache,_,_,_,_ = bookkeeping.get_selfcal_args(vis,loop,nloops,nterms,deconvolver,discard_nloops,calmode,outlier_threshold,outlier_radius,threshold,step='tclean')
    calcpsf = True

    vis = vis.split(',')
    
    tclean(vis=vis, selectdata=False, datacolumn='corrected', imagename=imagename,
            imsize=imsize[loop], cell=cell[loop], stokes='I', gridder=gridder[loop],
            wprojplanes = wprojplanes[loop], deconvolver = deconvolver[loop], restoration=True,
            weighting='briggs', robust = robust[loop], niter=niter[loop], outlierfile=outlierfile,
            threshold=threshold[loop], nterms=nterms[loop], calcpsf=calcpsf, # cfcache = cfcache,
            pblimit=-1, mask=pixmask, parallel = True)
 
def makeMask(params, method='default'):
    
    if method == 'default':
        rmsmap, outlierfile = find_outliers(**params,step='bdsf')
        pixmask = mask_image(**params)
             
    #elif method == "your method":
        #space to add different, user-defined methods into the pipeline. e.g., a spectral mask. 

    return rmsmap, outlierfile, pixmask
    
        
if __name__ == '__main__':

    args,params = bookkeeping.get_selfcal_params()
    method = params.pop('method')[0]
    
    ##blind deconvolution
    deconvolveMSs(**params)
    
    #make mask for imaging    
    rmsmap, outlierfile, pixmask = makeMask(params, method=method)
    
    loop = params['loop']
    
    #update config file for science imaging
    if config_parser.has_section(args['config'], 'image'):
        if config_parser.get_key(args['config'], 'image', 'specmode') != 'cube':
            # Don't copy over continuum mask for cube-mode science imaging; allow for 3D mask (e.g. SoFiA)
            config_parser.overwrite_config(args['config'], conf_dict={'mask' : "'{0}'".format(pixmask)}, conf_sec='image')
        config_parser.overwrite_config(args['config'], conf_dict={'rmsmap' : "'{0}'".format(rmsmap)}, conf_sec='image')
        config_parser.overwrite_config(args['config'], conf_dict={'outlierfile' : "'{0}'".format(outlierfile)}, conf_sec='image')

    bookkeeping.rename_logs(logfile)